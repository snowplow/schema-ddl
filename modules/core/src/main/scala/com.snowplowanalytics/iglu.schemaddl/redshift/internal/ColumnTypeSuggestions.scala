/*
 * Copyright (c) 2014-2023 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.iglu.schemaddl.redshift.internal

// cats
import cats.instances.bigInt._
import cats.instances.int._
import cats.instances.list._
import cats.instances.option._
import cats.syntax.eq._
import cats.syntax.foldable._
import cats.syntax.traverse._
import com.snowplowanalytics.iglu.schemaddl.redshift.ShredModelEntry
import io.circe.Json

import scala.annotation.tailrec

// This project
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.CommonProperties.Type
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.NumberProperty.{Maximum, MultipleOf}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.StringProperty.{Format, MaxLength, MinLength}
import com.snowplowanalytics.iglu.schemaddl.redshift.ShredModelEntry.ColumnType._

/**
 * Module containing functions for data type suggestions
 */
private[redshift] object ColumnTypeSuggestions {

  val DefaultArraySize: Int = 65535

  /**
   * Type alias for function suggesting an encode type based on map of
   * JSON Schema properties
   */
  type TypeSuggestion = Schema => Option[ShredModelEntry.ColumnType]

  // For complex enums Suggest VARCHAR with length of longest element
  val complexEnumSuggestion: TypeSuggestion = properties=>
    properties.enum match {
      case Some(enums) if isComplexEnum(enums.value) =>
        val longest = excludeNull(enums.value).map(_.noSpaces.length).maximumOption.getOrElse(16)
        Some(RedshiftVarchar(longest))
      case _ => None
    }

  // Suggest VARCHAR(4096) for all product types. Should be in the beginning
  val productSuggestion: TypeSuggestion = properties =>
    properties.`type` match {
      case Some(t: Type.Union) if t.isUnion =>
        val typeSet = t.value - Type.Null
        if (typeSet == Set(Type.Boolean, Type.Integer))
          Some(ProductType( Some(Int.MaxValue.toString.length)))
        else
          Some(ProductType(None))
      case _ => None
    }

  val timestampSuggestion: TypeSuggestion = properties=>
    (properties.`type`, properties.format) match {
      case (Some(types), Some(Format.DateTimeFormat)) if types.possiblyWithNull(Type.String) =>
        Some(RedshiftTimestamp)
      case _ => None
    }

  val dateSuggestion: TypeSuggestion = properties=>
    (properties.`type`, properties.format) match {
      case (Some(types), Some(Format.DateFormat)) if types.possiblyWithNull(Type.String) =>
        Some(RedshiftDate)
      case _ => None
    }

  val arraySuggestion: TypeSuggestion = properties=>
    properties.`type` match {
      case Some(types) if types.possiblyWithNull(Type.Array) =>
        Some(RedshiftVarchar(DefaultArraySize))
      case _ => None
    }

  val numberSuggestion: TypeSuggestion = properties=>
    (properties.`type`, properties.multipleOf) match {
      case (Some(types), Some(MultipleOf.NumberMultipleOf(m))) if types.possiblyWithNull(Type.Number) && m == BigDecimal(1,2) =>
        Some(RedshiftDecimal(Some(36), Some(2)))
      case (Some(types), _) if types.possiblyWithNull(Type.Number) =>
        Some(RedshiftDouble)
      case (Some(types: Type.Union), _) if (types.value - Type.Null) == Set(Type.Integer, Type.Number) =>
        Some(RedshiftDouble)
      case _ =>
        None
    }

  val integerSuggestion: TypeSuggestion = properties=> {
    (properties.`type`, properties.maximum, properties.enum, properties.multipleOf) match {
      case (Some(types), Some(max), _, _) if types.possiblyWithNull(Type.Integer) =>
        getIntSize(max)
      // Contains only enum
      case (types, _, Some(enum), _) if types.isEmpty || types.get.possiblyWithNull(Type.Integer) =>
        enum.value.traverse(_.asNumber.flatMap(_.toBigInt)).flatMap(_.maximumOption).flatMap(getIntSize)
      case (Some(types), _, _, _) if types.possiblyWithNull(Type.Integer) =>
        Some(RedshiftBigInt)
      case (_, max, _, Some(MultipleOf.IntegerMultipleOf(_))) =>
        max.flatMap(getIntSize).orElse(Some(RedshiftInteger))
      case _ => None
    }
  }

  val charSuggestion: TypeSuggestion = properties=> {
    (properties.`type`, properties.minLength, properties.maxLength) match {
      case (Some(types), Some(MinLength(min)), Some(MaxLength(max)))
        if min === max && types.possiblyWithNull(Type.String) =>
        Some(RedshiftChar(min.toInt))
      case _ => None
    }
  }

  val booleanSuggestion: TypeSuggestion = properties=> {
    properties.`type` match {
      case Some(types) if types.possiblyWithNull(Type.Boolean) => Some(RedshiftBoolean)
      case _ => None
    }
  }

  val uuidSuggestion: TypeSuggestion = properties=> {
    (properties.`type`, properties.format) match {
      case (Some(types), Some(Format.UuidFormat)) if types.possiblyWithNull(Type.String) =>
        Some(RedshiftChar(36))
      case _ => None
    }
  }

  val varcharSuggestion: TypeSuggestion = properties=> {
    (properties.`type`,  properties.maxLength, properties.enum, properties.format) match {
      case (Some(types), _,                    _,               Some(Format.Ipv6Format)) if types.possiblyWithNull(Type.String) =>
        Some(RedshiftVarchar(39))
      case (Some(types), _,                    _,               Some(Format.Ipv4Format)) if types.possiblyWithNull(Type.String) =>
        Some(RedshiftVarchar(15))
      case (Some(types), _,                    _,               Some(Format.EmailFormat)) if types.possiblyWithNull(Type.String) =>
        Some(RedshiftVarchar(255))
      case (Some(types), Some(maxLength),      None,            _) if types.possiblyWithNull(Type.String) =>
        Some(RedshiftVarchar(maxLength.value.toInt))
      case (_,           _,                    Some(enum),      _) =>
        enum.value.map(jsonLength).maximumOption match {
          case Some(maxLength) if enum.value.lengthCompare(1) == 0 =>
            Some(RedshiftChar(maxLength))
          case Some(maxLength) =>
            Some(RedshiftVarchar(maxLength))
          case None => None
        }
      case _ => None
    }
  }

  val columnTypeSuggestions: List[TypeSuggestion] = List(
    complexEnumSuggestion,
    productSuggestion,
    timestampSuggestion,
    dateSuggestion,
    arraySuggestion,
    integerSuggestion,
    numberSuggestion,
    booleanSuggestion,
    charSuggestion,
    uuidSuggestion,
    varcharSuggestion
  )
  
  private def jsonLength(json: Json): Int =
    json.fold(0, b => b.toString.length, _ => json.noSpaces.length, _.length, _ => json.noSpaces.length, _ => json.noSpaces.length)

  /**
   * Get set of types or enum as string excluding null
   *
   * @param types comma-separated types
   * @return set of strings
   */
  private def excludeNull(types: List[Json]): List[Json] =
    types.filterNot(_.isNull)

  /**
   * Helper function to get size of Integer
   *
   * @param max upper bound
   * @return Long representing biggest possible value or None if it's not Int
   */
  private def getIntSize(max: Maximum): Option[ShredModelEntry.ColumnType] =
    max match {
      case Maximum.IntegerMaximum(bigInt) => getIntSize(bigInt)
      case Maximum.NumberMaximum(_) => Some(RedshiftDecimal(None, None))
    }

  private def getIntSize(max: BigInt): Option[ShredModelEntry.ColumnType] =
    if (max <= Short.MaxValue.toInt) Some(RedshiftSmallInt)
    else if (max <= Int.MaxValue) Some(RedshiftInteger)
    else Some(RedshiftBigInt)

  /**
   * Check enum contains some different types
   * (string and number or number and boolean)
   */
  private def isComplexEnum(enum: List[Json]) = {
    // Predicates
    def isNumeric(s: Json) = s.isNumber
    def isNonNumeric(s: Json) = !isNumeric(s)
    def isBoolean(s: Json) = s.isBoolean

    val nonNullEnum = excludeNull(enum)
    somePredicates(nonNullEnum, List(isNumeric, isNonNumeric, isBoolean), 2)
  }

  /**
   * Check at least some `quantity` of `predicates` are true on `instances`
   *
   * @param instances list of instances to check on
   * @param predicates list of predicates to check
   * @param quantity required quantity
   */
  @tailrec
  private def somePredicates(instances: List[Json], predicates: List[Json => Boolean], quantity: Int): Boolean =
    if (quantity == 0) true
    else predicates match {
      case Nil => false
      case h :: tail if instances.exists(h) => somePredicates(instances, tail, quantity - 1)
      case _ :: tail => somePredicates(instances, tail, quantity)
    }
}
