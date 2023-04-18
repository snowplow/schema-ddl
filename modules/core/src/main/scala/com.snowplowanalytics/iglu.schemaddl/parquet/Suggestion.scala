/*
 * Copyright (c) 2021-2023 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.iglu.schemaddl.parquet

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.suggestion.decimals
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.{CommonProperties, NumberProperty, StringProperty}
import io.circe._

private[parquet] object Suggestion {

  val stringSuggestion: Suggestion = schema =>
    schema.`type`
      .filter(_.possiblyWithNull(CommonProperties.Type.String))
      .map(toNullableType(Type.String))

  val booleanSuggestion: Suggestion = schema =>
    schema.`type`
      .filter(_.possiblyWithNull(CommonProperties.Type.Boolean))
      .map(toNullableType(Type.Boolean))

  val numericSuggestion: Suggestion = schema =>
    schema.`type` match {
      case Some(types) if types.possiblyWithNull(CommonProperties.Type.Integer) =>
        Some(toNullableType(integerType(schema))(types))
      case Some(types) if onlyNumeric(types) =>
        schema.multipleOf match {
          case Some(NumberProperty.MultipleOf.IntegerMultipleOf(_)) =>
            Some(toNullableType(integerType(schema))(types))
          case Some(mult: NumberProperty.MultipleOf.NumberMultipleOf) =>
            Some(toNullableType(numericWithMultiple(mult, schema.maximum, schema.minimum))(types))
          case None =>
            Some(toNullableType(Type.Double)(types))
        }
      case _ => None
    }

  val enumSuggestion: Suggestion = schema =>
    schema.enum match {
      case Some(CommonProperties.Enum(values)) =>
        Some(numericEnum(values).orElse(stringEnum(values)).getOrElse(jsonEnum(values)))
      case _ => None
    }

  // `date-time` format usually means zoned format, which corresponds to Parquet Timestamp
  val timestampSuggestion: Suggestion = schema =>
    schema.`type` match {
      case Some(types) if types.possiblyWithNull(CommonProperties.Type.String) =>
        schema.format match {
          case Some(StringProperty.Format.DateFormat) =>
            Some(toNullableType(Type.Date)(types))
          case Some(StringProperty.Format.DateTimeFormat) =>
            Some(toNullableType(Type.Timestamp)(types))
          case _ => None
        }
      case _ => None
    }

  val allSuggestions: List[Suggestion] = List(
    timestampSuggestion,
    booleanSuggestion,
    stringSuggestion,
    numericSuggestion,
    enumSuggestion
  )

  private def toNullableType(value: Type)(commonType: CommonProperties.Type): Field.NullableType =
    Field.NullableType(value, Field.JsonNullability.extractFrom(commonType))

  private def numericWithMultiple(mult: NumberProperty.MultipleOf.NumberMultipleOf,
                                  maximum: Option[NumberProperty.Maximum],
                                  minimum: Option[NumberProperty.Minimum]): Type =
    Type.fromGenericType(decimals.numericWithMultiple(mult, maximum, minimum))

  private def numericEnum(enums: List[Json]): Option[Field.NullableType] = decimals.numericEnum(enums).map(Field.JsonNullability.fromNullableWrapper)

  private def stringEnum(enums: List[Json]): Option[Field.NullableType] = {
    def go(nullable: Field.JsonNullability, enums: List[Json]): Option[Field.NullableType] =
      enums match {
        case Nil => Some(Field.NullableType(Type.String, nullable))
        case Json.Null :: tail => go(Field.JsonNullability.ExplicitlyNullable, tail)
        case h :: tail if h.isString => go(nullable, tail)
        case _ => None
      }

    go(Field.JsonNullability.NoExplicitNull, enums)
  }

  private def jsonEnum(enums: List[Json]): Field.NullableType = {
    val nullable = if (enums.exists(_.isNull)) Field.JsonNullability.ExplicitlyNullable else Field.JsonNullability.NoExplicitNull
    Field.NullableType(Type.Json, nullable)
  }

  private def integerType(schema: Schema): Type = Type.fromGenericType(decimals.integerType(schema))
    
  private def onlyNumeric(types: CommonProperties.Type): Boolean =
    types match {
      case CommonProperties.Type.Number => true
      case CommonProperties.Type.Integer => true
      case CommonProperties.Type.Union(set) =>
        val noNull = set - CommonProperties.Type.Null
        noNull.nonEmpty && noNull.subsetOf(Set(CommonProperties.Type.Number, CommonProperties.Type.Integer))
      case _ => false
    }
}
