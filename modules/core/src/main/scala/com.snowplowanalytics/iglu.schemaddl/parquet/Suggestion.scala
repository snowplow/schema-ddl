/*
 * Copyright (c) 2021-2022 Snowplow Analytics Ltd. All rights reserved.
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
          case Some(NumberProperty.MultipleOf.NumberMultipleOf(mult)) =>
            (schema.maximum, schema.minimum) match {
              case (Some(max), Some(min)) =>
                val topPrecision = max match {
                  case NumberProperty.Maximum.IntegerMaximum(max) =>
                    BigDecimal(max).precision + mult.scale
                  case NumberProperty.Maximum.NumberMaximum(max) =>
                    max.precision - max.scale + mult.scale
                }
                val bottomPrecision = min match {
                  case NumberProperty.Minimum.IntegerMinimum(min) =>
                    BigDecimal(min).precision + mult.scale
                  case NumberProperty.Minimum.NumberMinimum(min) =>
                    min.precision - min.scale + mult.scale
                }
                Type.DecimalPrecision.of(topPrecision.max(bottomPrecision)) match {
                  case Some(precision) =>
                    Some(toNullableType(Type.Decimal(precision, mult.scale))(types))
                  case None =>
                    Some(toNullableType(Type.Double)(types))
                }
              case _ =>
                Some(toNullableType(Type.Double)(types))
            }
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

  private[iglu] def numericEnum(enums: List[Json]): Option[Field.NullableType] = {
    def go(precision: Int, scale: Int, nullable: Field.JsonNullability, enums: List[Json]): Option[Field.NullableType] =
      enums match {
        case Nil =>
          val t = Type.DecimalPrecision.of(precision).fold[Type](Type.Double)(Type.Decimal(_, scale))
          Some(Field.NullableType(t, nullable))
        case Json.Null :: tail => go(precision, scale, Field.JsonNullability.ExplicitlyNullable, tail)
        case h :: tail =>
          h.asNumber.flatMap(_.toBigDecimal) match {
            case Some(bigDecimal) =>
              val nextScale = scale.max(bigDecimal.scale)
              val nextPrecision = (precision - scale).max(bigDecimal.precision + bigDecimal.scale) + nextScale
              go(nextPrecision, nextScale, nullable, tail)
            case None => None
          }
      }

    go(0, 0, Field.JsonNullability.NoExplicitNull, enums)
  }

  private[iglu] def stringEnum(enums: List[Json]): Option[Field.NullableType] = {
    def go(nullable: Field.JsonNullability, enums: List[Json]): Option[Field.NullableType] =
      enums match {
        case Nil => Some(Field.NullableType(Type.String, nullable))
        case Json.Null :: tail => go(Field.JsonNullability.ExplicitlyNullable, tail)
        case h :: tail if h.isString => go(nullable, tail)
        case _ => None
      }
    go(Field.JsonNullability.NoExplicitNull, enums)
  }

  private[iglu] def jsonEnum(enums: List[Json]): Field.NullableType = {
    val nullable = if (enums.exists(_.isNull)) Field.JsonNullability.ExplicitlyNullable else Field.JsonNullability.NoExplicitNull
    Field.NullableType(Type.Json, nullable)
  }

  private def integerType(schema: Schema): Type =
    (schema.minimum, schema.maximum) match {
      case (Some(min), Some(max)) =>
        val minDecimal = min.getAsDecimal
        val maxDecimal = max.getAsDecimal
        if (maxDecimal <= Int.MaxValue && minDecimal >= Int.MinValue) Type.Integer
        else if (maxDecimal <= Long.MaxValue && minDecimal >= Long.MinValue) Type.Long
        else Type.DecimalPrecision
          .of((maxDecimal.precision - maxDecimal.scale).max(minDecimal.precision - minDecimal.scale))
          .fold[Type](Type.Double)(Type.Decimal(_, 0))
      case _ => Type.Long
    }

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
