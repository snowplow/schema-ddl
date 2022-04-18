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

import io.circe._

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.{CommonProperties, NumberProperty, StringProperty}

object Suggestion {

  val stringSuggestion: Suggestion = (schema, required) =>
    schema.`type` match {
      case Some(types) if types.possiblyWithNull(CommonProperties.Type.String) =>
        Some(name => Field(name, Type.String, !required || types.nullable))
      case _ => None
    }

  val booleanSuggestion: Suggestion = (schema, required) =>
    schema.`type` match {
      case Some(types) if types.possiblyWithNull(CommonProperties.Type.Boolean) =>
        Some(name => Field(name, Type.Boolean, !required || types.nullable))
      case _ => None
    }

  val numericSuggestion: Suggestion = (schema, required) =>
    schema.`type` match {
      case Some(types) if types.possiblyWithNull(CommonProperties.Type.Integer) =>
        Some(name => Field(name, integerType(schema), !required || types.nullable))
      case Some(types) if onlyNumeric(types) =>
        schema.multipleOf match {
          case Some(NumberProperty.MultipleOf.IntegerMultipleOf(_)) =>
            Some(name => Field(name, integerType(schema), !required || types.nullable))
          case Some(NumberProperty.MultipleOf.NumberMultipleOf(mult)) =>
            (schema.maximum, schema.minimum) match {
              case (Some(max), Some(min)) =>
                val topPrecision = max match {
                  case NumberProperty.Maximum.IntegerMaximum(max) =>
                    max.bitLength + mult.scale
                  case NumberProperty.Maximum.NumberMaximum(max) =>
                    max.precision - max.scale + mult.scale
                }
                val bottomPrecision = min match {
                  case NumberProperty.Minimum.IntegerMinimum(min) =>
                    min.bitLength + mult.scale
                  case NumberProperty.Minimum.NumberMinimum(min) =>
                    min.precision - min.scale + mult.scale
                }
                val precision = topPrecision.max(bottomPrecision)
                if (precision <= MaxNumericPrecision)
                  Some(name => Field(name, Type.Decimal(precision, mult.scale), !required || types.nullable))
                else
                  Some(name => Field(name, Type.Double, !required || types.nullable))
              case _ =>
                Some(name => Field(name, Type.Double, !required || types.nullable))
            }
          case None =>
            Some(name => Field(name, Type.Double, !required || types.nullable))
        }
      case _ => None
    }

  val complexEnumSuggestion: Suggestion = (schema, required) =>
    schema.enum match {
      case Some(CommonProperties.Enum(values)) =>
        Some(fromEnum(values, required))
      case _ => None
    }

  // `date-time` format usually means zoned format, which corresponds to BQ Timestamp
  val timestampSuggestion: Suggestion = (schema, required) =>
    schema.`type` match {
      case Some(types) if types.possiblyWithNull(CommonProperties.Type.String) =>
        schema.format match {
          case Some(StringProperty.Format.DateFormat) =>
            Some(name => Field(name, Type.Date, !required || types.nullable))
          case Some(StringProperty.Format.DateTimeFormat) =>
            Some(name => Field(name, Type.Timestamp, !required || types.nullable))
          case _ => None
        }
      case _ => None
    }

  // TODO: Should we have a "Type.Json" type and use parquet's Json logical type? https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L342
  // Would implementations (e.g. spark/databricks) read it as a string if they don't support JSON
  def finalSuggestion(schema: Schema, required: Boolean): String => Field =
    schema.`type` match {
      case Some(jsonType) if jsonType.nullable =>
        name => Field(name, Type.String, false)
      case _ =>
        name => Field(name, Type.String, !required)
    }

  val suggestions: List[Suggestion] = List(
    timestampSuggestion,
    booleanSuggestion,
    stringSuggestion,
    numericSuggestion,
    complexEnumSuggestion
  )

  private[iglu] def fromEnum(enums: List[Json], required: Boolean): String => Field = {
    def go(precision: Int, scale: Int, enums: List[Json]): Type =
      enums match {
        case Nil => Type.Decimal(precision, scale)
        case Json.Null :: tail => go(precision, scale, tail)
        case h :: tail =>
          h.asNumber.flatMap(_.toBigDecimal) match {
            case Some(bigDecimal) => go(precision.max(bigDecimal.precision), scale.max(bigDecimal.scale), tail)
            case None => Type.String
          }
      }

    val nullable: Boolean = !required || enums.contains(Json.Null)
    name => Field(name, go(0, 0, enums), nullable)
  }

  private def integerType(schema: Schema): Type =
    (schema.minimum, schema.maximum) match {
      case (Some(min), Some(max)) =>
        val minDecimal = min.getAsDecimal
        val maxDecimal = max.getAsDecimal
        if (maxDecimal <= Int.MaxValue && minDecimal >= Int.MinValue) Type.Integer
        else if (maxDecimal <= Long.MaxValue && minDecimal >= Long.MaxValue) Type.Long
        else {
          val precision = (maxDecimal.precision - maxDecimal.scale).max(minDecimal.precision - minDecimal.scale)
          if (precision <= MaxNumericPrecision) Type.Decimal(precision, 0)
          else Type.Double
        }
      case _ => Type.Long
    }

  // https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/DecimalType.html
  // TODO: Should we cutoff precision at 18 so we don't need to support fixed_len_byte_array? https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#decimal
  private val MaxNumericPrecision = 38

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
