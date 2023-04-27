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
package com.snowplowanalytics.iglu.schemaddl.bigquery

import io.circe._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.suggestion.decimals
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.{CommonProperties, StringProperty, NumberProperty}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.suggestion.numericType.NullableWrapper

object Suggestion {

  val stringSuggestion: Suggestion = (schema, required) =>
    schema.`type` match {
      case Some(CommonProperties.Type.String) =>
        Some(name => Field(name, Type.String, Mode.required(required)))
      case Some(types) if types.nullable(CommonProperties.Type.String) =>
        Some(name => Field(name, Type.String, Mode.Nullable))
      case _ => None
    }

  val booleanSuggestion: Suggestion = (schema, required) =>
    schema.`type` match {
      case Some(CommonProperties.Type.Boolean) =>
        Some(name => Field(name, Type.Boolean, Mode.required(required)))
      case Some(CommonProperties.Type.Union(types)) if withNull(types, CommonProperties.Type.Boolean) =>
        Some(name => Field(name, Type.Boolean, Mode.Nullable))
      case _ => None
    }

  val integerSuggestion: Suggestion = (schema, required) =>
    schema.`type` match {
      case Some(CommonProperties.Type.Integer) =>
        Some(name => Field(name, Type.fromGenericType(decimals.integerType(schema)), Mode.required(required)))
      case Some(CommonProperties.Type.Union(types)) if withNull(types, CommonProperties.Type.Integer) =>
        Some(name => Field(name, Type.fromGenericType(decimals.integerType(schema)), Mode.Nullable))
      case _ => None
    }

  val numericSuggestion: Suggestion = (schema, required) => schema.`type` match {
    case Some(CommonProperties.Type.Number) =>
      schema.multipleOf match {
        case Some(NumberProperty.MultipleOf.IntegerMultipleOf(_)) =>
          Some(name => Field(name, Type.fromGenericType(decimals.integerType(schema)), Mode.required(required)))
        case Some(mult: NumberProperty.MultipleOf.NumberMultipleOf) =>
          Some(name => Field(name, numericWithMultiple(mult, schema.maximum, schema.minimum), Mode.required(required)))
        case None => None
      }
    case Some(CommonProperties.Type.Union(types)) if withNull(types, CommonProperties.Type.Number) | onlyNumeric(types, true) =>
      schema.multipleOf match {
        case Some(NumberProperty.MultipleOf.IntegerMultipleOf(_)) =>
          Some(name => Field(name, Type.fromGenericType(decimals.integerType(schema)), Mode.Nullable))
        case Some(mult: NumberProperty.MultipleOf.NumberMultipleOf) =>
          Some(name => Field(name, numericWithMultiple(mult, schema.maximum, schema.minimum), Mode.Nullable))
        case None => None
      }
    case Some(CommonProperties.Type.Union(types)) if onlyNumeric(types, false) =>
      schema.multipleOf match {
        case Some(NumberProperty.MultipleOf.IntegerMultipleOf(_)) =>
          Some(name => Field(name, Type.fromGenericType(decimals.integerType(schema)), Mode.required(required)))
        case Some(mult: NumberProperty.MultipleOf.NumberMultipleOf) =>
          Some(name => Field(name, numericWithMultiple(mult, schema.maximum, schema.minimum), Mode.required(required)))
        case None => None
      }
    case _ => None
  }

  val floatSuggestion: Suggestion = (schema, required) =>
    schema.`type` match {
      case Some(CommonProperties.Type.Number) =>
        Some(name => Field(name, Type.Float, Mode.required(required)))
      case Some(CommonProperties.Type.Union(types)) if onlyNumeric(types, true) =>
        Some(name => Field(name, Type.Float, Mode.Nullable))
      case Some(CommonProperties.Type.Union(types)) if onlyNumeric(types, false) =>
        Some(name => Field(name, Type.Float, Mode.required(required)))
      case Some(CommonProperties.Type.Union(types)) if withNull(types, CommonProperties.Type.Number) =>
        Some(name => Field(name, Type.Float, Mode.Nullable))
      case _ => None
    }

  val complexEnumSuggestion: Suggestion = (schema, required) =>
    schema.enum match {
      case Some(CommonProperties.Enum(values)) =>
        Some(fromEnum(values, required))
      case _ => None
    }

  private def numericWithMultiple(mult: NumberProperty.MultipleOf.NumberMultipleOf,
                                  maximum: Option[NumberProperty.Maximum],
                                  minimum: Option[NumberProperty.Minimum]): Type =
    Type.fromGenericType(decimals.numericWithMultiple(mult, maximum, minimum))


  //  (Field.JsonNullability.fromNullableWrapper)


  // `date-time` format usually means zoned format, which corresponds to BQ Timestamp
  val timestampSuggestion: Suggestion = (schema, required) =>
    (schema.`type`, schema.format) match {
      case (Some(CommonProperties.Type.String), Some(StringProperty.Format.DateFormat)) =>
        Some(name => Field(name, Type.Date, Mode.required(required)))
      case (Some(CommonProperties.Type.Union(types)), Some(StringProperty.Format.DateFormat)) if withNull(types, CommonProperties.Type.String) =>
        Some(name => Field(name, Type.Date, Mode.Nullable))

      case (Some(CommonProperties.Type.String), Some(StringProperty.Format.DateTimeFormat)) =>
        Some(name => Field(name, Type.Timestamp, Mode.required(required)))
      case (Some(CommonProperties.Type.Union(types)), Some(StringProperty.Format.DateTimeFormat)) if withNull(types, CommonProperties.Type.String) =>
        Some(name => Field(name, Type.Timestamp, Mode.Nullable))

      case _ => None
    }

  def finalSuggestion(schema: Schema, required: Boolean): String => Field =
    schema.`type` match {
      case Some(jsonType) if jsonType.nullable =>
        name => Field(name, Type.String, Mode.Nullable)
      case _ =>
        name => Field(name, Type.String, Mode.required(required))
    }

  val suggestions: List[Suggestion] = List(
    timestampSuggestion,
    booleanSuggestion,
    stringSuggestion,
    integerSuggestion,
    numericSuggestion,
    floatSuggestion,
    complexEnumSuggestion
  )

  private[iglu] def fromEnum(enums: List[Json], required: Boolean): String => Field = {
    def isString(json: Json) = json.isString || json.isNull

    def isInteger(json: Json) = json.asNumber.exists(_.toBigInt.isDefined) || json.isNull

    def isNumeric(json: Json) = json.isNumber || json.isNull

    val noNull: Boolean = !enums.contains(Json.Null)

    if (enums.forall(isString)) {
      name => Field(name, Type.String, Mode.required(required && noNull))
    } else if (enums.forall(isInteger)) {
      name => Field(name, Type.Integer, Mode.required(required && noNull))
    } else if (enums.forall(isNumeric)) {
      name =>
        decimals.numericEnum(enums).map {
          case NullableWrapper.NullableValue(t) => Field(name, Type.fromGenericType(t), Mode.required(required && noNull))
          case NullableWrapper.NotNullValue(t) => Field(name, Type.fromGenericType(t), Mode.required(required && noNull))
        } match {
          case Some(value) => value
          // Unreachable as `None` here would mean that some `enums.forall(isNumeric)` did not work.
          case None => Field(name, Type.Float, Mode.required(required && noNull)) 
        }
    } else {
      name => Field(name, Type.String, Mode.required(required && noNull))
    }
  }

  private def withNull(types: Set[CommonProperties.Type], t: CommonProperties.Type): Boolean =
    types == Set(t, CommonProperties.Type.Null) || types == Set(t)

  private def onlyNumeric(types: Set[CommonProperties.Type], allowNull: Boolean): Boolean =
    if (allowNull) types == Set(CommonProperties.Type.Number, CommonProperties.Type.Integer, CommonProperties.Type.Null)
    else types == Set(CommonProperties.Type.Number, CommonProperties.Type.Integer)
}
