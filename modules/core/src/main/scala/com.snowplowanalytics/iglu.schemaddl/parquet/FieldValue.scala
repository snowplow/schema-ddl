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
import cats.implicits._
import cats.data.ValidatedNel

import java.time.Instant
import CastError._

/** Run-time value, conforming `Field` (type) */
sealed trait FieldValue extends Product with Serializable

object FieldValue {
  case object NullValue extends FieldValue
  case class StringValue(value: String) extends FieldValue
  case class BooleanValue(value: Boolean) extends FieldValue
  case class IntValue(value: Int) extends FieldValue
  case class LongValue(value: Long) extends FieldValue
  case class DoubleValue(value: Double) extends FieldValue
  case class DecimalValue(value: BigDecimal) extends FieldValue
  case class TimestampValue(value: java.sql.Timestamp) extends FieldValue
  case class DateValue(value: java.sql.Date) extends FieldValue
  case class StructValue(values: List[(String, FieldValue)]) extends FieldValue
  case class ArrayValue(values: List[FieldValue]) extends FieldValue

  /**
    * Turn JSON  value into Parquet-compatible row, matching schema defined in `field`
    * Top-level function, called only one columns
    * Performs following operations in order to prevent runtime insert failure:
    * * removes unexpected additional properties
    * * turns all unexpected types into string
    */
  def cast(field: Field)(value: Json): CastResult =
    value match {
      case Json.Null =>
        if (field.nullable) NullValue.validNel else WrongType(value, field.fieldType).invalidNel
      case nonNull =>
        castNonNull(field.fieldType, nonNull)
    }

  /**
    * Turn primitive JSON or JSON object into Parquet row
    */
  private [parquet] def castNonNull(fieldType: Type, value: Json): CastResult = {
    fieldType match {
      case Type.String =>   // Fallback strategy for union types
        value.asString.fold(StringValue(value.noSpaces))(StringValue(_)).validNel
      case Type.Boolean =>
        value.asBoolean.fold(WrongType(value, fieldType).invalidNel[FieldValue])(BooleanValue(_).validNel)
      case Type.Integer =>
        value.asNumber.flatMap(_.toInt).fold(WrongType(value, fieldType).invalidNel[FieldValue])(IntValue(_).validNel)
      case Type.Long =>
        value.asNumber.flatMap(_.toLong).fold(WrongType(value, fieldType).invalidNel[FieldValue])(LongValue(_).validNel)
      case Type.Double =>
        value.asNumber.fold(WrongType(value, fieldType).invalidNel[FieldValue])(num => DoubleValue(num.toDouble).validNel)
      case Type.Decimal(precision, scale) =>
        value.asNumber.flatMap(_.toBigDecimal).fold(WrongType(value, fieldType).invalidNel[FieldValue]) {
          bigDec =>
            if (bigDec.precision <= precision && bigDec.scale <= scale)
              DecimalValue(bigDec).validNel
            else
              WrongType(value, fieldType).invalidNel
        }
      case Type.Timestamp =>
        value.asString
          .flatMap(s => Either.catchNonFatal(java.sql.Timestamp.from(Instant.parse(s))).toOption)
          .fold(WrongType(value, fieldType).invalidNel[FieldValue])(TimestampValue(_).validNel)
      case Type.Date =>
        value.asString
          .flatMap(s => Either.catchNonFatal(java.sql.Date.valueOf(s)).toOption)
          .fold(WrongType(value, fieldType).invalidNel[FieldValue])(DateValue(_).validNel)
      case Type.Struct(subfields) =>
        value
          .asObject
          .fold(WrongType(value, fieldType).invalidNel[Map[String, Json]])(_.toMap.validNel)
          .andThen(castObject(subfields))
      case Type.Array(element, containsNull) =>
        value.asArray match {
          case Some(values) => values
            .toList
            .map {
              case Json.Null =>
                if (containsNull) NullValue.validNel else WrongType(Json.Null, element).invalidNel
              case nonNull => castNonNull(element, nonNull)
            }
            .sequence[ValidatedNel[CastError, *], FieldValue]
            .map(ArrayValue.apply)
          case None => WrongType(value, fieldType).invalidNel
        }
    }
  }

  /** Part of `castValue`, mapping JSON object into *ordered* list of `TableRow`s */
  private def castObject(subfields: List[Field])(jsonObject: Map[String, Json]): CastResult =
    subfields.map { f =>
        jsonObject.get(f.name) match {
          case Some(json) => cast(f)(json).map { (f.normalName, _) }
          case None if f.nullable => (f.normalName, NullValue).validNel[CastError]
          case None => MissingInValue(f.name, Json.fromFields(jsonObject)).invalidNel
        }
    }
    .sequence[ValidatedNel[CastError, *], (String, FieldValue)]
    .map(StructValue.apply)
}
