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

import io.circe._
import cats.implicits._
import cats.data.{ValidatedNel, Validated}

import java.time.Instant
import java.time.format.DateTimeFormatter
import CastError._

/** Run-time value, conforming [[Field]] (type) */
sealed trait FieldValue extends Product with Serializable

object FieldValue {
  case object NullValue extends FieldValue
  case class JsonValue(value: Json) extends FieldValue
  case class StringValue(value: String) extends FieldValue
  case class BooleanValue(value: Boolean) extends FieldValue
  case class IntValue(value: Int) extends FieldValue
  case class LongValue(value: Long) extends FieldValue
  case class DoubleValue(value: Double) extends FieldValue
  case class DecimalValue(value: BigDecimal, precision: Type.DecimalPrecision) extends FieldValue
  case class TimestampValue(value: java.sql.Timestamp) extends FieldValue
  case class DateValue(value: java.sql.Date) extends FieldValue
  case class StructValue(values: List[NamedValue]) extends FieldValue
  case class ArrayValue(values: List[FieldValue]) extends FieldValue
  /* Part of [[StructValue]] */
  case class NamedValue(name: String, value: FieldValue)

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
        if (field.nullability.nullable) NullValue.validNel
        else if (field.fieldType === Type.Json) JsonValue(value).validNel
        else WrongType(value, field.fieldType).invalidNel
      case nonNull =>
        castNonNull(field.fieldType)(nonNull)
    }

  /**
    * Turn primitive JSON or JSON object into Parquet row
    */
  private def castNonNull(fieldType: Type): Json => CastResult =
    fieldType match {
      case Type.Json => castJson
      case Type.String => castString
      case Type.Boolean => castBoolean
      case Type.Integer => castInteger
      case Type.Long => castLong
      case Type.Double => castDouble
      case d: Type.Decimal => castDecimal(d)
      case Type.Timestamp => castTimestamp
      case Type.Date => castDate
      case s: Type.Struct => castStruct(s)
      case a: Type.Array => castArray(a)
    }

  private def castString(value: Json): CastResult =
    value.asString.fold(WrongType(value, Type.String).invalidNel[FieldValue])(StringValue(_).validNel)

  private def castBoolean(value: Json): CastResult =
    value.asBoolean.fold(WrongType(value, Type.Boolean).invalidNel[FieldValue])(BooleanValue(_).validNel)

  private def castInteger(value: Json): CastResult =
    value.asNumber.flatMap(_.toInt).fold(WrongType(value, Type.Integer).invalidNel[FieldValue])(IntValue(_).validNel)

  private def castLong(value: Json): CastResult =
    value.asNumber.flatMap(_.toLong).fold(WrongType(value, Type.Long).invalidNel[FieldValue])(LongValue(_).validNel)

  private def castDouble(value: Json): CastResult =
    value.asNumber.fold(WrongType(value, Type.Double).invalidNel[FieldValue])(num => DoubleValue(num.toDouble).validNel)

  private def castDecimal(decimalType: Type.Decimal)(value: Json): CastResult =
    value.asNumber.flatMap(_.toBigDecimal).fold(WrongType(value, decimalType).invalidNel[FieldValue]) {
      bigDec =>
        Either.catchOnly[java.lang.ArithmeticException](bigDec.setScale(decimalType.scale, BigDecimal.RoundingMode.UNNECESSARY)) match {
          case Right(scaled) if bigDec.precision <= Type.DecimalPrecision.toInt(decimalType.precision) =>
            DecimalValue(scaled, decimalType.precision).validNel
          case _ =>
            WrongType(value, decimalType).invalidNel
        }
    }

  private def castTimestamp(value: Json): CastResult =
    value.asString
      .flatMap(s => Either.catchNonFatal(DateTimeFormatter.ISO_DATE_TIME.parse(s)).toOption)
      .flatMap(a => Either.catchNonFatal(java.sql.Timestamp.from(Instant.from(a))).toOption)
      .fold(WrongType(value, Type.Timestamp).invalidNel[FieldValue])(TimestampValue(_).validNel)

  private def castDate(value: Json): CastResult =
    value.asString
      .flatMap(s => Either.catchNonFatal(java.sql.Date.valueOf(s)).toOption)
      .fold(WrongType(value, Type.Date).invalidNel[FieldValue])(DateValue(_).validNel)

  private def castJson(value: Json): CastResult =
    JsonValue(value).validNel

  private def castArray(array: Type.Array)(value: Json): CastResult =
    value.asArray match {
      case Some(values) => values
        .toList
        .map {
          case Json.Null =>
            if (array.nullability.nullable) NullValue.validNel
            else if (array.element === Type.Json) JsonValue(Json.Null).validNel
            else WrongType(Json.Null, array.element).invalidNel
          case nonNull => castNonNull(array.element)(nonNull)
        }
        .sequence[ValidatedNel[CastError, *], FieldValue]
        .map(ArrayValue.apply)
      case None => WrongType(value, array).invalidNel
    }

  private def castStruct(struct: Type.Struct)(value: Json): CastResult =
    value.asObject match {
      case None =>
        WrongType(value, struct).invalidNel[FieldValue]
      case Some(obj) =>
        val map = obj.toMap
        struct.fields
          .map(castStructField(_, map))
          .sequence[ValidatedNel[CastError, *], NamedValue]
          .map(StructValue.apply)
    }

  /** Part of `castStruct`, mapping sub-fields of a JSON object into `FieldValue`s */
  private def castStructField(field: Field, jsonObject: Map[String, Json]): ValidatedNel[CastError, NamedValue] =
    field.accessors
      .toList
      .map { name =>
        jsonObject.get(name) match {
          case Some(json) => cast(field)(json).map(NamedValue(Field.normalizeName(field), _))
          case None =>
            field.nullability match {
              case Type.Nullability.Nullable => NamedValue(Field.normalizeName(field), NullValue).validNel
              case Type.Nullability.Required => MissingInValue(field.name, Json.fromFields(jsonObject)).invalidNel
            }
        }
      }
      .reduce[ValidatedNel[CastError, NamedValue]] {
        case (Validated.Valid(f), Validated.Invalid(_)) => Validated.Valid(f)
        case (Validated.Invalid(_), Validated.Valid(f)) => Validated.Valid(f)
        case (Validated.Valid(f), Validated.Valid(NamedValue(_, NullValue))) => Validated.Valid(f)
        case (Validated.Valid(NamedValue(_, NullValue)), Validated.Valid(f)) => Validated.Valid(f)
        case (Validated.Valid(f), Validated.Valid(_)) =>
          // We must non-deterministically pick on or the other
          Validated.Valid(f)
        case (Validated.Invalid(f), Validated.Invalid(_)) =>
          Validated.Invalid(f)
      }
}
