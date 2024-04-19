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
import cats.data.{NonEmptyList, ValidatedNel, Validated}
import cats.Semigroup

import java.time.{Instant, LocalDate}
import java.time.format.DateTimeFormatter
import CastError._

trait Caster[A] {

  def nullValue: A
  def jsonValue(v: Json): A
  def stringValue(v: String): A
  def booleanValue(v: Boolean): A
  def intValue(v: Int): A
  def longValue(v: Long): A
  def doubleValue(v: Double): A
  def decimalValue(unscaled: BigInt, details: Type.Decimal): A
  def dateValue(v: LocalDate): A
  def timestampValue(v: Instant): A
  def structValue(vs: NonEmptyList[Caster.NamedValue[A]]): A
  def arrayValue(vs: List[A]): A
}

object Caster {

  case class NamedValue[A](name: String, value: A)

  /** Result of (Schema, JSON) -> A transformation */
  type Result[A] = ValidatedNel[CastError, A]

  /**
    * Turn JSON  value into Parquet-compatible row, matching schema defined in `field`
    * Top-level function, called only one columns
    * Performs following operations in order to prevent runtime insert failure:
    * * removes unexpected additional properties
    * * turns all unexpected types into string
    */
  def cast[A](caster: Caster[A], field: Field, value: Json): Result[A] =
    value match {
      case Json.Null =>
        if (field.nullability.nullable) caster.nullValue.validNel
        else if (field.fieldType === Type.Json) caster.jsonValue(value).validNel
        else WrongType(value, field.fieldType).invalidNel
      case nonNull =>
        castNonNull(caster, field.fieldType, nonNull)
    }

  private def castNonNull[A](caster: Caster[A], fieldType: Type, value: Json): Result[A] =
    fieldType match {
      case Type.Json => castJson(caster, value)
      case Type.String => castString(caster, value)
      case Type.Boolean => castBoolean(caster, value)
      case Type.Integer => castInteger(caster, value)
      case Type.Long => castLong(caster, value)
      case Type.Double => castDouble(caster, value)
      case d: Type.Decimal => castDecimal(caster, d, value)
      case Type.Timestamp => castTimestamp(caster, value)
      case Type.Date => castDate(caster, value)
      case s: Type.Struct => castStruct(caster, s, value)
      case a: Type.Array => castArray(caster, a, value)
    }

  private def castJson[A](caster: Caster[A], value: Json): Result[A] =
    caster.jsonValue(value).validNel

  private def castString[A](caster: Caster[A], value: Json): Result[A] =
    value.asString.fold(WrongType(value, Type.String).invalidNel[A])(caster.stringValue(_).validNel)

  private def castBoolean[A](caster: Caster[A], value: Json): Result[A] =
    value.asBoolean.fold(WrongType(value, Type.Boolean).invalidNel[A])(caster.booleanValue(_).validNel)

  private def castInteger[A](caster: Caster[A], value: Json): Result[A] =
    value.asNumber.flatMap(_.toInt).fold(WrongType(value, Type.Integer).invalidNel[A])(caster.intValue(_).validNel)

  private def castLong[A](caster: Caster[A], value: Json): Result[A] =
    value.asNumber.flatMap(_.toLong).fold(WrongType(value, Type.Long).invalidNel[A])(caster.longValue(_).validNel)

  private def castDouble[A](caster: Caster[A], value: Json): Result[A] =
    value.asNumber.fold(WrongType(value, Type.Double).invalidNel[A])(num => caster.doubleValue(num.toDouble).validNel)

  private def castDecimal[A](caster: Caster[A], decimalType: Type.Decimal, value: Json): Result[A] =
    value.asNumber.flatMap(_.toBigDecimal).fold(WrongType(value, decimalType).invalidNel[A]) {
      bigDec =>
        Either.catchOnly[java.lang.ArithmeticException](bigDec.setScale(decimalType.scale, BigDecimal.RoundingMode.UNNECESSARY)) match {
          case Right(scaled) if bigDec.precision <= Type.DecimalPrecision.toInt(decimalType.precision) =>
            caster.decimalValue(scaled.underlying.unscaledValue, decimalType).validNel
          case _ =>
            WrongType(value, decimalType).invalidNel
        }
    }

  private def castTimestamp[A](caster: Caster[A], value: Json): Result[A] =
    value.asString
      .flatMap(s => Either.catchNonFatal(DateTimeFormatter.ISO_DATE_TIME.parse(s)).toOption)
      .flatMap(a => Either.catchNonFatal(Instant.from(a)).toOption)
      .fold(WrongType(value, Type.Timestamp).invalidNel[A])(caster.timestampValue(_).validNel)

  private def castDate[A](caster: Caster[A], value: Json): Result[A] =
    value.asString
      .flatMap(s => Either.catchNonFatal(DateTimeFormatter.ISO_LOCAL_DATE.parse(s)).toOption)
      .flatMap(a => Either.catchNonFatal(LocalDate.from(a)).toOption)
      .fold(WrongType(value, Type.Date).invalidNel[A])(caster.dateValue(_).validNel)

  private def castArray[A](caster: Caster[A], array: Type.Array, value: Json): Result[A] =
    value.asArray match {
      case Some(values) => values
        .toList
        .map {
          case Json.Null =>
            if (array.nullability.nullable) caster.nullValue.validNel
            else if (array.element === Type.Json) caster.jsonValue(Json.Null).validNel
            else WrongType(Json.Null, array.element).invalidNel
          case nonNull => castNonNull(caster, array.element, nonNull)
        }
        .sequence[Result, A]
        .map(caster.arrayValue(_))
      case None => WrongType(value, array).invalidNel
    }


  private def castStruct[A](caster: Caster[A], struct: Type.Struct, value: Json): Result[A] =
    value.asObject match {
      case None =>
        WrongType(value, struct).invalidNel[A]
      case Some(obj) =>
        val map = obj.toMap
        struct.fields
          .map(castStructField(caster, _, map))
          .sequence[Result, NamedValue[A]]
          .map(caster.structValue(_))
    }


  // Used internally as part of `castStructField`
  private case class CastAccumulate[A](result: Option[ValidatedNel[CastError,A]], observedNull: Boolean)

  private implicit def semigroupCastAccumulate[A]: Semigroup[CastAccumulate[A]] = new Semigroup[CastAccumulate[A]] {
    def combine(x: CastAccumulate[A], y: CastAccumulate[A]): CastAccumulate[A] = {
      val result = (x.result, y.result) match {
        case (None, r) => r
        case (r, None) => r
        case (Some(Validated.Valid(f)), Some(Validated.Invalid(_))) => Some(Validated.Valid(f))
        case (Some(Validated.Invalid(_)), Some(Validated.Valid(f))) => Some(Validated.Valid(f))
        case (Some(Validated.Valid(f)), Some(Validated.Valid(_))) =>
          // We are forced to pick on or the other
          Some(Validated.Valid(f))
        case (Some(Validated.Invalid(f)), Some(Validated.Invalid(_))) =>
          Some(Validated.Invalid(f))
      }
      CastAccumulate(result, x.observedNull || y.observedNull)
    }
  }

  /** Part of `castStruct`, mapping sub-fields of a JSON object into `FieldValue`s */
  private def castStructField[A](caster: Caster[A], field: Field, jsonObject: Map[String, Json]): ValidatedNel[CastError, NamedValue[A]] = {
    val ca = field.accessors
      .toList
      .map { name =>
        jsonObject.get(name) match {
          case Some(Json.Null) => CastAccumulate[A](None, true)
          case None => CastAccumulate[A](None, true)
          case Some(json) => CastAccumulate(cast(caster, field, json).some, false)
        }
      }
      .reduce(_ |+| _)

    ca match {
      case CastAccumulate(Some(Validated.Invalid(_)), true) if field.nullability.nullable =>
        NamedValue(Field.normalizeName(field), caster.nullValue).validNel
      case CastAccumulate(None, true) if field.nullability.nullable =>
        NamedValue(Field.normalizeName(field), caster.nullValue).validNel
      case CastAccumulate(None, true)  =>
        WrongType(Json.Null, field.fieldType).invalidNel
      case CastAccumulate(None, _)  =>
        MissingInValue(field.name, Json.fromFields(jsonObject)).invalidNel
      case CastAccumulate(Some(Validated.Invalid(f)), _) =>
        Validated.Invalid(f)
      case CastAccumulate(Some(Validated.Valid(f)), _) =>
        Validated.Valid(NamedValue(Field.normalizeName(field), f))
    }
  }
}
