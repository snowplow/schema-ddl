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
import cats.data.NonEmptyList

import java.time.{Instant, LocalDate}

/** Run-time value, conforming [[Field]] (type) */
@deprecated("Use `Caster` instead", "0.20.0")
sealed trait FieldValue extends Product with Serializable

@deprecated("Use `Caster` instead", "0.20.0")
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

  private val caster: Caster[FieldValue] = new Caster[FieldValue] {
    def nullValue: FieldValue = NullValue
    def jsonValue(v: Json): FieldValue = JsonValue(v)
    def stringValue(v: String): FieldValue = StringValue(v)
    def booleanValue(v: Boolean): FieldValue = BooleanValue(v)
    def intValue(v: Int): FieldValue = IntValue(v)
    def longValue(v: Long): FieldValue = LongValue(v)
    def doubleValue(v: Double): FieldValue = DoubleValue(v)
    def decimalValue(unscaled: BigInt, details: Type.Decimal): FieldValue =
      DecimalValue(BigDecimal(unscaled, details.scale), details.precision)
    def dateValue(v: LocalDate): FieldValue = DateValue(java.sql.Date.valueOf(v))
    def timestampValue(v: Instant): FieldValue = TimestampValue(java.sql.Timestamp.from(v))
    def structValue(vs: NonEmptyList[Caster.NamedValue[FieldValue]]): FieldValue =
      StructValue {
        vs.toList.map {
          case Caster.NamedValue(n, v) => NamedValue(n, v)
        }
      }
    def arrayValue(vs: List[FieldValue]): FieldValue = ArrayValue(vs)
  }

  /**
    * Turn JSON  value into Parquet-compatible row, matching schema defined in `field`
    * Top-level function, called only one columns
    * Performs following operations in order to prevent runtime insert failure:
    * * removes unexpected additional properties
    * * turns all unexpected types into string
    */
  def cast(field: Field)(value: Json): CastResult =
    Caster.cast(caster, field, value)
}
