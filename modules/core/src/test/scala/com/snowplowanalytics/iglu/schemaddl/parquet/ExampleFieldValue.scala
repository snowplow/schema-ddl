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

import io.circe.Json

import java.time.{LocalDate, Instant}

/** ADT used for testing the Caster functionality */
sealed trait ExampleFieldValue extends Product with Serializable

object ExampleFieldValue {
  case object NullValue extends ExampleFieldValue
  case class JsonValue(value: Json) extends ExampleFieldValue
  case class StringValue(value: String) extends ExampleFieldValue
  case class BooleanValue(value: Boolean) extends ExampleFieldValue
  case class IntValue(value: Int) extends ExampleFieldValue
  case class LongValue(value: Long) extends ExampleFieldValue
  case class DoubleValue(value: Double) extends ExampleFieldValue
  case class DecimalValue(value: BigDecimal, precision: Type.DecimalPrecision) extends ExampleFieldValue
  case class TimestampValue(value: Instant) extends ExampleFieldValue
  case class DateValue(value: java.sql.Date) extends ExampleFieldValue
  case class StructValue(values: List[Caster.NamedValue[ExampleFieldValue]]) extends ExampleFieldValue
  case class ArrayValue(values: List[ExampleFieldValue]) extends ExampleFieldValue

  val caster: Caster[ExampleFieldValue] = new Caster[ExampleFieldValue] {
    def nullValue: ExampleFieldValue = NullValue
    def jsonValue(v: Json): ExampleFieldValue = JsonValue(v)
    def stringValue(v: String): ExampleFieldValue = StringValue(v)
    def booleanValue(v: Boolean): ExampleFieldValue = BooleanValue(v)
    def intValue(v: Int): ExampleFieldValue = IntValue(v)
    def longValue(v: Long): ExampleFieldValue = LongValue(v)
    def doubleValue(v: Double): ExampleFieldValue = DoubleValue(v)
    def decimalValue(unscaled: BigInt, details: Type.Decimal): ExampleFieldValue =
      DecimalValue(BigDecimal(unscaled, details.scale), details.precision)
    def dateValue(v: LocalDate): ExampleFieldValue = DateValue(java.sql.Date.valueOf(v))
    def timestampValue(v: Instant): ExampleFieldValue = TimestampValue(v)
    def structValue(vs: List[Caster.NamedValue[ExampleFieldValue]]): ExampleFieldValue = StructValue(vs)
    def arrayValue(vs: List[ExampleFieldValue]): ExampleFieldValue = ArrayValue(vs)
  }
}
