/*
 * Copyright (c) 2018-2022 Snowplow Analytics Ltd. All rights reserved.
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
import io.circe.literal._
import org.specs2.matcher.ValidatedMatchers._
import org.specs2.matcher.MatchResult
import com.snowplowanalytics.iglu.schemaddl.parquet.Type.Nullability.{Nullable, Required}
import com.snowplowanalytics.iglu.schemaddl.parquet.Type.DecimalPrecision.{Digits18, Digits38, Digits9}


class FieldValueSpec extends org.specs2.Specification { def is = s2"""
  cast transforms any primitive value $e1
  cast transforms a UTC timestamp value $e2
  cast transforms a non-UTC timestamp value $e3
  cast transforms a date value $e4
  cast transforms object with matching primitive fields $e5
  cast transforms object with missing nullable field $e6
  cast transforms array values $e7
  cast transforms nullable array values $e8
  cast strips away undefined properties $e9
  cast transforms deeply nested object $e10
  cast does not stringify null when mode is Nullable $e11
  cast turns any unexpected type into json, when schema type is json $e12
  cast does not transform unexpected JSON $e13
  cast transforms decimal values with correct scale and precision $e14
  cast does not transform decimal values with invalid scale or precision $e15
  """

  import FieldValue._

  // a helper
  def testCast(fieldType: Type, value: Json, expected: FieldValue): MatchResult[CastResult] =
    List(
      cast(Field("top", fieldType, Required))(value) must beValid(expected),
      cast(Field("top", fieldType, Nullable))(value) must beValid(expected),
      cast(Field("top", fieldType, Nullable))(Json.Null) must beValid[FieldValue](NullValue),
      cast(Field("top", fieldType, Required))(Json.Null) must beInvalid
    ).reduce(_ and _)

  def e1 = {
    List(
      testCast(Type.String, json""""foo"""", StringValue("foo")),
      testCast(Type.Integer, json"-43", IntValue(-43)),
      testCast(Type.Long, json"-43", LongValue(-43L)),
      testCast(Type.Long, json"2147483648", LongValue(2147483648L)),
      testCast(Type.Double, json"-43", DoubleValue(-43d)),
      testCast(Type.Double, json"-43.3", DoubleValue(-43.3d)),
      testCast(Type.Decimal(Digits9, 2), json"-87.98", DecimalValue(new java.math.BigDecimal("-87.98"), Digits9)),
      testCast(Type.Boolean, Json.fromBoolean(false), BooleanValue(false))
    ).reduce(_ and _)
  }

  def e2 = {
    val input = json""""2022-02-02T01:02:03.123z""""
    val expected = TimestampValue(java.sql.Timestamp.valueOf("2022-02-02 01:02:03.123"))
    testCast(Type.Timestamp, input, expected)
  }

  def e3 = {
    val input = json""""2022-02-02T12:02:03.123+03:00""""
    val expected = TimestampValue(java.sql.Timestamp.valueOf("2022-02-02 09:02:03.123"))
    testCast(Type.Timestamp, input, expected)
  }

  def e4 = {
    val input = json""""2022-02-02""""
    val expected = DateValue(java.sql.Date.valueOf("2022-02-02"))
    testCast(Type.Date, input, expected)
  }

  def e5 = {
    val inputJson = json"""{"foo": 42, "bar": true}"""
    val inputField = Type.Struct(List(
      Field("foo", Type.Integer, Nullable),
      Field("bar", Type.Boolean, Required)))
    
    val expected = StructValue(List(
      NamedValue("foo", IntValue(42)),
      NamedValue("bar", BooleanValue(true))
    ))
    testCast(inputField, inputJson, expected)
  }

  def e6 = {
    val inputJson = json"""{"bar": true}"""
    val inputField = Type.Struct(List(
      Field("foo", Type.Integer, Nullable),
      Field("bar", Type.Boolean, Required)))
    
    val expected = StructValue(List(
      NamedValue("foo", NullValue),
      NamedValue("bar", BooleanValue(true))
    ))
    testCast(inputField, inputJson, expected)
  }

  def e7 = {
    val inputJson = json"""["x", "y", "z"]"""
    val inputField = Type.Array(Type.String, Type.Nullability.Required)
    
    val expected = ArrayValue(List(StringValue("x"), StringValue("y"), StringValue("z")))
    testCast(inputField, inputJson, expected)
  }

  def e8 = {
    val inputJson = json"""["x", "y", null]"""
    val inputField = Type.Array(Type.String, Type.Nullability.Nullable)
    
    val expected = ArrayValue(List(StringValue("x"), StringValue("y"), NullValue))
    testCast(inputField, inputJson, expected)
  }

  def e9 = {
    val inputJson =
      json"""{
        "someBool": true,
        "repeatedInt": [1,5,2],
        "undefined": 42
      }"""

    val inputField = Type.Struct(List(
      Field("someBool", Type.Boolean, Required),
      Field("repeatedInt", Type.Array(Type.Integer, Required), Required)))

    val expected = StructValue(List(
      NamedValue("some_bool", BooleanValue(true)),
      NamedValue("repeated_int", ArrayValue(List(IntValue(1), IntValue(5), IntValue(2))))
    ))
    testCast(inputField, inputJson, expected)
  }

  def e10 = {
    val inputJson =
      json"""{
        "someBool": true,
        "nested": {
          "str": "foo bar",
          "int": 3,
          "deep": { "str": "foo" },
          "arr": [{"a": "b"}, {"a": "d", "b": "c"}]
        }
      }"""

    val inputField = Type.Struct(List(
      Field("someBool", Type.Boolean, Required),
      Field("nested", Type.Struct(List(
        Field("str", Type.String, Required),
        Field("int", Type.Integer, Nullable),
        Field("deep", Type.Struct(List(Field("str", Type.String, Nullable))), Required),
        Field("arr", Type.Array(Type.Struct(List(Field("a", Type.String, Required))), Required), Required)
      )), Nullable)
    ))

    val expected = StructValue(List(
      NamedValue("some_bool", BooleanValue(true)),
      NamedValue("nested", StructValue(List(
        NamedValue("str", StringValue("foo bar")),
        NamedValue("int", IntValue(3)),
        NamedValue("deep", StructValue(List(NamedValue("str", StringValue("foo"))))),
        NamedValue("arr", ArrayValue(List(
          StructValue(List(NamedValue("a", StringValue("b")))),
          StructValue(List(NamedValue("a", StringValue("d"))))
        )))
      )))
    ))

    testCast(inputField, inputJson, expected)
  }

  def e11 = {
    val inputJson =
      json"""{
        "optional": null
      }"""

    val inputField = Type.Struct(List(Field("optional", Type.String, Nullable)))

    val expected = StructValue(List(
      NamedValue("optional", NullValue)
    ))
    testCast(inputField, inputJson, expected)
  }

  def e12 = {
    val inputJson =
      json"""{
        "unionType": ["this", "is", "fallback", "strategy"]
      }"""

    val inputField = Type.Struct(List(Field("unionType", Type.Json, Nullable)))

    val expected = StructValue(List(
      NamedValue("union_type", JsonValue(json"""["this","is","fallback","strategy"]"""))
    ))
    testCast(inputField, inputJson, expected)
  }

  def e13 = {
    def testInvalidCast(fieldType: Type, value: Json) =
      cast(Field("top", fieldType, Required))(value) must beInvalid
    List(
      testInvalidCast(Type.String, json"""42"""),
      testInvalidCast(Type.String, json"""true"""),
      testInvalidCast(Type.Boolean, json""""hello""""),
      testInvalidCast(Type.Integer, json"2147483648"),
      testInvalidCast(Type.Integer, json"3.4"),
      testInvalidCast(Type.Long, json"9223372036854775808"),
      testInvalidCast(Type.Long, json"3.4"),
      testInvalidCast(Type.Decimal(Digits9, 2), json"123.123"),
      testInvalidCast(Type.Decimal(Digits9, 2), json"1234567890"),
      testInvalidCast(Type.Timestamp, json""""not a timestamp"""")
    ).reduce(_ and _)
  }

  def e14 = {
    def testDecimal(decimal: Type.Decimal, value: Json, expectedLong: Long, expectedScale: Int) =
      cast(Field("top", decimal, Required))(value) must beValid.like {
        case DecimalValue(bd, digits) =>
          (digits must_== decimal.precision) and
          (bd.underlying.unscaledValue.longValue must_== expectedLong) and
          (bd.scale must_== expectedScale)
      }
    List(
      testDecimal(Type.Decimal(Digits9, 2), json"87.98", 8798, 2),
      testDecimal(Type.Decimal(Digits9, 2), json"-87.98", -8798, 2),
      testDecimal(Type.Decimal(Digits9, 2), json"87.98000", 8798, 2),
      testDecimal(Type.Decimal(Digits9, 2), json"87.90000", 8790, 2),
      testDecimal(Type.Decimal(Digits9, 2), json"879", 87900, 2),
      testDecimal(Type.Decimal(Digits9, 10), json"0.00000001", 100, 10),

      testDecimal(Type.Decimal(Digits18, 2), json"879", 87900, 2),
      testDecimal(Type.Decimal(Digits18, 2), json"-879", -87900, 2),
      testDecimal(Type.Decimal(Digits18, 18), json"0.123456789123456789", 123456789123456789L, 18),

      testDecimal(Type.Decimal(Digits38, 2), json"879", 87900, 2),
      testDecimal(Type.Decimal(Digits38, 2), json"-879", -87900, 2),
    ).reduce(_ and _)
  }

  def e15 = {
    def testInvalidCast(decimal: Type.Decimal, value: Json) =
      cast(Field("top", decimal, Required))(value) must beInvalid
    List(
      testInvalidCast(Type.Decimal(Digits9, 2), json"""12.1234"""),
      testInvalidCast(Type.Decimal(Digits9, 2), json"""-12.1234"""),
      testInvalidCast(Type.Decimal(Digits9, 2), json"""123456789.12"""),
      testInvalidCast(Type.Decimal(Digits9, 2), json"""1000000000"""),
      testInvalidCast(Type.Decimal(Digits9, 2), json"""0.00001"""),
      testInvalidCast(Type.Decimal(Digits18, 2), json"""0.00001"""),
      testInvalidCast(Type.Decimal(Digits38, 2), json"""0.00001"""),
    ).reduce(_ and _)
  }

}
