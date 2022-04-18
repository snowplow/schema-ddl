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

import com.snowplowanalytics.iglu.schemaddl.parquet.Type.Nullability.{Optional, Required}
import com.snowplowanalytics.iglu.schemaddl.parquet.Type.DecimalPrecision.Digits9

class FieldValueSpec extends org.specs2.Specification { def is = s2"""
  castNonNull transforms any primitive value $e1
  castNonNull transforms a UTC timestamp value $e2
  castNonNull transforms a non-UTC timestamp value $e3
  castNonNull transforms a date value $e4
  castNonNull transforms object with matching primitive fields $e5
  castNonNull transforms object with missing nullable field $e6
  castNonNull transforms array values $e7
  castNonNull transforms nullable array values $e8
  castNonNull strips away undefined properties $e9
  castNonNull transforms deeply nested object $e10
  castNonNull does not stringify null when mode is Nullable $e11
  castNonNull turns any unexpected type into json, when schema type is json $e12
  castNonNull does not transform unexpected JSON $e13
  """

  import FieldValue._
  
  def e1 = {
    List(
      castNonNull(Type.String, json""""foo"""") must beValid(StringValue("foo")),
      castNonNull(Type.Integer, json"-43") must beValid(IntValue(-43)),
      castNonNull(Type.Long, json"-43") must beValid(LongValue(-43l)),
      castNonNull(Type.Long, json"2147483648") must beValid(LongValue(2147483648l)),
      castNonNull(Type.Double, json"-43") must beValid(DoubleValue(-43d)),
      castNonNull(Type.Double, json"-43.3") must beValid(DoubleValue(-43.3d)),
      castNonNull(Type.Decimal(Digits9, 2), json"-87.98") must beValid(DecimalValue(new java.math.BigDecimal("-87.98"))),
      castNonNull(Type.Boolean, Json.fromBoolean(false)) must beValid(BooleanValue(false))
    ).reduce(_ and _)
  }

  def e2 = {
    val input = json""""2022-02-02T01:02:03.123z""""
    val expected = TimestampValue(java.sql.Timestamp.valueOf("2022-02-02 01:02:03.123"))
    castNonNull(Type.Timestamp, input) must beValid(expected)
  }

  def e3 = {
    val input = json""""2022-02-02T12:02:03.123+03:00""""
    val expected = TimestampValue(java.sql.Timestamp.valueOf("2022-02-02 09:02:03.123"))
    castNonNull(Type.Timestamp, input) must beValid(expected)
  }

  def e4 = {
    val input = json""""2022-02-02""""
    val expected = DateValue(java.sql.Date.valueOf("2022-02-02"))
    castNonNull(Type.Date, input) must beValid(expected)
  }

  def e5 = {
    val inputJson = json"""{"foo": 42, "bar": true}"""
    val inputField = Type.Struct(List(
      Field("foo", Type.Integer, Optional),
      Field("bar", Type.Boolean, Required)))
    
    val expected = StructValue(List(("foo", IntValue(42)), ("bar", BooleanValue(true))))
    castNonNull(inputField, inputJson) must beValid(expected)
  }

  def e6 = {
    val inputJson = json"""{"bar": true}"""
    val inputField = Type.Struct(List(
      Field("foo", Type.Integer, Optional),
      Field("bar", Type.Boolean, Required)))
    
    val expected = StructValue(List(("foo", NullValue), ("bar", BooleanValue(true))))
    castNonNull(inputField, inputJson) must beValid(expected)
  }

  def e7 = {
    val inputJson = json"""["x", "y", "z"]"""
    val inputField = Type.Array(Type.String, Type.Nullability.Required)
    
    val expected = ArrayValue(List(StringValue("x"), StringValue("y"), StringValue("z")))
    castNonNull(inputField, inputJson) must beValid(expected)
  }

  def e8 = {
    val inputJson = json"""["x", "y", null]"""
    val inputField = Type.Array(Type.String, Type.Nullability.Optional)
    
    val expected = ArrayValue(List(StringValue("x"), StringValue("y"), NullValue))
    castNonNull(inputField, inputJson) must beValid(expected)
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

    val expected = StructValue(List(("some_bool", BooleanValue(true)), ("repeated_int", ArrayValue(List(IntValue(1), IntValue(5), IntValue(2))))))
    castNonNull(inputField, inputJson) must beValid(expected)
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
        Field("int", Type.Integer, Optional),
        Field("deep", Type.Struct(List(Field("str", Type.String, Optional))), Required),
        Field("arr", Type.Array(Type.Struct(List(Field("a", Type.String, Required))), Required), Required)
      )), Optional)
    ))

    val expected = StructValue(List(
      ("some_bool", BooleanValue(true)),
      ("nested", StructValue(List(
        ("str", StringValue("foo bar")),
        ("int", IntValue(3)),
        ("deep", StructValue(List(("str", StringValue("foo"))))),
        ("arr", ArrayValue(List(StructValue(List(("a", StringValue("b")))), StructValue(List(("a", StringValue("d")))))))
      )))
    ))

    castNonNull(inputField, inputJson) must beValid(expected)
  }

  def e11 = {
    val inputJson =
      json"""{
        "optional": null
      }"""

    val inputField = Type.Struct(List(Field("optional", Type.String, Optional)))

    val expected = StructValue(List(("optional", NullValue)))
    castNonNull(inputField, inputJson) must beValid(expected)
  }

  def e12 = {
    val inputJson =
      json"""{
        "unionType": ["this", "is", "fallback", "strategy"]
      }"""

    val inputField = Type.Struct(List(Field("unionType", Type.Json, Optional)))

    val expected = StructValue(List(("union_type", JsonValue(json"""["this","is","fallback","strategy"]"""))))
    castNonNull(inputField, inputJson) must beValid(expected)
  }

  def e13 = {
    List(
      castNonNull(Type.String, json"""42""") must beInvalid,
      castNonNull(Type.String, json"""true""") must beInvalid,
      castNonNull(Type.Boolean, json""""hello"""") must beInvalid,
      castNonNull(Type.Integer, json"2147483648") must beInvalid,
      castNonNull(Type.Integer, json"3.4") must beInvalid,
      castNonNull(Type.Long, json"9223372036854775808") must beInvalid,
      castNonNull(Type.Long, json"3.4") must beInvalid,
      castNonNull(Type.Decimal(Digits9, 2), json"123.123") must beInvalid,
      castNonNull(Type.Decimal(Digits9, 2), json"1234567890") must beInvalid,
      castNonNull(Type.Timestamp, json""""not a timestamp"""") must beInvalid
    ).reduce(_ and _)
  }
}
