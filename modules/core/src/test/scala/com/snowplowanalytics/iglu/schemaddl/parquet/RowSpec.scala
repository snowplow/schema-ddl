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

class RowSpec extends org.specs2.Specification { def is = s2"""
  castNonNull transforms any primitive value $e1
  castNonNull transforms object with matching primitive fields $e2
  castNonNull transforms object with missing nullable field $e3
  cast strips away undefined properties $e4
  cast transforms deeply nested object $e5
  cast does not stringify null when mode is Nullable $e6
  cast turns any unexpected type into string, when schema type is string $e7
  """ //TODO no e8, e9 and e10 like in BQ

  import FieldValue._
  
  def e1 = {
    val string = castNonNull(Type.String, json""""foo"""") must beValid(StringValue("foo"))
    val int = castNonNull(Type.Integer, json"-43") must beValid(IntValue(-43))
    val num = castNonNull(Type.Decimal(4, 2), json"-87.98") must beValid(DecimalValue(new java.math.BigDecimal("-87.98")))
    val bool = castNonNull(Type.Boolean, Json.fromBoolean(false)) must beValid(BooleanValue(false))
    string and int and num and bool
  }

  def e2 = {
    val inputJson = json"""{"foo": 42, "bar": true}"""
    val inputField = Type.Struct(List(
      Field("foo", Type.Integer, nullable = true),
      Field("bar", Type.Boolean, nullable = false)))
    
    val expected = StructValue(List(("foo", IntValue(42)), ("bar", BooleanValue(true))))
    castNonNull(inputField, inputJson) must beValid(expected)
  }

  def e3 = {
    val inputJson = json"""{"bar": true}"""
    val inputField = Type.Struct(List(
      Field("foo", Type.Integer, nullable = true),
      Field("bar", Type.Boolean, nullable = false)))
    
    val expected = StructValue(List(("foo", NullValue), ("bar", BooleanValue(true))))
    castNonNull(inputField, inputJson) must beValid(expected)
  }

  def e4 = {
    val inputJson =
      json"""{
        "someBool": true,
        "repeatedInt": [1,5,2],
        "undefined": 42
      }"""

    val inputField = Type.Struct(List(
      Field("someBool", Type.Boolean, nullable = false),
      Field("repeatedInt", Type.Array(Type.Integer, containsNull = false), nullable = false)))

    val expected = StructValue(List(("some_bool", BooleanValue(true)), ("repeated_int", ArrayValue(List(IntValue(1), IntValue(5), IntValue(2))))))
    castNonNull(inputField, inputJson) must beValid(expected)
  }

  def e5 = {
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
      Field("someBool", Type.Boolean, nullable = false),
      Field("nested", Type.Struct(List(
        Field("str", Type.String, nullable = false),
        Field("int", Type.Integer, nullable = true),
        Field("deep", Type.Struct(List(Field("str", Type.String, nullable = true))), nullable = false),
        Field("arr", Type.Array(Type.Struct(List(Field("a", Type.String, nullable = false))), containsNull = false), nullable = false)
      )), nullable = true)
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

  def e6 = {
    val inputJson =
      json"""{
        "optional": null
      }"""

    val inputField = Type.Struct(List(Field("optional", Type.String, nullable = true)))

    val expected = StructValue(List(("optional", NullValue)))
    castNonNull(inputField, inputJson) must beValid(expected)
  }

  def e7 = {
    val inputJson =
      json"""{
        "unionType": ["this", "is", "fallback", "strategy"]
      }"""

    val inputField = Type.Struct(List(Field("unionType", Type.String, nullable = true)))

    val expected = StructValue(List(("union_type", StringValue("""["this","is","fallback","strategy"]"""))))
    castNonNull(inputField, inputJson) must beValid(expected)
  }
}
