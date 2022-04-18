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

import com.snowplowanalytics.iglu.schemaddl.SpecHelpers
import com.snowplowanalytics.iglu.schemaddl.parquet.Type.Nullability.{Optional, Required}
import com.snowplowanalytics.iglu.schemaddl.parquet.Field.NullableType
import com.snowplowanalytics.iglu.schemaddl.parquet.Field.JsonNullability.NoNull

class FieldSpec extends org.specs2.Specification { def is = s2"""
  build generates field for object with string and object $e1
  build recognizes numeric properties $e2
  build generates repeated field for array $e3
  build generates repeated nullary field for array $e4
  build uses json-fallback strategy for union types $e5
  build uses json-fallback strategy for empty object $e6
  build recognized nullable union type $e7
  build generates repeated string for empty schema in items $e8
  build generates repeated record for nullable array $e9
  normalName handles camel case and disallowed characters $e10
  """

  def e1 = {
    val input = SpecHelpers.parseSchema(
      """
        |{"type": "object",
        |"properties": {
        |  "stringKey": {
        |    "type": "string",
        |    "maxLength": 500
        |  },
        |  "objectKey": {
        |    "type": "object",
        |    "properties": {
        |      "nestedKey1": { "type": "string" },
        |      "nestedKey2": { "type": ["integer", "null"] },
        |      "nestedKey3": { "type": "boolean" }
        |    },
        |    "required": ["nestedKey3"]
        |  }
        |}
        |}
      """.stripMargin)

    val expected =
      Type.Struct(List(
        Field(
          name = "objectKey",
          nullable = Optional,
          fieldType = Type.Struct(
            List(
              Field("nestedKey1", Type.String,  nullable = Optional), 
              Field("nestedKey2", Type.Long,    nullable = Optional), 
              Field("nestedKey3", Type.Boolean, nullable = Required),
            )
          ),
        ),
        Field("stringKey", Type.String, nullable = Optional))
      )

    Field.buildType(input) must beEqualTo(NullableType(expected, NoNull))
  }

  def e2 = {
    val input = SpecHelpers.parseSchema(
      """
        |{"type": "object",
        |"properties": {
        |  "numeric1": {"type": "number" },
        |  "numeric2": {"type": "integer" },
        |  "numeric3": {"type": ["number", "null"] },
        |  "numeric4": {"type": ["integer", "null", "number"] }
        |},
        |"required": ["numeric4", "numeric2"]
        |}
      """.stripMargin)

    val expected =
      Type.Struct(
        List(
          Field("numeric1", Type.Double, nullable = Optional),
          Field("numeric2", Type.Long,   nullable = Required), 
          Field("numeric3", Type.Double, nullable = Optional),
          Field("numeric4", Type.Double, nullable = Optional)
        )
      )

    Field.buildType(input) must beEqualTo(NullableType(expected, NoNull))
  }

  def e3 = {
    val input = SpecHelpers.parseSchema(
      """
        |{"type": "array",
        |"items": {
        |  "type": "object",
        |  "properties": {
        |    "foo": { "type": "string" },
        |    "bar": { "type": "integer" }
        |  }
        |}
        |}
      """.stripMargin)

    val expected =
      Type.Array(
        element = Type.Struct(List(
          Field("bar", Type.Long,   nullable = Optional),
          Field("foo", Type.String, nullable = Optional)
        )),
        nullable = Required
      )

    Field.buildType(input) must beEqualTo(NullableType(expected, NoNull))
  }

  def e4 = {
    val input = SpecHelpers.parseSchema(
      """
        |{"type": "array",
        |"items": {
        |  "type": ["object", "null"],
        |  "properties": {
        |    "foo": { "type": "string" },
        |    "bar": { "type": "integer" }
        |  }
        |}
        |}
      """.stripMargin)

    val expected =
      Type.Array(
        element = Type.Struct(List(
          Field("bar", Type.Long,   nullable = Optional),
          Field("foo", Type.String, nullable = Optional)
        )),
        nullable = Optional
      )

    Field.buildType(input) must beEqualTo(NullableType(expected, NoNull))
  }

  def e5 = {
    val input = SpecHelpers.parseSchema(
      """
        |{
        |  "type": "object",
        |  "properties": {
        |    "union": { "type": ["string", "integer"] }
        |  }
        |}
      """.stripMargin)

    val expected =
      Type.Struct(
        fields = List(
          Field(
            name = "union",
            fieldType = Type.Json,
            nullable = Optional
          )
        )
      )

    Field.buildType(input) must beEqualTo(NullableType(expected, NoNull))
  }

  def e6 = {
    val input = SpecHelpers.parseSchema(
      """
        |{
        |  "type": "object",
        |  "properties": { }
        |}
      """.stripMargin)

    val expected = Type.Json

    Field.buildType(input) must beEqualTo(NullableType(expected, NoNull))
  }

  def e7 = {
    val input = SpecHelpers.parseSchema(
      """
        |{
        |  "type": "object",
        |  "properties": {
        |    "union": { "type": ["string", "integer", "null"] }
        |  },
        |  "required": ["union"]
        |}
      """.stripMargin)

    val expected =
      Type.Struct(
        fields = List(
          Field(
            name = "union",
            fieldType = Type.Json,
            nullable = Optional
          )
        )
      )

    Field.buildType(input) must beEqualTo(NullableType(expected, NoNull))
  }

  def e8 = {
    val input = SpecHelpers.parseSchema(
      """
        |  {
        |    "type": "object",
        |    "properties": {
        |      "imp": {
        |        "type": "array",
        |        "items": {}
        |      }
        |    }
        |  }
      """.stripMargin)

    val expected =
      Type.Struct(
        fields = List(
          Field(
            name = "imp",
            fieldType = Type.Array(Type.Json, Optional),
            nullable = Optional
          )
        )
      )

    Field.buildType(input) must beEqualTo(NullableType(expected, NoNull))
  }

  def e9 = {
    val input = SpecHelpers.parseSchema(
      """
        |  {
        |    "type": "object",
        |    "properties": {
        |      "imp": {
        |        "type": ["array", "null"],
        |        "items": {"type": "boolean"}
        |      }
        |    }
        |  }
      """.stripMargin)

    val expected =
      Type.Struct(
        fields = List(
          Field(
            name = "imp",
            fieldType = Type.Array(Type.Boolean, Required),
            nullable = Optional
          )
        )
      )

    Field.buildType(input) must beEqualTo(NullableType(expected, NoNull))
  }

  def e10 = {
    (fieldNormalName("") must beEqualTo("")) and
      (fieldNormalName("test1") must beEqualTo("test1")) and
      (fieldNormalName("test1Test2Test3") must beEqualTo("test1_test2_test3")) and
      (fieldNormalName("Test1Test2TEST3") must beEqualTo("test1_test2_test3")) and
      (fieldNormalName("test1,test2.test3;test4") must beEqualTo("test1_test2_test3_test4")) and
      (fieldNormalName("1test1,test2.test3;test4") must beEqualTo("1test1_test2_test3_test4")) and 
      (fieldNormalName("_50test1,test2.test3;test4") must beEqualTo("_50test1_test2_test3_test4")) and
      (fieldNormalName("_.test1,test2.test3;test4") must beEqualTo("__test1_test2_test3_test4")) and
      (fieldNormalName(",.;:") must beEqualTo("____")) and
      (fieldNormalName("1test1,Test2Test3Test4.test5;test6") must beEqualTo("1test1_test2_test3_test4_test5_test6")) 
  }

  private def fieldNormalName(name: String) = Field.normalizeName(Field(name, Type.String, nullable = Optional))
}
