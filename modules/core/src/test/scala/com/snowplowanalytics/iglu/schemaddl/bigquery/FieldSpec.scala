/*
 * Copyright (c) 2018-2023 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.iglu.schemaddl
package bigquery

class FieldSpec extends org.specs2.Specification { def is = s2"""
  build generates field for object with string and object $e1
  build recognizes numeric properties $e2
  build generates repeated field for array $e3
  build generates repeated nullary field for array $e4
  build uses string-fallback strategy for union types $e5
  build recognized nullable union type $e6
  build generates repeated string for empty schema in items $e7
  build generates repeated record for nullable array $e8
  normalName handles camel case and disallowed characters $e9
  build generates nullable field for oneOf types $e10
  build generates nullable field for nullable object without nested keys $e11
  build generates nullable field for nullable array without items $e12
  build generates numeric/decimal for enums $e13
  build generates numeric/decimal for multipleof $e14
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

    val expected = Field(
      "foo",
      Type.Record(List(
        Field("objectKey",
          Type.Record(List(
            Field("nestedKey3", Type.Boolean, Mode.Required),
            Field("nestedKey1", Type.String, Mode.Nullable),
            Field("nestedKey2", Type.Integer, Mode.Nullable)
          )),
          Mode.Nullable
        ),
        Field("stringKey", Type.String, Mode.Nullable))),
      Mode.Nullable
    )

    Field.build("foo", input, false) must beEqualTo(expected)
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

    val expected = Field(
      "foo",
      Type.Record(List(
        Field("numeric2", Type.Integer, Mode.Required),
        Field("numeric1", Type.Float, Mode.Nullable),
        Field("numeric3", Type.Float, Mode.Nullable),
        Field("numeric4", Type.Float, Mode.Nullable)
      )),
      Mode.Nullable
    )

    Field.build("foo", input, false) must beEqualTo(expected)
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

    val expected = Field(
      "foo",
      Type.Record(List(
        Field("bar", Type.Integer, Mode.Nullable),
        Field("foo", Type.String, Mode.Nullable)
      )),
      Mode.Repeated
    )

    Field.build("foo", input, false) must beEqualTo(expected)
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

    val expected = Field(
      "foo",
      Type.Record(List(
        Field("bar", Type.Integer, Mode.Nullable),
        Field("foo", Type.String, Mode.Nullable)
      )),
      Mode.Repeated
    )

    Field.build("foo", input, false) must beEqualTo(expected)
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

    val expected = Field("foo", Type.Record(List(
      Field("union", Type.String, Mode.Nullable)
    )), Mode.Nullable)

    Field.build("foo", input, false) must beEqualTo(expected)
  }

  def e6 = {
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

    val expected = Field("foo", Type.Record(List(
      Field("union", Type.String, Mode.Nullable)
    )), Mode.Nullable)

    Field.build("foo", input, false) must beEqualTo(expected)
  }

  def e7 = {
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

    val expected = Field("arrayTest", Type.Record(List(Field("imp", Type.String, Mode.Repeated))), Mode.Required)
    Field.build("arrayTest", input, true) must beEqualTo(expected)
  }

  def e8 = {
    val input = SpecHelpers.parseSchema(
      """
        |  {
        |    "type": "object",
        |    "properties": {
        |      "imp": {
        |        "type": ["array", "null"],
        |        "items": { }
        |      }
        |    }
        |  }
      """.stripMargin)

    val expected = Field("arrayTest", Type.Record(List(Field("imp", Type.String, Mode.Repeated))), Mode.Required)
    Field.build("arrayTest", input, true) must beEqualTo(expected)
  }

  def e9 = {
    (fieldNormalName("") must beEqualTo("")) and
      (fieldNormalName("test1") must beEqualTo("test1")) and
      (fieldNormalName("test1Test2Test3") must beEqualTo("test1_test2_test3")) and
      (fieldNormalName("Test1Test2TEST3") must beEqualTo("test1_test2_test3")) and
      (fieldNormalName("test1,test2.test3;test4") must beEqualTo("test1_test2_test3_test4")) and
      (fieldNormalName("1test1,test2.test3;test4") must beEqualTo("_1test1_test2_test3_test4")) and
      (fieldNormalName("_50test1,test2.test3;test4") must beEqualTo("_50test1_test2_test3_test4")) and
      (fieldNormalName("_.test1,test2.test3;test4") must beEqualTo("__test1_test2_test3_test4")) and
      (fieldNormalName(",.;:") must beEqualTo("____")) and
      (fieldNormalName("1test1,Test2Test3Test4.test5;test6") must beEqualTo("_1test1_test2_test3_test4_test5_test6"))
  }

  def e10 = {
    val input = SpecHelpers.parseSchema(
      """
        |  {
        |    "type": "object",
        |    "required": ["xyz"],
        |    "properties": {
        |      "xyz": {
        |        "oneOf": [
        |           {"type": "string"},
        |           {"type": "number"},
        |           {"type": "null"}
        |        ]
        |      }
        |    }
        |  }
      """.stripMargin)

    val expected = Field(
      "foo",
      Type.Record(List(
        Field("xyz", Type.String, Mode.Nullable)
      )),
      Mode.Nullable
    )

    Field.build("foo", input, false) must beEqualTo(expected)
  }

  def e11 = {
    val input = SpecHelpers.parseSchema(
      """
        |  {
        |    "type": "object",
        |    "required": ["xyz"],
        |    "properties": {
        |      "xyz": {
        |        "type": ["object", "null"]
        |      }
        |    }
        |  }
      """.stripMargin)

    val expected = Field(
      "foo",
      Type.Record(List(
        Field("xyz", Type.String, Mode.Nullable)
      )),
      Mode.Nullable
    )

    Field.build("foo", input, false) must beEqualTo(expected)
  }

  def e12 = {
    val input = SpecHelpers.parseSchema(
      """
        |  {
        |    "type": "object",
        |    "required": ["xyz"],
        |    "properties": {
        |      "xyz": {
        |        "type": ["array", "null"]
        |      }
        |    }
        |  }
      """.stripMargin)

    val expected = Field(
      "foo",
      Type.Record(List(
        Field("xyz", Type.String, Mode.Nullable)
      )),
      Mode.Nullable
    )

    Field.build("foo", input, false) must beEqualTo(expected)
  }

  def e13 = {
    val input = SpecHelpers.parseSchema(
      """
        |  {
        |    "type": "object",
        |    "required": ["xyz"],
        |    "properties": {
        |      "xyz": {
        |        "enum": [10, 1.12, 1e9]
        |      }
        |    }
        |  }
      """.stripMargin)

    val expected = Field(
      "foo",
      Type.Record(List(
        Field("xyz", Type.Numeric(12,2), Mode.Required)
      )),
      Mode.Nullable
    )

    Field.build("foo", input, false) must beEqualTo(expected)
  }

  def e14 = {
    val input = SpecHelpers.parseSchema(
      """
        |  {
        |    "type": "object",
        |    "required": ["xyz"],
        |    "properties": {
        |      "xyz": {
        |        "type": ["number", "null"],
        |        "multipleOf": 0.001,
        |        "maximum": 2,
        |        "minimum": 1
        |      }
        |    }
        |  }
      """.stripMargin)

    val expected = Field(
      "foo",
      Type.Record(List(
        Field("xyz", Type.Numeric(4,3), Mode.Nullable)
      )),
      Mode.Nullable
    )

    Field.build("foo", input, false) must beEqualTo(expected)
  }
  private def fieldNormalName(name: String) = Field(name, Type.String, Mode.Nullable).normalName
}
