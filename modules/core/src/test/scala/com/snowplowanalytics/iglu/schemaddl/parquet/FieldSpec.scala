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
package com.snowplowanalytics.iglu.schemaddl.parquet

import cats.data.NonEmptyVector

import com.snowplowanalytics.iglu.schemaddl.SpecHelpers
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.parquet.Type.Nullability.{Nullable, Required}

import org.specs2.matcher.MatchResult

class FieldSpec extends org.specs2.Specification { def is = s2"""
  build generates field for object with string and object $e1
  build recognizes numeric properties $e2
  build generates repeated field for array $e3
  build generates repeated nullary field for array $e4
  build uses json-fallback strategy for union types $e5
  build uses json-fallback strategy for empty object allowing additional properties $e6
  build recognized nullable union type $e7
  build generates repeated string for empty schema in items $e8
  build generates repeated record for nullable array $e9
  build returns nothing for empty object disallowing additional properties $e10
  build ignores nested field if it is an empty object disallowing additional properties $e11
  build returns nothing if the only nested field is an empty object disallowing additional properties $e12
  normalName handles camel case and disallowed characters $e13
  normalize would collapse colliding names $e14
  normalize would collapse colliding names with deterministic type selection $e15
  normalize would sort fields in order of name $e16
  normalize would sort nested fields in order of name $e17
  """

  // a helper
  def testBuild(input: Schema, expected: Type): MatchResult[Option[Field]] =
    List(
      Field.build("top", input, false) must beSome(beEqualTo(Field("top", expected, Nullable))),
      Field.build("top", input, true) must beSome(beEqualTo(Field("top", expected, Required))),
      Field.buildRepeated("top", input, true, Nullable) must beSome(beEqualTo(Field("top", Type.Array(expected, Required), Nullable))),
      Field.buildRepeated("top", input, true, Required) must beSome(beEqualTo(Field("top", Type.Array(expected, Required), Required))),
      Field.buildRepeated("top", input, false, Nullable) must beSome(beEqualTo(Field("top", Type.Array(expected, Nullable), Nullable))),
      Field.buildRepeated("top", input, false, Required) must beSome(beEqualTo(Field("top", Type.Array(expected, Nullable), Required))),
    ).reduce(_ and _)

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
      Type.Struct(NonEmptyVector.of(
        Field(
          name = "objectKey",
          nullability = Nullable,
          fieldType = Type.Struct(
            NonEmptyVector.of(
              Field("nestedKey1", Type.String,  nullability = Nullable),
              Field("nestedKey2", Type.Long,    nullability = Nullable),
              Field("nestedKey3", Type.Boolean, nullability = Required),
            )
          ),
        ),
        Field("stringKey", Type.String, nullability = Nullable))
      )

    testBuild(input, expected)
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
        NonEmptyVector.of(
          Field("numeric1", Type.Double, nullability = Nullable),
          Field("numeric2", Type.Long,   nullability = Required),
          Field("numeric3", Type.Double, nullability = Nullable),
          Field("numeric4", Type.Double, nullability = Nullable)
        )
      )

    testBuild(input, expected)
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
        element = Type.Struct(NonEmptyVector.of(
          Field("bar", Type.Long,   nullability = Nullable),
          Field("foo", Type.String, nullability = Nullable)
        )),
        nullability = Required
      )

    testBuild(input, expected)
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
        element = Type.Struct(NonEmptyVector.of(
          Field("bar", Type.Long,   nullability = Nullable),
          Field("foo", Type.String, nullability = Nullable)
        )),
        nullability = Nullable
      )

    testBuild(input, expected)
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
        fields = NonEmptyVector.of(
          Field(
            name = "union",
            fieldType = Type.Json,
            nullability = Nullable
          )
        )
      )

    testBuild(input, expected)
  }

  def e6 = {
    val input = SpecHelpers.parseSchema(
      """
        |{
        |  "type": "object",
        |  "properties": { },
        |  "additionalProperties": true
        |}
      """.stripMargin)

    val expected = Type.Json

    testBuild(input, expected)
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
        fields = NonEmptyVector.of(
          Field(
            name = "union",
            fieldType = Type.Json,
            nullability = Nullable
          )
        )
      )

    testBuild(input, expected)
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
        fields = NonEmptyVector.of(
          Field(
            name = "imp",
            fieldType = Type.Array(Type.Json, Nullable),
            nullability = Nullable
          )
        )
      )

    testBuild(input, expected)
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
        fields = NonEmptyVector.of(
          Field(
            name = "imp",
            fieldType = Type.Array(Type.Boolean, Required),
            nullability = Nullable
          )
        )
      )

    testBuild(input, expected)
  }

  def e10 = {
    val input = SpecHelpers.parseSchema(
      """
        |{
        |  "type": "object",
        |  "properties": { },
        |  "additionalProperties": false
        |}
      """.stripMargin)

    List(
      Field.build("top", input, false) must beNone,
      Field.build("top", input, true) must beNone,
      Field.buildRepeated("top", input, true, Nullable) must beNone,
      Field.buildRepeated("top", input, true, Required) must beNone,
      Field.buildRepeated("top", input, false, Nullable) must beNone,
      Field.buildRepeated("top", input, false, Required) must beNone
    ).reduce(_ and _)
  }


  def e11 = {
    val input = SpecHelpers.parseSchema(
      """
        |{"type": "object",
        |"properties": {
        |  "objectKey": {
        |    "type": "object",
        |    "properties": {
        |      "nestedKey1": { "type": "string" },
        |      "shouldBeRemoved": { "type": "object", "additionalProperties": false }
        |    }
        |  }
        |}
        |}
      """.stripMargin)

    val expected =
      Type.Struct(NonEmptyVector.of(
        Field(
          name = "objectKey",
          nullability = Nullable,
          fieldType = Type.Struct(
            NonEmptyVector.of(
              Field("nestedKey1", Type.String,  nullability = Nullable)
            )
          )
        )
      ))

    testBuild(input, expected)
  }

  def e12 = {
    val input = SpecHelpers.parseSchema(
      """
        |{"type": "object",
        |"properties": {
        |  "objectKey": {
        |    "type": "object",
        |    "properties": {
        |      "shouldBeRemoved": { "type": "object", "additionalProperties": false }
        |    }
        |  }
        |}
        |}
      """.stripMargin)
    List(
      Field.build("top", input, false) must beNone,
      Field.build("top", input, true) must beNone,
      Field.buildRepeated("top", input, true, Nullable) must beNone,
      Field.buildRepeated("top", input, true, Required) must beNone,
      Field.buildRepeated("top", input, false, Nullable) must beNone,
      Field.buildRepeated("top", input, false, Required) must beNone
    ).reduce(_ and _)
  }

  def e13 = {
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

  def e14 = {
    Field.normalize(Field(
      name = "top",
      fieldType = Type.Struct(
        fields = NonEmptyVector.of(
          Field(
            name = "_ga",
            fieldType = Type.Integer,
            nullability = Nullable
          ),
          Field(
            name = "__b",
            fieldType = Type.Integer,
            nullability = Nullable
          ),
          Field(
            name = "_Ga",
            fieldType = Type.Integer,
            nullability = Nullable
          )
        )
      ),
      nullability = Nullable)) must equalTo(Field(
      name = "top",
      fieldType = Type.Struct(
        fields = NonEmptyVector.of(
          Field(
            name = "__b",
            fieldType = Type.Integer,
            nullability = Nullable
          ),
          Field(
            name = "_ga",
            fieldType = Type.Integer,
            nullability = Nullable,
            accessors = Set("_ga", "_Ga")
          ),
        )
      ),
      nullability = Nullable))
  }

  def e15 = {
    val fields = NonEmptyVector.of(
      Field(
        name = "xyz",
        fieldType = Type.Integer,
        nullability = Nullable
      ),
      Field(
        name = "XYZ",
        fieldType = Type.String,
        nullability = Nullable
      )
    )

    val input1 = Field.normalize(Field(name = "top", fieldType = Type.Struct( fields = fields), nullability = Nullable))
    val input2 = Field.normalize(Field(name = "top", fieldType = Type.Struct( fields = fields.reverse), nullability = Nullable))

    val expected = Field(
      name = "top",
      fieldType = Type.Struct(
        fields = NonEmptyVector.of(
          Field(
            name = "xyz",
            fieldType = Type.String,
            nullability = Nullable,
            accessors = Set("xyz", "XYZ")
          ),
        )
      ),
      nullability = Nullable)

    (input1 must_== expected) and (input2 must_== expected)
  }

  def e16 = {
    val input = {
      val struct = Type.Struct(
        NonEmptyVector.of(
          Field("Apple", Type.String, Nullable),
          Field("cherry", Type.String, Nullable),
          Field("banana", Type.String, Nullable),
          Field("Damson", Type.String, Nullable)
        )
      )
      Field("top", struct, Nullable)
    }

    val expected = {
      val struct = Type.Struct(
        NonEmptyVector.of(
          Field("apple", Type.String, Nullable, Set("Apple")),
          Field("banana", Type.String, Nullable, Set("banana")),
          Field("cherry", Type.String, Nullable, Set("cherry")),
          Field("damson", Type.String, Nullable, Set("Damson"))
        )
      )
      Field("top", struct, Nullable)
    }

    Field.normalize(input) must beEqualTo(expected)
  }

  def e17 = {
    val input = {
      val nested = Type.Struct(
        NonEmptyVector.of(
          Field("Apple", Type.String, Nullable),
          Field("cherry", Type.String, Nullable),
          Field("banana", Type.String, Nullable),
          Field("Damson", Type.String, Nullable)
        )
      )
      val arr = Field("nested_arr", Type.Array(nested, Nullable), Nullable)
      val struct = Field("nested_obj", nested, Nullable)
      Field("top", Type.Struct(NonEmptyVector.of(arr, struct)), Nullable)
    }

    val expected = {
      val nested = Type.Struct(
        NonEmptyVector.of(
          Field("apple", Type.String, Nullable, Set("Apple")),
          Field("banana", Type.String, Nullable, Set("banana")),
          Field("cherry", Type.String, Nullable, Set("cherry")),
          Field("damson", Type.String, Nullable, Set("Damson"))
        )
      )
      val arr = Field("nested_arr", Type.Array(nested, Nullable), Nullable)
      val struct = Field("nested_obj", nested, Nullable)
      Field("top", Type.Struct(NonEmptyVector.of(arr, struct)), Nullable)
    }

    Field.normalize(input) must beEqualTo(expected)
  }

  private def fieldNormalName(name: String) = Field.normalizeName(Field(name, Type.String, nullability = Nullable))
}
