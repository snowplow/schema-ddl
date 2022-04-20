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
      Type.Struct(List(
        Field("objectKey",
           Type.Struct(List(
             Field("nestedKey1", Type.String,  nullable = true), //TODO is is correct order? No ordering by 'mode' as in BQ
             Field("nestedKey2", Type.Long,    nullable = true), //TODO is long correct?
             Field("nestedKey3", Type.Boolean, nullable = false),
          )),
          nullable = true 
        ),
        Field("stringKey", Type.String, nullable = true))),
      nullable = true 
    )

    Field.build("foo", input, required = false) must beEqualTo(expected)
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
       Type.Struct(List(
        Field("numeric1", Type.Double, nullable = true), //TODO is it correct order? No ordering by 'mode' as in BQ
        Field("numeric2", Type.Long,   nullable = false), //TODO is long correct?
        Field("numeric3", Type.Double, nullable = true),
        Field("numeric4", Type.Double, nullable = true)
      )),
      nullable = true 
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
      "some_field",
      Type.Array(
        element = Type.Struct(List(
          Field("bar", Type.Long,   nullable = true), //TODO is long correct?
          Field("foo", Type.String, nullable = true)
        )),
        containsNull = false 
      ),
      nullable = true
    )

    Field.build("some_field", input, required = false) must beEqualTo(expected)
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
      "some_field",
      Type.Array(
        element = Type.Struct(List(
          Field("bar", Type.Long,   nullable = true), //TODO is long correct?
          Field("foo", Type.String, nullable = true)
        )),
        containsNull = true
      ),
      nullable = true
    )
    Field.build("some_field", input, required = false) must beEqualTo(expected)
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

    val expected = Field(
      name = "foo",
      fieldType = Type.Struct(
        fields = List(
          Field(
            name = "union",
            fieldType = Type.String,
            nullable = true 
          )
        )
      ),
      nullable = true
    )

    Field.build("foo", input, required = false) must beEqualTo(expected)
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
    
    val expected = Field(
      name = "foo",
      fieldType = Type.Struct(
        fields = List(
          Field(
            name = "union",
            fieldType = Type.String,
            nullable = true 
          )
        )
      ),
      nullable = true
    )

    Field.build("foo", input, required = false) must beEqualTo(expected)
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

    val expected = Field(
      name = "arrayTest",
      fieldType = Type.Struct(
        fields = List(
          Field(
            name = "imp",
            fieldType = Type.Array(Type.String, containsNull = false), //TODO is false correct?
            nullable = true
          )
        )
      ),
      nullable = false
    )
    Field.build("arrayTest", input, required = true) must beEqualTo(expected)
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
    
    val expected = Field(
      name = "arrayTest",
      fieldType = Type.Struct(
        fields = List(
          Field(
            name = "imp",
            fieldType = Type.Array(Type.String, containsNull = false), //TODO is false correct?
            nullable = true
          )
        )
      ),
      nullable = false
    )
    Field.build("arrayTest", input, required = true) must beEqualTo(expected)
  }

  def e9 = {
    (fieldNormalName("") must beEqualTo("")) and
      (fieldNormalName("test1") must beEqualTo("test1")) and
      (fieldNormalName("test1Test2Test3") must beEqualTo("test1_test2_test3")) and
      (fieldNormalName("Test1Test2TEST3") must beEqualTo("test1_test2_test3")) and
      (fieldNormalName("test1,test2.test3;test4") must beEqualTo("test1_test2_test3_test4")) and
      (fieldNormalName("1test1,test2.test3;test4") must beEqualTo("1test1_test2_test3_test4")) and //TODO '_' at the beginning?
      (fieldNormalName("_50test1,test2.test3;test4") must beEqualTo("_50test1_test2_test3_test4")) and
      (fieldNormalName("_.test1,test2.test3;test4") must beEqualTo("__test1_test2_test3_test4")) and
      (fieldNormalName(",.;:") must beEqualTo("____")) and
      (fieldNormalName("1test1,Test2Test3Test4.test5;test6") must beEqualTo("1test1_test2_test3_test4_test5_test6")) //TODO '_' at the beginning?
  }

  private def fieldNormalName(name: String) = Field(name, Type.String, nullable = true).normalName
}