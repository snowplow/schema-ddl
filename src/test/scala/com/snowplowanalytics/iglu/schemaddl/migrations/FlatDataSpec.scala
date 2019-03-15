/*
 * Copyright (c) 2016-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.iglu.schemaddl.migrations

import cats.data.NonEmptyList

import io.circe.literal._

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaMap, SchemaVer, SelfDescribingSchema}

import org.specs2.mutable.Specification

import com.snowplowanalytics.iglu.schemaddl.SpecHelpers._

class FlatDataSpec extends Specification {
  "getPath" should {
    "getPath goes into nested data" >> {
      val path = "/properties/foo/properties/bar".jsonPointer.forData
      val data = json"""{"foo": {"bar": "success"}}"""

      FlatData.getPath(path, data, None) must beEqualTo("success")
    }
  }

  "flatten" should {
    "process complex schema list" >> {
      val schemaA =
        json"""{"type": "object", "additionalProperties": false, "properties": {
        "a": {"type": "string"}
      }}""".schema
      val schemaB =
        json"""{"type": "object", "additionalProperties": false, "properties": {
        "a": {"type": "string"},
        "b": {"type": "object", "additionalProperties": false, "properties": {"ba": {}}}
      }}""".schema
      val schemaC =
        json"""{"type": "object", "additionalProperties": false, "properties": {
        "a": {"type": "string"},
        "b": {"type": "object", "additionalProperties": false, "properties": {"ba": {}}},
        "c": {"type": "integer"}
      }}""".schema
      val schemaD =
        json"""{"type": "object", "additionalProperties": false, "properties": {
        "a": {"type": "string"},
        "b": {"type": "object",  "additionalProperties": false, "properties": {"ba": {}, "bb": {}}},
        "c": {"type": "integer"}
      }}""".schema

      val schemas = NonEmptyList.of(
        SelfDescribingSchema(SchemaMap(SchemaKey("com.acme", "test", "jsonschema", SchemaVer.Full(1,0,0))), schemaA),
        SelfDescribingSchema(SchemaMap(SchemaKey("com.acme", "test", "jsonschema", SchemaVer.Full(1,0,1))), schemaB),
        SelfDescribingSchema(SchemaMap(SchemaKey("com.acme", "test", "jsonschema", SchemaVer.Full(1,0,2))), schemaC),
        SelfDescribingSchema(SchemaMap(SchemaKey("com.acme", "test", "jsonschema", SchemaVer.Full(1,0,3))), schemaD)
      )

      val schemaList = SchemaList.buildMultiple(schemas).right.get.head

      val expected = List("one", "two", "three", "four")

      val data = json"""{"a": "one", "b": {"ba": "two", "bb": "four"}, "c": "three"}"""
      val result = FlatData.flatten(data, schemaList, None)

      result must beEqualTo(expected)
    }

    "process schema which contains oneOf" >> {
      val json = json"""
      {
        "type": "object",
        "properties": {
          "union": {
            "oneOf": [
              {
                "type": "object",
                "properties": {
                  "object_without_properties": { "type": "object" }
                }
              },
              {
                "type": "string"
              }
            ]
          }
        },
        "required": ["union"],
        "additionalProperties": false
      }
    """.schema

      val schema =
        SelfDescribingSchema(SchemaMap(SchemaKey("com.acme", "test", "jsonschema", SchemaVer.Full(1,0,0))), json)

      val schemaList = SchemaList.buildSingleSchema(schema).get

      val data1 = json"""{"union": "union_value"}"""
      val result1 = FlatData.flatten(data1, schemaList, None)
      val expected1 = List("union_value")
      val comp1 = result1 must beEqualTo(expected1)

      val data2 = json"""{"union": {"foo": "foo_val", "bar": "bar_val"}}"""
      val result2 = FlatData.flatten(data2, schemaList, None)
      val expected2 = List("""{"foo":"foo_val","bar":"bar_val"}""")
      val comp2 = result2 must beEqualTo(expected2)

      val expected3 = List("""{"object_without_properties":"val"}""")
      val data3 = json"""{"union": {"object_without_properties": "val"}}"""
      val result3 = FlatData.flatten(data3, schemaList, None)
      val comp3 = result3 must beEqualTo(expected3)

      comp1 and comp2 and comp3
    }

    "process schema which contains list array" >> {
      val schema = json"""
      {
        "type": "object",
        "properties": {
          "someBool": { "type": "boolean" },
          "someArray": {
            "type": "array",
            "items": [{"type": "integer"}, {"type": "string"}]
          }
        }
      } """.schema

      val schemas = NonEmptyList.of(
        SelfDescribingSchema(SchemaMap(SchemaKey("com.acme", "test", "jsonschema", SchemaVer.Full(1,0,0))), schema)
      )
      val schemaList = SchemaList.buildMultiple(schemas).right.get.head

      val data = json"""{"someBool": true, "someArray": ["item1", "item2", "item3"]}"""
      val result = FlatData.flatten(data, schemaList, None)
      val expected = List("""["item1","item2","item3"]""", "1")

      result must beEqualTo(expected)
    }

    "process schema which contains tuple array" >> {
      val schema = json"""
      {
        "type": "object",
        "properties": {
          "someBool": { "type": "boolean" },
          "someArray": {
            "type": "array",
            "items": {"type": "integer"}
          }
        }
      } """.schema

      val schemas = NonEmptyList.of(
        SelfDescribingSchema(SchemaMap(SchemaKey("com.acme", "test", "jsonschema", SchemaVer.Full(1,0,0))), schema)
      )
      val schemaList = SchemaList.buildMultiple(schemas).right.get.head

      val data = json"""{"someBool": true, "someArray": ["item1","item2","item3"]}"""
      val result = FlatData.flatten(data, schemaList, None)
      val expected = List("""["item1","item2","item3"]""", "1")

      result must beEqualTo(expected)
    }

    "process schema which contains union type" >> {
      val schema = json"""
      {
        "type": "object",
        "properties": {
          "a": {
            "type": ["integer", "object"],
            "properties": {
              "b": { "type": "string" },
              "c": { "type": "integer" }
            }
          }
        }
      } """.schema

      val schemas = NonEmptyList.of(
        SelfDescribingSchema(SchemaMap(SchemaKey("com.acme", "test", "jsonschema", SchemaVer.Full(1,0,0))), schema)
      )
      val schemaList = SchemaList.buildMultiple(schemas).right.get.head

      val data1 = json"""{"a": 2}"""
      val result1 = FlatData.flatten(data1, schemaList, None)
      val expected1 = List("2")
      val comp1 = result1 must beEqualTo(expected1)

      val data2 = json"""{"a":{"b":1,"c":"val"}}"""
      val result2 = FlatData.flatten(data2, schemaList, None)
      val expected2 = List("""{"b":1,"c":"val"}""")
      val comp2 = result2 must beEqualTo(expected2)

      val data3 = json"""{"a":{"key1":"value1","key2":"value2"}}"""
      val result3 = FlatData.flatten(data3, schemaList, None)
      val expected3 = List("""{"key1":"value1","key2":"value2"}""")
      val comp3 = result3 must beEqualTo(expected3)

      comp1 and comp2 and comp3
    }
  }
}
