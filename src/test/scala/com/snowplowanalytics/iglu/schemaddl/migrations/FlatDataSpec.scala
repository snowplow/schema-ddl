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

import org.specs2.Specification

import com.snowplowanalytics.iglu.schemaddl.SpecHelpers._

class FlatDataSpec extends Specification { def is = s2"""
  getPath goes into nested data $e1
  shit is getting serious $e2
  """

  def e1 = {
    val path = "/properties/foo/properties/bar".jsonPointer.forData
    val data = json"""{"foo": {"bar": "success"}}"""
    FlatData.getPath(path, data) must beEqualTo("success")
  }

  def e2 = {
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

    val expected = List("one", "two", "three", "four")

    val source = Migration.buildMigrationMatrix(schemas).toList.maxBy(_.schemas.length)
    val data = json"""{"a": "one", "b": {"ba": "two", "bb": "four"}, "c": "three"}"""
    val result = FlatData.flatten(data, source)

    result must beEqualTo(expected)
  }
}
