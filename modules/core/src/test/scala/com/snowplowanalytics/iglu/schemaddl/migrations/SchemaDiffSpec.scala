/*
 * Copyright (c) 2012-2023 Snowplow Analytics Ltd. All rights reserved.
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

import com.snowplowanalytics.iglu.core.{SchemaVer, SchemaMap, SelfDescribingSchema}

import com.snowplowanalytics.iglu.schemaddl.IgluSchema
import com.snowplowanalytics.iglu.schemaddl.SpecHelpers.{JsonOps, StringOps}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Pointer
import com.snowplowanalytics.iglu.schemaddl.migrations.SchemaDiff.Modified
import com.snowplowanalytics.iglu.schemaddl.migrations.SchemaList.ModelGroupSet

import org.specs2.mutable.Specification

class SchemaDiffSpec extends Specification {
  "build" should {
    "recognize schemas with increased length" in {
      val segment = SchemaDiffSpec.unsafeSchemaList(List(
        SelfDescribingSchema(
          SchemaMap("v", "n", "jsonschema", SchemaVer.Full(1,0,0)),
          json"""{"properties": {"one": {"type": "string", "maxLength": 32}}}""".schema
        ),
        SelfDescribingSchema(
          SchemaMap("v", "n", "jsonschema", SchemaVer.Full(1,0,1)),
          json"""{"properties": {"one": {"type": "string", "maxLength": 64}}}""".schema
        ),
      )).toSegment

      val expected =
        Set(
          Modified(
            Pointer.Root,
            json"""{"properties": {"one": {"type": "string", "maxLength": 32}}}""".schema,
            json"""{"properties": {"one": {"type": "string", "maxLength": 64}}}""".schema
          ),
          Modified(
            "/properties/one".jsonPointer,
            json"""{"type": ["string","null"], "maxLength": 32}""".schema,
            json"""{"type": ["string","null"], "maxLength": 64}""".schema
          ),
        )

      SchemaDiff.build(segment).modified must beEqualTo(expected)
    }
  }

  "getModifiedProperties" should {
    "recognize schemas with increased length" in {
      val source = Set(
        "/properties/bar".jsonPointer ->
          json"""{"type": ["string"], "maxLength": 32}""".schema
      )
      val target = Set(
        "/properties/bar".jsonPointer ->
          json"""{"type": ["string"], "maxLength": 64}""".schema
      )
      val expected = Set(SchemaDiff.Modified(
        "/properties/bar".jsonPointer,
        json"""{"type": ["string"], "maxLength": 32}""".schema,
        json"""{"type": ["string"], "maxLength": 64}""".schema
      ))

      SchemaDiff.getModifiedProperties(source, target) must beEqualTo(expected)
    }
  }
}

object SchemaDiffSpec {
  def unsafeSchemaList(list: List[IgluSchema]): SchemaList.Full = {
    val modelGroup = ModelGroupSet.groupSchemas(NonEmptyList.fromListUnsafe(list)).head
    SchemaList.fromUnambiguous(modelGroup) match {
      case Right(f: SchemaList.Full) => f
      case Left(value) => throw new RuntimeException(value.toString)
      case Right(_) => throw new RuntimeException("Not Full")
    }
  }
}
