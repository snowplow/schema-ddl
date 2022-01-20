/*
 * Copyright (c) 2012-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.iglu.schemaddl.redshift.generators


import cats.data.NonEmptyList

import io.circe.literal._

// Specs2
import org.specs2.Specification

// This library
import com.snowplowanalytics.iglu.schemaddl.SpecHelpers._

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Pointer
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.CommonProperties.Type

import com.snowplowanalytics.iglu.schemaddl.redshift._

// TODO: union type specs (string, object)

class DdlGeneratorSpec extends Specification { def is = s2"""
  Check DDL generation specification
    Generate correct DDL for atomic table $e1
    Generate correct DDL for with runlength encoding for booleans $e2
    Generate correct DDL when enum schema is nullable $e3
    Generate correct DDL when only pointer is root pointer $e4
    Generate correct DDL with sum type $e5
  """

  def e1 = {
    val orderedSubSchemas = List(
      "/foo".jsonPointer -> json"""{"type": "string", "maxLength": 30}""".schema,
      "/bar".jsonPointer -> json"""{"enum": ["one","two","three",null]}""".schema
    )

    val resultDdl = CreateTable(
      "atomic.launch_missles",
      DdlGenerator.selfDescSchemaColumns ++
      DdlGenerator.parentageColumns ++
      List(
        Column("foo",RedshiftVarchar(30),Set(CompressionEncoding(ZstdEncoding)),Set(Nullability(NotNull))),
        Column("bar",RedshiftVarchar(5),Set(CompressionEncoding(Text255Encoding)),Set())
      ),
      Set(ForeignKeyTable(NonEmptyList.of("root_id"),RefTable("atomic.events",Some("event_id")))),
      Set(Diststyle(Key), DistKeyTable("root_id"),SortKeyTable(None,NonEmptyList.of("root_tstamp")))
    )

    val ddl = DdlGenerator.generateTableDdl(orderedSubSchemas, "launch_missles", None, 4096, false)

    ddl must beEqualTo(resultDdl)
  }

  def e2 = {
    val orderedSubSchemas = List(
      "/foo".jsonPointer -> json"""{"type": "boolean"}""".schema,
      "/baz".jsonPointer -> json"""{"type": "boolean"}""".schema,
      "/bar".jsonPointer -> json"""{"enum": ["one","two","three"]}""".schema
    )

    val resultDdl = CreateTable(
      "atomic.launch_missles",
      DdlGenerator.selfDescSchemaColumns ++
      DdlGenerator.parentageColumns ++
      List(
        Column("foo",RedshiftBoolean,Set(CompressionEncoding(RunLengthEncoding)),Set(Nullability(NotNull))),
        Column("baz",RedshiftBoolean,Set(CompressionEncoding(RunLengthEncoding)),Set(Nullability(NotNull))),
        Column("bar",RedshiftVarchar(5),Set(CompressionEncoding(Text255Encoding)),Set(Nullability(NotNull)))
      ),
      Set(ForeignKeyTable(NonEmptyList.of("root_id"),RefTable("atomic.events",Some("event_id")))),
      Set(Diststyle(Key), DistKeyTable("root_id"),SortKeyTable(None,NonEmptyList.of("root_tstamp")))
    )

    val ddl = DdlGenerator.generateTableDdl(orderedSubSchemas, "launch_missles", None, 4096, false)

    ddl must beEqualTo(resultDdl)
  }

  def e3 = {
    val enumSchemaWithNull = json"""{"enum": ["one","two","three"]}""".schema.copy(`type` = Some(Type.Null))
    val orderedSubSchemas = List(
      "/foo".jsonPointer -> json"""{"type": "boolean"}""".schema,
      "/baz".jsonPointer -> json"""{"type": "boolean"}""".schema,
      "/enumField".jsonPointer -> enumSchemaWithNull
    )

    val resultDdl = CreateTable(
      "atomic.launch_missles",
      DdlGenerator.selfDescSchemaColumns ++
        DdlGenerator.parentageColumns ++
        List(
          Column("foo",RedshiftBoolean,Set(CompressionEncoding(RunLengthEncoding)),Set(Nullability(NotNull))),
          Column("baz",RedshiftBoolean,Set(CompressionEncoding(RunLengthEncoding)),Set(Nullability(NotNull))),
          Column("enum_field",RedshiftVarchar(5),Set(CompressionEncoding(Text255Encoding)),Set())
        ),
      Set(ForeignKeyTable(NonEmptyList.of("root_id"),RefTable("atomic.events",Some("event_id")))),
      Set(Diststyle(Key), DistKeyTable("root_id"),SortKeyTable(None,NonEmptyList.of("root_tstamp")))
    )

    val ddl = DdlGenerator.generateTableDdl(orderedSubSchemas, "launch_missles", None, 4096, false)

    ddl must beEqualTo(resultDdl)
  }

  def e4 = {
    val orderedSubSchemas = List(
      Pointer.Root -> Schema.empty
    )

    val ddl = DdlGenerator.generateTableDdl(orderedSubSchemas, "launch_missles", None, 4096, false)

    val resultDdl = CreateTable(
      "atomic.launch_missles",
      DdlGenerator.selfDescSchemaColumns ++
        DdlGenerator.parentageColumns,
      Set(ForeignKeyTable(NonEmptyList.of("root_id"),RefTable("atomic.events",Some("event_id")))),
      Set(Diststyle(Key), DistKeyTable("root_id"),SortKeyTable(None,NonEmptyList.of("root_tstamp")))
    )

    ddl must beEqualTo(resultDdl)
  }

  def e5 = {

    val subSchemas = List(
      "/properties/union".jsonPointer ->
        json"""{
          "oneOf": [
            {
              "type": "object",
              "properties": {
                "one": { "type": "integer" }
              }
            },
            {
              "type": "object",
              "properties": {
                "two": { "type": "string" }
              }
            }
          ]
        }""".schema.copy(`type` = Some(Type.Null))
    )

    val ddl = DdlGenerator
      .generateTableDdl(subSchemas, "launch_missles", None, 4096, false)
      .columns

    val resultDdl = CreateTable(
      "atomic.launch_missles",
      DdlGenerator.selfDescSchemaColumns ++
        DdlGenerator.parentageColumns :+
        Column("union",RedshiftVarchar(4096),Set(CompressionEncoding(ZstdEncoding)),Set()),
      Set(ForeignKeyTable(NonEmptyList.of("root_id"),RefTable("atomic.events",Some("event_id")))),
      Set(Diststyle(Key), DistKeyTable("root_id"),SortKeyTable(None,NonEmptyList.of("root_tstamp")))
    ).columns

    ddl must beEqualTo(resultDdl)
  }
}
