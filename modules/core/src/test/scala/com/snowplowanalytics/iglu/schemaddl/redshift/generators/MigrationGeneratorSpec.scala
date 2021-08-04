/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.iglu.schemaddl.redshift
package generators

import com.snowplowanalytics.iglu.schemaddl.migrations.{Migration, SchemaDiff}
import io.circe.literal._

// specs2
import org.specs2.Specification

// Iglu
import com.snowplowanalytics.iglu.core.SchemaVer

// This library
import com.snowplowanalytics.iglu.schemaddl.SpecHelpers._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.{ Pointer, Schema }

class MigrationGeneratorSpec extends Specification { def is = s2"""
  Redshift migration
    generates addition migration with one new column                 $e1
    generates addition migration without visible changes             $e2
    generates addition migration with three new columns              $e3
    generates migration when increasing maxLength                    $e4
    generates migration when increasing maxLength and adding a field $e5
    generates migration when increasing maxLength of nullable field  $e6
    generates migration when adding new longer enum value            $e7
  maxLengthIncreased
    correctly detects when maxLength has been increased              $e8
  enumLonger
    correctly detects when enum gets new longer value                $e9
  """

  val emptyModified = Set.empty[SchemaDiff.Modified]
  val emptySubschemas = List.empty[(Pointer.SchemaPointer, Schema)]

  def e1 = {
    val diff = SchemaDiff(List("status".jsonPointer -> json"""{"type": ["string", "null"]}""".schema), emptyModified, emptySubschemas)
    val schemaMigration = Migration("com.acme", "launch_missles", SchemaVer.Full(1,0,0), SchemaVer.Full(1,0,1), diff)
    val ddlMigration = MigrationGenerator.generateMigration(schemaMigration, 4096, Some("atomic")).render

    val result =
      """|-- WARNING: only apply this file to your database if the following SQL returns the expected:
         |--
         |-- SELECT pg_catalog.obj_description(c.oid) FROM pg_catalog.pg_class c WHERE c.relname = 'com_acme_launch_missles_1';
         |--  obj_description
         |-- -----------------
         |--  iglu:com.acme/launch_missles/jsonschema/1-0-0
         |--  (1 row)
         |
         |BEGIN TRANSACTION;
         |
         |  ALTER TABLE atomic.com_acme_launch_missles_1
         |    ADD COLUMN "status" VARCHAR(4096) ENCODE ZSTD;
         |
         |  COMMENT ON TABLE atomic.com_acme_launch_missles_1 IS 'iglu:com.acme/launch_missles/jsonschema/1-0-1';
         |
         |END TRANSACTION;""".stripMargin

    ddlMigration must beEqualTo(result)
  }

  def e2 = {
    val diff = SchemaDiff.empty
    val schemaMigration = Migration("com.acme", "launch_missles", SchemaVer.Full(2,0,0), SchemaVer.Full(2,0,1), diff)
    val ddlMigration = MigrationGenerator.generateMigration(schemaMigration, 4096, Some("atomic")).render

    val result =
      """|-- WARNING: only apply this file to your database if the following SQL returns the expected:
         |--
         |-- SELECT pg_catalog.obj_description(c.oid) FROM pg_catalog.pg_class c WHERE c.relname = 'com_acme_launch_missles_2';
         |--  obj_description
         |-- -----------------
         |--  iglu:com.acme/launch_missles/jsonschema/2-0-0
         |--  (1 row)
         |
         |-- NO ADDED COLUMNS CAN BE EXPRESSED IN SQL MIGRATION
         |
         |  COMMENT ON TABLE atomic.com_acme_launch_missles_2 IS 'iglu:com.acme/launch_missles/jsonschema/2-0-1';
         |""".stripMargin

    ddlMigration must beEqualTo(result)
  }

  def e3 = {
    val newProps = List(
      "/status".jsonPointer -> json"""{"type": ["string", "null"]}""".schema,
      "/launch_time".jsonPointer -> json"""{"type": ["string", "null"], "format": "date-time"}""".schema,
      "/latitude".jsonPointer -> json"""{"type": "number", "minimum": -90, "maximum": 90}""".schema,
      "/longitude".jsonPointer ->json"""{"type": "number", "minimum": -180, "maximum": 180}""".schema)

    val diff = SchemaDiff(newProps, emptyModified, emptySubschemas)
    val schemaMigration = Migration("com.acme", "launch_missles", SchemaVer.Full(1,0,2), SchemaVer.Full(1,0,3), diff)
    val ddlMigration = MigrationGenerator.generateMigration(schemaMigration, 4096, Some("atomic")).render

    // TODO: NOT NULL columns should be first
    val result =
      """|-- WARNING: only apply this file to your database if the following SQL returns the expected:
         |--
         |-- SELECT pg_catalog.obj_description(c.oid) FROM pg_catalog.pg_class c WHERE c.relname = 'com_acme_launch_missles_1';
         |--  obj_description
         |-- -----------------
         |--  iglu:com.acme/launch_missles/jsonschema/1-0-2
         |--  (1 row)
         |
         |BEGIN TRANSACTION;
         |
         |  ALTER TABLE atomic.com_acme_launch_missles_1
         |    ADD COLUMN "status" VARCHAR(4096) ENCODE ZSTD;
         |  ALTER TABLE atomic.com_acme_launch_missles_1
         |    ADD COLUMN "launch_time" TIMESTAMP ENCODE ZSTD;
         |  ALTER TABLE atomic.com_acme_launch_missles_1
         |    ADD COLUMN "latitude" DOUBLE PRECISION NOT NULL ENCODE RAW;
         |  ALTER TABLE atomic.com_acme_launch_missles_1
         |    ADD COLUMN "longitude" DOUBLE PRECISION NOT NULL ENCODE RAW;
         |
         |  COMMENT ON TABLE atomic.com_acme_launch_missles_1 IS 'iglu:com.acme/launch_missles/jsonschema/1-0-3';
         |
         |END TRANSACTION;""".stripMargin

    ddlMigration must beEqualTo(result)
  }

  def e4 = {
    val added = List.empty[(Pointer.SchemaPointer, Schema)]
    val modified = Set(
      SchemaDiff.Modified(
        "/foo".jsonPointer,
        json"""{"type": "string", "maxLength": "1024"}""".schema,
        json"""{"type": "string", "maxLength": "2048"}""".schema
      )
    )
    val removed = List.empty[(Pointer.SchemaPointer, Schema)]

    val diff = SchemaDiff(added, modified, removed)
    val schemaMigration = Migration("com.acme", "example", SchemaVer.Full(1,0,0), SchemaVer.Full(1,0,1), diff)
    val ddlMigration = MigrationGenerator.generateMigration(schemaMigration, 4096, Some("atomic")).render

    val result =
      """|-- WARNING: only apply this file to your database if the following SQL returns the expected:
         |--
         |-- SELECT pg_catalog.obj_description(c.oid) FROM pg_catalog.pg_class c WHERE c.relname = 'com_acme_example_1';
         |--  obj_description
         |-- -----------------
         |--  iglu:com.acme/example/jsonschema/1-0-0
         |--  (1 row)
         |
         |  ALTER TABLE atomic.com_acme_example_1
         |    ALTER "foo" TYPE VARCHAR(2048);
         |
         |  COMMENT ON TABLE atomic.com_acme_example_1 IS 'iglu:com.acme/example/jsonschema/1-0-1';
         |""".stripMargin

    ddlMigration must beEqualTo(result)
  }

  def e5 = {
    val added = List(
      "/foo2".jsonPointer -> json"""{"type": ["string", "null"], "maxLength": "1024"}""".schema
    )
    val modified = Set(
      SchemaDiff.Modified(
        "/foo".jsonPointer,
        json"""{"type": "string", "maxLength": "1024"}""".schema,
        json"""{"type": "string", "maxLength": "2048"}""".schema
      )
    )
    val removed = List.empty[(Pointer.SchemaPointer, Schema)]

    val diff = SchemaDiff(added, modified, removed)
    val schemaMigration = Migration("com.acme", "example", SchemaVer.Full(1,0,0), SchemaVer.Full(1,0,1), diff)
    val ddlMigration = MigrationGenerator.generateMigration(schemaMigration, 4096, Some("atomic")).render

    val result =
      """|-- WARNING: only apply this file to your database if the following SQL returns the expected:
         |--
         |-- SELECT pg_catalog.obj_description(c.oid) FROM pg_catalog.pg_class c WHERE c.relname = 'com_acme_example_1';
         |--  obj_description
         |-- -----------------
         |--  iglu:com.acme/example/jsonschema/1-0-0
         |--  (1 row)
         |
         |  ALTER TABLE atomic.com_acme_example_1
         |    ALTER "foo" TYPE VARCHAR(2048);
         |
         |BEGIN TRANSACTION;
         |
         |  ALTER TABLE atomic.com_acme_example_1
         |    ADD COLUMN "foo2" VARCHAR(1024) ENCODE ZSTD;
         |
         |  COMMENT ON TABLE atomic.com_acme_example_1 IS 'iglu:com.acme/example/jsonschema/1-0-1';
         |
         |END TRANSACTION;""".stripMargin

    ddlMigration must beEqualTo(result)
  }

  def e6 = {
    val added = List(
      "/foo2".jsonPointer -> json"""{"type": ["string", "null"], "maxLength": "1024"}""".schema
    )
    val modified = Set(
      SchemaDiff.Modified(
        "/foo".jsonPointer,
        json"""{"type": ["string","null"], "maxLength": "1024"}""".schema,
        json"""{"type": ["string","null"], "maxLength": "2048"}""".schema
      )
    )
    val removed = List.empty[(Pointer.SchemaPointer, Schema)]

    val diff = SchemaDiff(added, modified, removed)
    val schemaMigration = Migration("com.acme", "example", SchemaVer.Full(1,0,0), SchemaVer.Full(1,0,1), diff)
    val ddlMigration = MigrationGenerator.generateMigration(schemaMigration, 4096, Some("atomic")).render

    val result =
      """|-- WARNING: only apply this file to your database if the following SQL returns the expected:
         |--
         |-- SELECT pg_catalog.obj_description(c.oid) FROM pg_catalog.pg_class c WHERE c.relname = 'com_acme_example_1';
         |--  obj_description
         |-- -----------------
         |--  iglu:com.acme/example/jsonschema/1-0-0
         |--  (1 row)
         |
         |  ALTER TABLE atomic.com_acme_example_1
         |    ALTER "foo" TYPE VARCHAR(2048);
         |
         |BEGIN TRANSACTION;
         |
         |  ALTER TABLE atomic.com_acme_example_1
         |    ADD COLUMN "foo2" VARCHAR(1024) ENCODE ZSTD;
         |
         |  COMMENT ON TABLE atomic.com_acme_example_1 IS 'iglu:com.acme/example/jsonschema/1-0-1';
         |
         |END TRANSACTION;""".stripMargin

    ddlMigration must beEqualTo(result)
  }

  def e7 = {
    val added = List.empty[(Pointer.SchemaPointer, Schema)]
    val modified = Set(
      SchemaDiff.Modified(
        "/foo".jsonPointer,
        json"""{"type": "string", "enum": ["FOO", "BAR"]}""".schema,
        json"""{"type": "string", "enum": ["FOO", "BAR", "FOOBAR"]}""".schema
      )
    )
    val removed = List.empty[(Pointer.SchemaPointer, Schema)]

    val diff = SchemaDiff(added, modified, removed)
    val schemaMigration = Migration("com.acme", "example", SchemaVer.Full(1,0,0), SchemaVer.Full(1,0,1), diff)
    val ddlMigration = MigrationGenerator.generateMigration(schemaMigration, 4096, Some("atomic")).render

    val result =
      """|-- WARNING: only apply this file to your database if the following SQL returns the expected:
         |--
         |-- SELECT pg_catalog.obj_description(c.oid) FROM pg_catalog.pg_class c WHERE c.relname = 'com_acme_example_1';
         |--  obj_description
         |-- -----------------
         |--  iglu:com.acme/example/jsonschema/1-0-0
         |--  (1 row)
         |
         |  ALTER TABLE atomic.com_acme_example_1
         |    ALTER "foo" TYPE VARCHAR(6);
         |
         |  COMMENT ON TABLE atomic.com_acme_example_1 IS 'iglu:com.acme/example/jsonschema/1-0-1';
         |""".stripMargin

    ddlMigration must beEqualTo(result)
  }

  def e8 = {
    val modifiedY = SchemaDiff.Modified(
      "/foo".jsonPointer,
      json"""{"type": "string", "maxLength": "1024"}""".schema,
      json"""{"type": "string", "maxLength": "2048"}""".schema
    )
    val yes = MigrationGenerator.maxLengthIncreased(modifiedY) must beTrue

    val modifiedN = SchemaDiff.Modified(
      "/foo".jsonPointer,
      json"""{"type": "string", "maxLength": "2048"}""".schema,
      json"""{"type": "string", "maxLength": "1024"}""".schema
    )
    val no = MigrationGenerator.maxLengthIncreased(modifiedN) must beFalse

    yes and no
  }

  def e9 = {
    val modifiedY1 = SchemaDiff.Modified(
      "/foo".jsonPointer,
      json"""{"type": "string", "enum": ["FOO", "BAR"]}""".schema,
      json"""{"type": "string", "enum": ["FOO", "BAR", "FOOBAR"]}""".schema
    )
    val yes1 = MigrationGenerator.enumLonger(modifiedY1) must beTrue

    val modifiedY2 = SchemaDiff.Modified(
      "/foo".jsonPointer,
      json"""{"enum": ["FOO", "BAR"]}""".schema,
      json"""{"enum": ["FOO", "BAR", "FOOBAR"]}""".schema
    )
    val yes2 = MigrationGenerator.enumLonger(modifiedY2) must beTrue

    val modifiedN = SchemaDiff.Modified(
      "/foo".jsonPointer,
      json"""{"type": "string", "enum": ["FOO", "BAR"]}""".schema,
      json"""{"type": "string", "enum": ["FO", "BA"]}""".schema
    )
    val no = MigrationGenerator.enumLonger(modifiedN) must beFalse

    yes1 and yes2 and no
  }

}
