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

import io.circe.literal._
import cats.data._
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaMap, SchemaVer, SelfDescribingSchema}
import com.snowplowanalytics.iglu.schemaddl.SpecHelpers._
import com.snowplowanalytics.iglu.schemaddl.migrations.Migration.BuildError
import com.snowplowanalytics.iglu.schemaddl.IgluSchema
import SchemaList._
import org.specs2.Specification

class MigrationSpec extends Specification { def is = s2"""
  Check common Schema migrations
    create correct addition migration from 1-0-0 to 1-0-1 $e1
    create correct addition migrations from 1-0-0 to 1-0-2 $e2
    create correct addition/modification migrations from 1-0-0 to 1-0-2 $e3
  migrateFrom function in Migration
    return error when schemaKey not found in given schemas $e9
    return error when schemaKey is latest state of given schemas $e10
    create migration as expected when schemaKey is initial version of given schemas $e11
    create migration as expected when schemaKey is second version of given schemas $e12
  """

  private def createSchemaListFull(schemas: NonEmptyList[IgluSchema]) = {
    val schemaList = SchemaList.buildMultiple(schemas).right.get.collect { case s: Full => s}
    NonEmptyList.fromListUnsafe(schemaList)
  }

  def e1 = {
    val initial = json"""
      {
        "type": "object",
        "properties": {
          "foo": {
            "type": "string"
          }
        },
        "additionalProperties": false
      }
    """.schema
    val initialSchema = SelfDescribingSchema(SchemaMap("com.acme", "example", "jsonschema", SchemaVer.Full(1,0,0)), initial)

    val second = json"""
      {
        "type": "object",
        "properties": {
          "foo": {
            "type": "string"
          },
          "bar": {
            "type": "integer",
            "maximum": 4000
          }
        },
        "additionalProperties": false
      }
    """.schema

    val secondSchema = SelfDescribingSchema(SchemaMap("com.acme", "example", "jsonschema", SchemaVer.Full(1,0,1)), second)

    val fromSchema = json"""{"type": ["integer", "null"], "maximum": 4000}""".schema
    val fromPointer = "/properties/bar".jsonPointer

    val migration = Migration(
        "com.acme",
        "example",
        SchemaVer.Full(1,0,0),
        SchemaVer.Full(1,0,1),
        SchemaDiff(
          List(fromPointer -> fromSchema),
          Set.empty,
          List.empty
        )
      )

    val segment = SchemaList.Segment(NonEmptyList.of(initialSchema, secondSchema))

    Migration.fromSegment(segment) must beEqualTo(migration)
  }

  def e2 = {
    val initial = json"""
        {
          "type": "object",
          "properties": {
            "foo": {
              "type": "string"
            }
          },
          "additionalProperties": false
        }
      """.schema
    val initialSchema = SelfDescribingSchema(SchemaMap("com.acme", "example", "jsonschema", SchemaVer.Full(1,0,0)), initial)

    val second = json"""
        {
          "type": "object",
          "properties": {
            "foo": {
              "type": "string"
            },
            "bar": {
              "type": "integer",
              "maximum": 4000
            }
          },
          "additionalProperties": false
        }
      """.schema
    val secondSchema = SelfDescribingSchema(SchemaMap("com.acme", "example", "jsonschema", SchemaVer.Full(1,0,1)), second)

    val third = json"""
      {
        "type": "object",
        "properties": {
          "foo": {
            "type": "string"
          },
          "bar": {
            "type": "integer",
            "maximum": 4000
          },
          "baz": {
            "type": "array"
          }
        },
        "additionalProperties": false
      }
    """.schema
    val thirdSchema = SelfDescribingSchema(SchemaMap("com.acme", "example", "jsonschema", SchemaVer.Full(1,0,2)), third)

    val migration1 = Migration(
      "com.acme",
      "example",
      SchemaVer.Full(1,0,0),
      SchemaVer.Full(1,0,1),
      SchemaDiff(
        List("/properties/bar".jsonPointer -> json"""{"type": ["integer", "null"], "maximum": 4000}""".schema),
        Set.empty,
        List.empty
      )
    )
    val comp1 = Migration.fromSegment(SchemaList.Segment(NonEmptyList.of(initialSchema, secondSchema))) must beEqualTo(migration1)

    val migration2 = Migration(
      "com.acme",
      "example",
      SchemaVer.Full(1,0,0),
      SchemaVer.Full(1,0,2),
      SchemaDiff(
        List(
          "/properties/bar".jsonPointer -> json"""{"type": ["integer", "null"], "maximum": 4000}""".schema,
          "/properties/baz".jsonPointer -> json"""{"type": ["array", "null"]}""".schema),
        Set.empty,
        List.empty
      )
    )
    val comp2 = Migration.fromSegment(SchemaList.Segment(NonEmptyList.of(initialSchema, secondSchema, thirdSchema))) must beEqualTo(migration2)

    val migration3 = Migration(
      "com.acme",
      "example",
      SchemaVer.Full(1,0,1),
      SchemaVer.Full(1,0,2),
      SchemaDiff(
        List("/properties/baz".jsonPointer -> json"""{"type": ["array", "null"]}""".schema),
        Set.empty,
        List.empty
      )
    )
    val comp3 = Migration.fromSegment(SchemaList.Segment(NonEmptyList.of(secondSchema, thirdSchema))) must beEqualTo(migration3)

    comp1 and comp2 and comp3
  }

  def e3 = {
    val initial = json"""
        {
          "type": "object",
          "properties": {
            "foo": {
              "type": "string"
            }
          },
          "additionalProperties": false
        }
      """.schema
    val initialSchema = SelfDescribingSchema(SchemaMap("com.acme", "example", "jsonschema", SchemaVer.Full(1,0,0)), initial)

    val second = json"""
        {
          "type": "object",
          "properties": {
            "foo": {
              "type": "string",
              "maxLength": 20
            },
            "bar": {
              "type": "integer",
              "maximum": 4000
            }
          },
          "additionalProperties": false
        }
      """.schema
    val secondSchema = SelfDescribingSchema(SchemaMap("com.acme", "example", "jsonschema", SchemaVer.Full(1,0,1)), second)

    val migration = Migration(
      "com.acme",
      "example",
      SchemaVer.Full(1,0,0),
      SchemaVer.Full(1,0,1),
      SchemaDiff(
        List("/properties/bar".jsonPointer -> json"""{"type": ["integer", "null"], "maximum": 4000}""".schema),
        Set(
          SchemaDiff.Modified(
            "/properties/foo".jsonPointer,
            json"""{"type":["string","null"]}""".schema,
            json"""{"type":["string","null"],"maxLength": 20}""".schema)
        ),
        List.empty
      )
    )

    Migration.fromSegment(SchemaList.Segment(NonEmptyList.of(initialSchema, secondSchema))) must beEqualTo(migration)
  }

  def e9 = {
    val initial = json"""
        {
          "type": "object",
          "properties": {
            "foo": {
              "type": "string"
            }
          },
          "additionalProperties": false
        }
      """.schema
    val initialSchema = SelfDescribingSchema(SchemaMap("com.acme", "example", "jsonschema", SchemaVer.Full(1,0,0)), initial)

    val second = json"""
        {
          "type": "object",
          "properties": {
            "foo": {
              "type": "string"
            },
            "bar": {
              "type": "integer",
              "maximum": 4000
            }
          },
          "additionalProperties": false
        }
      """.schema
    val secondSchema = SelfDescribingSchema(SchemaMap("com.acme", "example", "jsonschema", SchemaVer.Full(1,0,1)), second)

    val third = json"""
      {
        "type": "object",
        "properties": {
          "foo": {
            "type": "string"
          },
          "bar": {
            "type": "integer",
            "maximum": 4000
          },
          "baz": {
            "type": "array"
          }
        },
        "additionalProperties": false
      }
    """.schema
    val thirdSchema = SelfDescribingSchema(SchemaMap("com.acme", "example", "jsonschema", SchemaVer.Full(1,0,2)), third)

    val nonExistingSchemaKey = SchemaKey("com.acme", "non-existing", "jsonschema", SchemaVer.Full(1,0,0))
    val orderedSchemas = createSchemaListFull(NonEmptyList.of(initialSchema, secondSchema, thirdSchema)).head

    val res = Migration.migrateFrom(nonExistingSchemaKey, orderedSchemas)

    res must beLeft(BuildError.UnknownSchemaKey: BuildError)
  }

  def e10 = {
    val initial = json"""
        {
          "type": "object",
          "properties": {
            "foo": {
              "type": "string"
            }
          },
          "additionalProperties": false
        }
      """.schema
    val initialSchema = SelfDescribingSchema(SchemaMap("com.acme", "example", "jsonschema", SchemaVer.Full(1,0,0)), initial)

    val second = json"""
        {
          "type": "object",
          "properties": {
            "foo": {
              "type": "string"
            },
            "bar": {
              "type": "integer",
              "maximum": 4000
            }
          },
          "additionalProperties": false
        }
      """.schema
    val secondSchema = SelfDescribingSchema(SchemaMap("com.acme", "example", "jsonschema", SchemaVer.Full(1,0,1)), second)

    val third = json"""
      {
        "type": "object",
        "properties": {
          "foo": {
            "type": "string"
          },
          "bar": {
            "type": "integer",
            "maximum": 4000
          },
          "baz": {
            "type": "array"
          }
        },
        "additionalProperties": false
      }
    """.schema
    val thirdSchema = SelfDescribingSchema(SchemaMap("com.acme", "example", "jsonschema", SchemaVer.Full(1,0,2)), third)

    val orderedSchemas = createSchemaListFull(NonEmptyList.of(initialSchema, secondSchema, thirdSchema)).head

    val res = Migration.migrateFrom(thirdSchema.self.schemaKey, orderedSchemas)

    res must beLeft(BuildError.NoOp: BuildError)
  }

  def e11 = {
    val initial = json"""
        {
          "type": "object",
          "properties": {
            "foo": {
              "type": "string"
            }
          },
          "additionalProperties": false
        }
      """.schema
    val initialSchema = SelfDescribingSchema(SchemaMap("com.acme", "example", "jsonschema", SchemaVer.Full(1,0,0)), initial)

    val second = json"""
        {
          "type": "object",
          "properties": {
            "foo": {
              "type": "string"
            },
            "bar": {
              "type": "integer",
              "maximum": 4000
            }
          },
          "additionalProperties": false
        }
      """.schema
    val secondSchema = SelfDescribingSchema(SchemaMap("com.acme", "example", "jsonschema", SchemaVer.Full(1,0,1)), second)

    val third = json"""
      {
        "type": "object",
        "properties": {
          "foo": {
            "type": "string"
          },
          "bar": {
            "type": "integer",
            "maximum": 4000
          },
          "baz": {
            "type": "array"
          }
        },
        "additionalProperties": false
      }
    """.schema
    val thirdSchema = SelfDescribingSchema(SchemaMap("com.acme", "example", "jsonschema", SchemaVer.Full(1,0,2)), third)

    val migration = Migration(
      "com.acme",
      "example",
      SchemaVer.Full(1,0,0),
      SchemaVer.Full(1,0,2),
      SchemaDiff(
        List(
          "/properties/bar".jsonPointer -> json"""{"type": ["integer", "null"], "maximum": 4000}""".schema,
          "/properties/baz".jsonPointer -> json"""{"type": ["array", "null"]}""".schema),
        Set.empty,
        List.empty)
    )

    val orderedSchemas = createSchemaListFull(NonEmptyList.of(initialSchema, secondSchema, thirdSchema)).head

    val res = Migration.migrateFrom(initialSchema.self.schemaKey, orderedSchemas)

    res must beRight(migration)
  }

  def e12 = {
    val initial = json"""
        {
          "type": "object",
          "properties": {
            "foo": {
              "type": "string"
            }
          },
          "additionalProperties": false
        }
      """.schema
    val initialSchema = SelfDescribingSchema(SchemaMap("com.acme", "example", "jsonschema", SchemaVer.Full(1,0,0)), initial)

    val second = json"""
        {
          "type": "object",
          "properties": {
            "foo": {
              "type": "string"
            },
            "bar": {
              "type": "integer",
              "maximum": 4000
            }
          },
          "additionalProperties": false
        }
      """.schema
    val secondSchema = SelfDescribingSchema(SchemaMap("com.acme", "example", "jsonschema", SchemaVer.Full(1,0,1)), second)

    val third = json"""
      {
        "type": "object",
        "properties": {
          "foo": {
            "type": "string"
          },
          "bar": {
            "type": "integer",
            "maximum": 4000
          },
          "baz": {
            "type": "array"
          }
        },
        "additionalProperties": false
      }
    """.schema
    val thirdSchema = SelfDescribingSchema(SchemaMap("com.acme", "example", "jsonschema", SchemaVer.Full(1,0,2)), third)

    val migration = Migration(
      "com.acme",
      "example",
      SchemaVer.Full(1,0,1),
      SchemaVer.Full(1,0,2),
      SchemaDiff(
        List("/properties/baz".jsonPointer -> json"""{"type": ["array", "null"]}""".schema),
        Set.empty,
        List.empty)
    )

    val orderedSchemas = createSchemaListFull(NonEmptyList.of(initialSchema, secondSchema, thirdSchema)).head

    val res = Migration.migrateFrom(secondSchema.self.schemaKey, orderedSchemas)

    res must beRight(migration)
  }
}
