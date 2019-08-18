/*
 * Copyright (c) 2012-2016 Snowplow Analytics Ltd. All rights reserved.
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
import com.snowplowanalytics.iglu.schemaddl.migrations.Migration.MigrateFromError
import com.snowplowanalytics.iglu.schemaddl.IgluSchema
import SchemaList._
import org.specs2.Specification

class MigrationSpec extends Specification { def is = s2"""
  Check common Schema migrations
    create correct addition migration from 1-0-0 to 1-0-1 $e1
    create correct addition migrations from 1-0-0 to 1-0-2 $e2
    create correct addition/modification migrations from 1-0-0 to 1-0-2 $e3
    create correct ordered subschemas from 1-0-0 to 1-0-1 $e4
    create correct ordered subschemas from 1-0-0 to 1-0-2 $e5
    create correct ordered subschemas for complex schema $e6
    create correct ordered subschemas for complex schema $e7
    create correct migrations when there are schemas with different vendor and name $e8
  migrateFrom function in Migration
    return error when schemaKey not found in given schemas $e9
    return error when schemaKey is latest state of given schemas $e10
    create migration as expected when schemaKey is initial version of given schemas $e11
    create migration as expected when schemaKey is second version of given schemas $e12
  """

  private def createSchemaListFull(schemas: NonEmptyList[IgluSchema]) = {
    val schemaList = SchemaList.buildMultiple(schemas).right.get.collect { case s: SchemaListFull => s}
    NonEmptyList.fromListUnsafe(schemaList)
  }

  private def createSchemaList(schemas: NonEmptyList[IgluSchema]) =
    SchemaList.buildMultiple(schemas).right.get

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

    val migrations = NonEmptyList.of(
      Migration(
        "com.acme",
        "example",
        SchemaVer.Full(1,0,0),
        SchemaVer.Full(1,0,1),
        SchemaDiff(
          List(fromPointer -> fromSchema),
          Set.empty,
          List.empty)))

    val migrationMap = Map(
      SchemaMap("com.acme", "example", "jsonschema", SchemaVer.Full(1,0,0)) -> migrations
    )

    val schemaListFulls = createSchemaListFull(NonEmptyList.of(initialSchema, secondSchema))

    Migration.buildMigrationMap(schemaListFulls) must beEqualTo(migrationMap)
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

    val migrations1 = NonEmptyList.of(
      Migration(
        "com.acme",
        "example",
        SchemaVer.Full(1,0,0),
        SchemaVer.Full(1,0,1),
        SchemaDiff(
          List("/properties/bar".jsonPointer -> json"""{"type": ["integer", "null"], "maximum": 4000}""".schema),
          Set.empty,
          List.empty)),
      Migration(
        "com.acme",
        "example",
        SchemaVer.Full(1,0,0),
        SchemaVer.Full(1,0,2),
        SchemaDiff(
          List(
            "/properties/bar".jsonPointer -> json"""{"type": ["integer", "null"], "maximum": 4000}""".schema,
            "/properties/baz".jsonPointer -> json"""{"type": ["array", "null"]}""".schema),
          Set.empty,
          List.empty)))

    val migrations2 = NonEmptyList.of(
      Migration(
        "com.acme",
        "example",
        SchemaVer.Full(1,0,1),
        SchemaVer.Full(1,0,2),
        SchemaDiff(
          List("/properties/baz".jsonPointer -> json"""{"type": ["array", "null"]}""".schema),
          Set.empty,
          List.empty))
    )

    val migrationMap = Map(
      SchemaMap("com.acme", "example", "jsonschema", SchemaVer.Full(1,0,0)) -> migrations1,
      SchemaMap("com.acme", "example", "jsonschema", SchemaVer.Full(1,0,1)) -> migrations2
    )

    val schemaListFulls = createSchemaListFull(NonEmptyList.of(initialSchema, secondSchema, thirdSchema))

    Migration.buildMigrationMap(schemaListFulls) must beEqualTo(migrationMap)
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

    val migrations1 = NonEmptyList.of(
      Migration(
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
          List.empty)))


    val migrationMap = Map(
      SchemaMap("com.acme", "example", "jsonschema", SchemaVer.Full(1,0,0)) -> migrations1
    )

    val schemaListFulls = createSchemaListFull(NonEmptyList.of(initialSchema, secondSchema))

    Migration.buildMigrationMap(schemaListFulls) must beEqualTo(migrationMap)
  }

  def e4 = {
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
            "a_field": {
              "type": "integer"
            },
            "b_field": {
              "type": "integer"
            }
          },
          "required": ["b_field"],
          "additionalProperties": false
        }
      """.schema
    val secondSchema = SelfDescribingSchema(SchemaMap("com.acme", "example", "jsonschema", SchemaVer.Full(1,0,1)), second)

    val schemaLists = createSchemaList(NonEmptyList.of(initialSchema, secondSchema))

    val orderedSubSchemasMap = Migration.buildOrderedSubSchemasMap(schemaLists)

    val expected = Map(SchemaMap("com.acme", "example", "jsonschema", SchemaVer.Full(1,0,1)) -> List("foo", "b_field", "a_field"))

    val res = extractOrder(orderedSubSchemasMap)

    res must beEqualTo(expected)
  }

  def e5 = {
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

    val third = json"""
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
            },
            "aField": {
              "type": "integer"
            },
            "cField": {
              "type": "integer"
            },
            "dField": {
              "type": "string"
            }
          },
          "required": ["bar", "cField"],
          "additionalProperties": false
        }
      """.schema
    val thirdSchema = SelfDescribingSchema(SchemaMap("com.acme", "example", "jsonschema", SchemaVer.Full(1,0,2)), third)

    val schemaLists = createSchemaList(NonEmptyList.of(initialSchema, secondSchema, thirdSchema))

    val orderedSubSchemasMap = Migration.buildOrderedSubSchemasMap(schemaLists)

    val expected = Map(SchemaMap("com.acme", "example", "jsonschema", SchemaVer.Full(1,0,2)) -> List("foo", "bar", "c_field", "a_field", "d_field"))

    val res = extractOrder(orderedSubSchemasMap)

    res must beEqualTo(expected)
  }

  def e6 = {
    val initial = json"""
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
            },
            "a_field": {
              "type": "object",
              "properties": {
                "b_field": {
                  "type": "string"
                },
                "c_field": {
                  "type": "object",
                  "properties": {
                    "d_field": {
                      "type": "string"
                    },
                    "e_field": {
                      "type": "string"
                    }
                  }
                },
                "d_field": {
                  "type": "object"
                }
              },
              "required": ["d_field"]
            },
            "b_field": {
              "type": "integer"
            },
            "c_field": {
              "type": "integer"
            },
            "d_field": {
              "type": "object",
              "properties": {
                "e_field": {
                  "type": "string"
                },
                "f_field": {
                  "type": "string"
                }
              }
            }
          },
          "required": ["a_field"],
          "additionalProperties": false
        }
      """.schema
    val secondSchema = SelfDescribingSchema(SchemaMap("com.acme", "example", "jsonschema", SchemaVer.Full(1,0,1)), second)

    val schemaLists = createSchemaList(NonEmptyList.of(initialSchema, secondSchema))

    val orderedSubSchemasMap = Migration.buildOrderedSubSchemasMap(schemaLists)

    val expected = Map(
      SchemaMap("com.acme", "example", "jsonschema", SchemaVer.Full(1,0,1)) ->
        List(
          "bar",
          "foo",
          "a_field.d_field",
          "a_field.b_field",
          "a_field.c_field.d_field",
          "a_field.c_field.e_field",
          "b_field",
          "c_field",
          "d_field.e_field",
          "d_field.f_field"
        )
    )

    val res = extractOrder(orderedSubSchemasMap)

    res must beEqualTo(expected)
  }

  def e7 = {
    val initial = json"""
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
            },
            "a_field": {
              "type": "object",
              "properties": {
                "b_field": {
                  "type": "string"
                },
                "c_field": {
                  "type": "object",
                  "properties": {
                    "d_field": {
                      "type": "string"
                    },
                    "e_field": {
                      "type": "string"
                    }
                  }
                },
                "d_field": {
                  "type": "object"
                }
              },
              "required": ["d_field"]
            },
            "b_field": {
              "type": "integer"
            },
            "c_field": {
              "type": "integer"
            },
            "d_field": {
              "type": "object",
              "properties": {
                "e_field": {
                  "type": "string"
                },
                "f_field": {
                  "type": "string"
                }
              }
            }
          },
          "required": ["a_field"],
          "additionalProperties": false
        }
      """.schema
    val secondSchema = SelfDescribingSchema(SchemaMap("com.acme", "example", "jsonschema", SchemaVer.Full(1,0,1)), second)

    val third = json"""
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
            },
            "a_field": {
              "type": "object",
              "properties": {
                "b_field": {
                  "type": "string"
                },
                "c_field": {
                  "type": "object",
                  "properties": {
                    "d_field": {
                      "type": "string"
                    },
                    "e_field": {
                      "type": "string"
                    }
                  }
                },
                "d_field": {
                  "type": "object"
                }
              },
              "required": ["d_field"]
            },
            "b_field": {
              "type": "integer"
            },
            "c_field": {
              "type": "integer"
            },
            "d_field": {
              "type": "object",
              "properties": {
                "e_field": {
                  "type": "string"
                },
                "f_field": {
                  "type": "string"
                }
              }
            },
            "e_field": {
              "type": "object",
              "properties": {
                "f_field": {
                  "type": "string"
                },
                "g_field": {
                  "type": "string"
                }
              },
              "required": ["g_field"]
            },
            "f_field": {
              "type": "string"
            },
            "g_field": {
              "type": "string"
            }
          },
          "required": ["a_field", "f_field", "e_field"],
          "additionalProperties": false
        }
      """.schema
    val thirdSchema = SelfDescribingSchema(SchemaMap("com.acme", "example", "jsonschema", SchemaVer.Full(1,0,2)), third)

    val schemaLists = createSchemaList(NonEmptyList.of(initialSchema, secondSchema, thirdSchema))

    val orderedSubSchemasMap = Migration.buildOrderedSubSchemasMap(schemaLists)

    val expected = Map(
      SchemaMap("com.acme", "example", "jsonschema", SchemaVer.Full(1,0,2)) ->
        List(
          "bar",
          "foo",
          "a_field.d_field",
          "a_field.b_field",
          "a_field.c_field.d_field",
          "a_field.c_field.e_field",
          "b_field",
          "c_field",
          "d_field.e_field",
          "d_field.f_field",
          "e_field.g_field",
          "f_field",
          "e_field.f_field",
          "g_field"
        )
    )

    val res = extractOrder(orderedSubSchemasMap)

    res must beEqualTo(expected)
  }

  def e8 = {
    val schemaJson = json"""
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

    val schema11 = SchemaMap("com.acme", "example1", "jsonschema", SchemaVer.Full(1,0,0))
    val schema12 = SchemaMap("com.acme", "example1", "jsonschema", SchemaVer.Full(1,0,1))

    val schema21 = SchemaMap("com.acme", "example2", "jsonschema", SchemaVer.Full(1,0,0))
    val schema22 = SchemaMap("com.acme", "example2", "jsonschema", SchemaVer.Full(1,0,1))
    val schema23 = SchemaMap("com.acme", "example2", "jsonschema", SchemaVer.Full(1,0,2))

    val schema31 = SchemaMap("com.acme", "example3", "jsonschema", SchemaVer.Full(1,0,0))
    val schema32 = SchemaMap("com.acme", "example3", "jsonschema", SchemaVer.Full(1,0,1))
    val schema33 = SchemaMap("com.acme", "example3", "jsonschema", SchemaVer.Full(1,0,2))
    val schema34 = SchemaMap("com.acme", "example3", "jsonschema", SchemaVer.Full(1,0,3))

    val schemas = NonEmptyList.of(
        SelfDescribingSchema(schema11, schemaJson),
        SelfDescribingSchema(schema12, schemaJson),
        SelfDescribingSchema(schema21, schemaJson),
        SelfDescribingSchema(schema22, schemaJson),
        SelfDescribingSchema(schema23, schemaJson),
        SelfDescribingSchema(schema31, schemaJson),
        SelfDescribingSchema(schema32, schemaJson),
        SelfDescribingSchema(schema33, schemaJson),
        SelfDescribingSchema(schema34, schemaJson)
      )

    val schemaListFulls = createSchemaListFull(schemas)

    val migrationMap = Migration.buildMigrationMap(schemaListFulls)

    val expected = Set(schema11, schema21, schema22, schema31, schema32, schema33)

    migrationMap.keySet must beEqualTo(expected)
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

    res must beLeft(MigrateFromError.SchemaKeyNotFoundInSchemas)
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

    res must beLeft(MigrateFromError.SchemaInLatestState)
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
