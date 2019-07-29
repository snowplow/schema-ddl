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
import cats.data.NonEmptyList
import com.snowplowanalytics.iglu.core.{SchemaMap, SchemaVer, SelfDescribingSchema}
import com.snowplowanalytics.iglu.schemaddl.SpecHelpers._
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
    create correct ordered subschemas for complex schema $e8
    create correct migrations when there are schemas with different vendor and name $e9
  """

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

    Migration.buildMigrationMap(List(initialSchema, secondSchema)) must beEqualTo(migrationMap)
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

    Migration.buildMigrationMap(List(initialSchema, secondSchema, thirdSchema)) must beEqualTo(migrationMap)
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

    Migration.buildMigrationMap(List(initialSchema, secondSchema)) must beEqualTo(migrationMap)
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


    val schemas = List(initialSchema, secondSchema)
    val migrationMap = Migration.buildMigrationMap(schemas)
    val orderedSubSchemasMap = Migration.buildOrderedSubSchemasMap(schemas, migrationMap)

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


    val schemas = List(initialSchema, secondSchema, thirdSchema)
    val migrationMap = Migration.buildMigrationMap(schemas)
    val orderedSubSchemasMap = Migration.buildOrderedSubSchemasMap(schemas, migrationMap)

    val expected = Map(SchemaMap("com.acme", "example", "jsonschema", SchemaVer.Full(1,0,2)) -> List("foo", "bar", "c_field", "a_field", "d_field"))

    val res = extractOrder(orderedSubSchemasMap)

    res must beEqualTo(expected)
  }

  def e6 = {
    val initial = json"""
      {
      	"type":"object",
      	"properties":{
      		"functionName":{
      			"type":"string"
      		},
      		"logStreamName":{
      			"type":"string"
      		},
      		"awsRequestId":{
      			"type":"string"
      		},
      		"remainingTimeMillis":{
      			"type":"integer",
      			"minimum":0
      		},
      		"logGroupName":{
      			"type":"string"
      		},
      		"memoryLimitInMB":{
      			"type":"integer",
      			"minimum":0
      		},
      		"clientContext":{
      			"type":"object",
      			"properties":{
      				"client":{
      					"type":"object",
      					"properties":{
      						"appTitle":{
      							"type":"string"
      						},
      						"appVersionName":{
      							"type":"string"
      						},
      						"appVersionCode":{
      							"type":"string"
      						},
      						"appPackageName":{
      							"type":"string"
      						}
      					},
      					"additionalProperties":false
      				},
      				"custom":{
      					"type":"object",
      					"patternProperties":{
      						".*":{
      							"type":"string"
      						}
      					}
      				},
      				"environment":{
      					"type":"object",
      					"patternProperties":{
      						".*":{
      							"type":"string"
      						}
      					}
      				}
      			},
      			"additionalProperties":false
      		},
      		"identity":{
      			"type":"object",
      			"properties":{
      				"identityId":{
      					"type":"string"
      				},
      				"identityPoolId":{
      					"type":"string"
      				}
      			},
      			"additionalProperties":false
      		}
      	},
      	"additionalProperties":false
      }
    """.schema
    val initialSchema = SelfDescribingSchema(SchemaMap("com.amazon.aws.lambda", "java_context", "jsonschema", SchemaVer.Full(1,0,0)), initial)

    val schemas = List(initialSchema)
    val migrationMap = Migration.buildMigrationMap(schemas)
    val orderedSubSchemasMap = Migration.buildOrderedSubSchemasMap(schemas, migrationMap)

    val expected = Map(
      SchemaMap("com.amazon.aws.lambda", "java_context", "jsonschema", SchemaVer.Full(1,0,0)) ->
        List(
          "aws_request_id",
          "client_context.client.app_package_name",
          "client_context.client.app_title",
          "client_context.client.app_version_code",
          "client_context.client.app_version_name",
          "client_context.custom",
          "client_context.environment",
          "function_name",
          "identity.identity_id",
          "identity.identity_pool_id",
          "log_group_name",
          "log_stream_name",
          "memory_limit_in_mb",
          "remaining_time_millis"
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

    val schemas = List(initialSchema, secondSchema)
    val migrationMap = Migration.buildMigrationMap(schemas)
    val orderedSubSchemasMap = Migration.buildOrderedSubSchemasMap(schemas, migrationMap)

    val expected = Map(
      SchemaMap("com.acme", "example", "jsonschema", SchemaVer.Full(1,0,1)) ->
        List(
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

  def e8 = {
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
            "a_field": {
              "type": "object",
              "properties": {
                "b_field": {
                  "type": "string"
                },
                "c_field": {
                  "type": "object",
                  "properties": {
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
            "d_field": {
              "type": "object",
              "properties": {
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
    val thirdSchema = SelfDescribingSchema(SchemaMap("com.acme", "example", "jsonschema", SchemaVer.Full(1,1,0)), third)

    val schemas = List(initialSchema, secondSchema, thirdSchema)
    val migrationMap = Migration.buildMigrationMap(schemas)
    val orderedSubSchemasMap = Migration.buildOrderedSubSchemasMap(schemas, migrationMap)

    val expected = Map(
      SchemaMap("com.acme", "example", "jsonschema", SchemaVer.Full(1,1,0)) ->
        List(
          "foo",
          "a_field.d_field",
          "a_field.b_field",
          "a_field.c_field.e_field",
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

    val schema11 = SchemaMap("com.acme", "example1", "jsonschema", SchemaVer.Full(1,0,0))
    val schema12 = SchemaMap("com.acme", "example1", "jsonschema", SchemaVer.Full(1,0,1))

    val schema21 = SchemaMap("com.acme", "example2", "jsonschema", SchemaVer.Full(1,0,0))
    val schema22 = SchemaMap("com.acme", "example2", "jsonschema", SchemaVer.Full(1,0,1))

    val schema31 = SchemaMap("com.acme", "example3", "jsonschema", SchemaVer.Full(1,0,0))
    val schema32 = SchemaMap("com.acme", "example3", "jsonschema", SchemaVer.Full(1,0,1))

    val schemas = List(
      SelfDescribingSchema(schema11, initial),
      SelfDescribingSchema(schema12, second),
      SelfDescribingSchema(schema21, initial),
      SelfDescribingSchema(schema22, second),
      SelfDescribingSchema(schema31, initial),
      SelfDescribingSchema(schema32, second)
    )
    val migrationMap = Migration.buildMigrationMap(schemas)

    migrationMap.keySet must beEqualTo(Set(schema11, schema21, schema31))
  }
}
