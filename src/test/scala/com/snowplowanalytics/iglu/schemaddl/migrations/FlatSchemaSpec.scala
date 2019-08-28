/*
 * Copyright (c) 2014-2016 Snowplow Analytics Ltd. All rights reserved.
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

import cats.implicits._
import cats.data._

import io.circe.literal._

import org.specs2.Specification
import org.specs2.matcher.Matcher

import com.snowplowanalytics.iglu.core.SelfDescribingSchema
import com.snowplowanalytics.iglu.core.{SchemaMap, SchemaVer}
import com.snowplowanalytics.iglu.schemaddl.SubSchemas
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.CommonProperties.{Description, Type}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.{Pointer, Schema}
import com.snowplowanalytics.iglu.schemaddl.SpecHelpers._


class FlatSchemaSpec extends Specification { def is = s2"""
    build recognizes a JSON schema without properties $e1
    build recognizes an object property without 'properties' as primitive $e2
    build recognizes an empty self-describing schema as empty FlatSchema $e3
    build recognizes an array as primitive $e4
    build transforms object,string union type into single primitive $e5
    build adds all required properties and skips not-nested required $e6
    nestedRequired returns true if all parent properties are required (no null in type) $e7
    build skips properties inside patternProperties $e8
    build recognizes oneOf with object and string as primitive $e9
    isHeterogeneousUnion recognizes a Schema with oneOf $e10
    build recognizes optional enum field $e11
    build recognizes optional nested enum field $e12
    build recognizes field without type $e13
    create correct ordered subschemas from 1-0-0 to 1-0-1 $e14
    create correct ordered subschemas from 1-0-0 to 1-0-2 $e15
    create correct ordered subschemas for complex schema $e16
    create correct ordered subschemas for complex schema $e17
  """

  def e1 = {
    val schema = json"""{"type": "object"}""".schema
    val expected = FlatSchema(
      Set(Pointer.Root -> Schema.empty.copy(`type` = Some(Type.Object))),
      Set.empty,
      Set.empty)

    FlatSchema.build(schema) must beEqualTo(expected)
  }

  def e2 = {
    val json = json"""
      {
        "type": "object",
        "properties": {
          "nested": {
            "type": "object",
            "properties": {
              "object_without_properties": {
                "type": "object"
              }
            }
          }
        }
      }
    """.schema

    val subSchemas = Set(
      "/properties/nested/properties/object_without_properties".jsonPointer ->
        json"""{"type": ["object", "null"]}""".schema)

    val result = FlatSchema.build(json)

    val parentsExpectation = result.parents.map(_._1) must contain(Pointer.Root, "/properties/nested".jsonPointer)

    (result.subschemas must beEqualTo(subSchemas)) and (result.required must beEmpty) and parentsExpectation
  }

  def e3 = {
    val json = json"""
      {
      	"description": "Wildcard schema #1 to match any valid JSON instance",
      	"self": {
      		"vendor": "com.snowplowanalytics.iglu",
      		"name": "anything-a",
      		"format": "jsonschema",
      		"version": "1-0-0"
      	}
      }
    """.schema
    val description = "Wildcard schema #1 to match any valid JSON instance"
    val expected = FlatSchema(Set(
      Pointer.Root -> Schema.empty.copy(description = Some(Description(description)))),
      Set.empty,
      Set.empty)

    val res = FlatSchema.build(json)

    res must beEqualTo(expected)
  }

  def e4 = {
    val schema = json"""
      {
        "type": "object",
        "properties": {
          "foo": {
            "type": "array",
            "items": {
              "type": "string"
            }
          }
        },
        "additionalProperties": false
      }
    """.schema

    val expected = Set(
      "/properties/foo".jsonPointer ->
        json"""{"type": ["array", "null"], "items": {"type": "string"}}""".schema
    )

    val result = FlatSchema.build(schema)

    val subschemasExpectation = result.subschemas must beEqualTo(expected)
    val requiredExpectation = result.required must beEmpty
    val parentsExpectation = result.parents.map(_._1) must contain(Pointer.Root)

    subschemasExpectation and requiredExpectation and parentsExpectation
  }

  def e5 = {
    val schema = json"""
      {
        "type": "object",
        "properties": {
          "foo": {
            "type": ["string", "object"],
            "properties": {
              "one": {
                "type": "string"
              },
              "two": {
                "type": "integer"
              }
            }
          },
          "a_field": {
            "type": ["string", "integer"]
          },
          "b_field": {
            "type": "string"
          },
          "c_field": {
            "type": ["integer", "number"]
          },
          "d_field": {
            "type": "object",
            "properties": {
              "one": {
                "type": ["integer", "object"],
                "properties": {
                  "two": {
                    "type": "string"
                  },
                  "three": {
                    "type": "integer"
                  }
                }
              }
            }
          }
        },
        "additionalProperties": false
      }
    """.schema

    val result = FlatSchema.build(schema)

    val expectedSubschemas = Set(
      "/properties/foo".jsonPointer ->
        json"""{
          "type": ["string", "object", "null"],
          "properties": {
            "one": {
              "type": "string"
            },
            "two": {
              "type": "integer"
            }
          }
        }""".schema,
      "/properties/d_field/properties/one".jsonPointer ->
        json"""{
          "type": ["integer", "object", "null"],
          "properties": {
            "two": {
              "type": "string"
            },
            "three": {
              "type": "integer"
            }
          }
        }""".schema,
      "/properties/a_field".jsonPointer -> json"""{"type": ["string", "integer", "null"]}""".schema,
      "/properties/b_field".jsonPointer -> json"""{"type": ["string", "null"]}""".schema,
      "/properties/c_field".jsonPointer -> json"""{"type": ["integer", "number", "null"]}""".schema
    )

    result.subschemas must beEqualTo(expectedSubschemas) and (result.required must beEmpty)
  }

  def e6 = {
    val schema = json"""
      {
        "type": "object",
        "required": ["foo"],
        "properties": {
          "foo": {
            "type": "object",
            "required": ["one"],
            "properties": {
              "one": {
                "type": "string"
              },
              "nonRequiredNested": {
                "type": "object",
                "required": ["nestedRequired"],
                "properties": {
                  "nestedRequired": {"type": "integer"}
                }
              }
            }
          }
        },
        "additionalProperties": false
      }
    """.schema

    val result = FlatSchema.build(schema)

    val expectedRequired = Set("/properties/foo".jsonPointer, "/properties/foo/properties/one".jsonPointer)
    val expectedSubschemas = Set(
      "/properties/foo/properties/nonRequiredNested/properties/nestedRequired".jsonPointer ->
        json"""{"type": ["integer", "null"]}""".schema,
      "/properties/foo/properties/one".jsonPointer ->
        json"""{"type": "string"}""".schema
    )

    val required = result.required must bePointers(expectedRequired)
    val subschemas = result.subschemas must beEqualTo(expectedSubschemas)

    required and subschemas
  }

  def e7 = {
    val subschemas: SubSchemas =
      Set("/deeply".jsonPointer, "/deeply/nested".jsonPointer, "/other/property".jsonPointer)
        .map((p: Pointer.SchemaPointer) => p -> Schema.empty)

    val schema = FlatSchema(subschemas, Set("/deeply".jsonPointer, "/deeply/nested".jsonPointer), Set.empty[(Pointer.SchemaPointer, Schema)])
    val result = schema.nestedRequired("/deeply/nested/property".jsonPointer)

    result must beTrue
  }

  def e8 = {
    val schema = json"""
      {
        "type": "object",
        "required": ["one"],
        "properties": {
          "one": {
            "type": "object",
            "required": ["two"],
            "properties": {
              "two": {
                "type": "string"
              },
              "withProps": {
                "type": "object",
                "patternProperties": {
                  ".excluded": {"type": "string"},
                  ".excluded-with-required": {
                    "type": "object",
                    "properties": {
                      "also-excluded": {"type": "integer"}
                    }
                  }
                },
                "properties": {
                  "included": {"type": "integer"}
                }
              }
            }
          }
        },
        "additionalProperties": false
      }
    """.schema

    val result = FlatSchema.build(schema)

    val expectedRequired = Set("/properties/one".jsonPointer, "/properties/one/properties/two".jsonPointer)
    val expectedSubschemas = Set(
      "/properties/one/properties/two".jsonPointer ->
        json"""{"type": "string"}""".schema,
      "/properties/one/properties/withProps/properties/included".jsonPointer ->
        json"""{"type": ["integer", "null"]}""".schema
    )

    val required = result.required must bePointers(expectedRequired)
    val subschemas = result.subschemas must beEqualTo(expectedSubschemas)

    required and subschemas
  }

  def e9 = {
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
        "additionalProperties": false
      }
    """.schema

    val subSchemas = Set(
      "/properties/union".jsonPointer ->
        json"""{
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
        }""".schema.copy(`type` = Some(Type.Null))
    )

    val result = FlatSchema.build(json)

    (result.subschemas must beEqualTo(subSchemas)) and (result.required must beEmpty)
  }

  def e10 = {
    val json = json"""
      {
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
      """.schema

    FlatSchema.isHeterogeneousUnion(json) must beTrue
  }

  def e11 = {
    val schema = json"""
        {
         "type": "object",
         "properties": {
           "enum_field": {
             "enum": [
               "event",
               "exception",
               "item"
             ]
           },
           "nonInteractionHit": {
             "type": ["boolean", "null"]
           }
         },
         "additionalProperties": false
        }
      """.schema

    val expectedSubSchemas = Set(
      "/properties/enum_field".jsonPointer ->
        json"""{"enum": ["event","exception","item"]}""".schema.copy(`type` = Some(Type.Null)),
      "/properties/nonInteractionHit".jsonPointer ->
        json"""{"type": ["boolean", "null"]}""".schema)

    val result = FlatSchema.build(schema)

    (result.subschemas must beEqualTo(expectedSubSchemas)) and (result.required must beEmpty)
  }


  def e12 = {
    val schema = json"""
        {
         "type": "object",
         "properties": {
           "a_field": {
            "type": "object",
            "properties": {
             "enum_field": {
               "enum": [
                 "event",
                 "exception",
                 "item"
               ]
             }
            }
           },
           "nonInteractionHit": {
             "type": ["boolean", "null"]
           }
         },
         "additionalProperties": false
        }
      """.schema

    val expectedSubSchemas = Set(
      "/properties/a_field/properties/enum_field".jsonPointer ->
        json"""{"enum": ["event","exception","item"]}""".schema.copy(`type` = Some(Type.Null)),
      "/properties/nonInteractionHit".jsonPointer ->
        json"""{"type": ["boolean", "null"]}""".schema)

    val result = FlatSchema.build(schema)

    (result.subschemas must beEqualTo(expectedSubSchemas)) and (result.required must beEmpty)
  }

  def e13 = {
    val schema = json"""
        {
          "type": "object",
          "properties": {
            "a_field": { "type": "string" },
            "b_field": {}
          }
        }
      """.schema

    val expectedSubSchemas = Set(
      "/properties/a_field".jsonPointer -> json"""{"type": ["string", "null"]}""".schema,
      "/properties/b_field".jsonPointer -> Schema.empty.copy(`type` = Some(Type.Null))
    )

    val result = FlatSchema.build(schema)

    (result.subschemas must beEqualTo(expectedSubSchemas)) and (result.required must beEmpty)
  }

  def e14 = {
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

    val schemaList = SchemaList.Full(NonEmptyList.of(initialSchema, secondSchema))

    val res = extractOrder(FlatSchema.buildOrderedSubSchemas(schemaList))

    val expected = List("foo", "b_field", "a_field")

    res must beEqualTo(expected)
  }

  def e15 = {
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

    val schemaList = SchemaList.Full(NonEmptyList.of(initialSchema, secondSchema, thirdSchema))

    val res = extractOrder(FlatSchema.buildOrderedSubSchemas(schemaList))

    val expected = List("foo", "bar", "c_field", "a_field", "d_field")

    res must beEqualTo(expected)
  }

  def e16 = {
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

    val schemaList = SchemaList.Full(NonEmptyList.of(initialSchema, secondSchema))

    val res = extractOrder(FlatSchema.buildOrderedSubSchemas(schemaList))

    val expected = List(
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

    res must beEqualTo(expected)
  }

  def e17 = {
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

    val schemaList = SchemaList.Full(NonEmptyList.of(initialSchema, secondSchema, thirdSchema))

    val res = extractOrder(FlatSchema.buildOrderedSubSchemas(schemaList))

    val expected = List(
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

    res must beEqualTo(expected)
  }

  def bePointers(expected: Set[Pointer.SchemaPointer]): Matcher[Set[Pointer.SchemaPointer]] = { actual: Set[Pointer.SchemaPointer] =>
    val result = s"""|actual: ${actual.toList.map(_.show).sortBy(_.length).mkString(", ")}
                     |expected: ${expected.toList.map(_.show).sortBy(_.length).mkString(", ")}""".stripMargin
    (actual == expected, result)
  }
}
