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
package com.snowplowanalytics.iglu.schemaddl.jsonschema.mutate

import com.snowplowanalytics.iglu.schemaddl.SpecHelpers

class MutateSpec extends org.specs2.Specification {

  def is = s2"""
    forStorage should not make any changes to a simpe schema $common1
    forStorage should merge oneOf string/number into a union type $common2
    forStorage should merge oneOf enums into an extended enum $common3
    forStorage should merge anyOf string/number into a union type $common4
    forStorage should merge anyOf enums into an extended enum $common5

    NUMBERS

    forStorage should take widest minimum and maximum from oneOf fields $numbers1
    forStorage should take narrowest minimum and maximum between parent and oneOf field $numbers2a $numbers2b
    forStorage should drop incompatible multipleOfs $numbers3

    STRINGS

    forStorage should take widest minLength and maxLength from oneOf fields $strings1
    forStorage should take narrowest minLength and maxLength between parent and oneOf field $strings2a $strings2b
    forStorage should preserve compatible pattern and format $strings3
    forStorage should drop incompatible pattern and format $strings4

    OBJECTS

    forStorage should merge oneOf object properties into a common object type $object1
    forStorage should merge common oneOf properties into a union type property $object2
    forStorage should take widest minLength and maxLength from common stringy fields $object3
    forStorage should take widest minimum and maximum from common numeric fields $object4
    forStorage should filter required fields from oneOf properties $object5
    forStorage should ignore pattern properties $object6
    forStorage should merge enums of a common oneOf property $object7

    ARRAYS

    forStorage should merge oneOf items into a union type item $array1
    forStorage should take widest minItems and maxItems from oneOf alternatives $array2
    forStorage should weaken tuples of additionalItems $array3
    """
  def common1 = {
    val input = SpecHelpers.parseSchema(
      """
        |{
        |"type": ["object", "null"],
        | "description": "simple schema",
        | "properties": {
        |   "numeric": {
        |     "description": "this is a number",
        |     "type": "number",
        |     "minimum": 150,
        |     "maximum": 200,
        |     "multipleOf": 10
        |   },
        |   "stringy": {
        |     "type": "string",
        |     "minLength": 150,
        |     "maxLength": 200,
        |     "pattern": "^[a-z0-9-]*$",
        |     "format": "uuid"
        |   },
        |   "listy": {
        |     "type": "array",
        |     "items": {"type": "number"},
        |     "minItems": 2,
        |     "minItems": 2
        |   },
        |   "status": {
        |     "enum": ["HAPPY", "SAD"]
        |   }
        | },
        | "additionalProperties": false,
        | "required": ["numeric", "stringy"]
        |}
      """.stripMargin)

    Mutate.forStorage(input) must_== input
  }

  def common2 = {
    val input = SpecHelpers.parseSchema(
      """
        |{
        | "oneOf": [
        |   {"type": "string"},
        |   {"type": "number"}
        | ]
        |}
      """.stripMargin)

    val expected = SpecHelpers.parseSchema(
      """
        |{
        |"type": ["string", "number"]
        |}
      """.stripMargin)
    Mutate.forStorage(input) must_== expected
  }

  def common3 = {
    val input = SpecHelpers.parseSchema(
      """
        |{
        | "oneOf": [
        |   {"enum": ["x", "y", "z"]},
        |   {"enum": ["a", "b", "c"]}
        | ]
        |}
      """.stripMargin)

    val expected = SpecHelpers.parseSchema(
      """
        |{
        |"enum": ["x", "y", "z", "a", "b", "c"]
        |}
      """.stripMargin)
    Mutate.forStorage(input) must_== expected
  }

  def common4 = {
    val input = SpecHelpers.parseSchema(
      """
        |{
        | "anyOf": [
        |   {"type": "string"},
        |   {"type": "number"}
        | ]
        |}
      """.stripMargin)

    val expected = SpecHelpers.parseSchema(
      """
        |{
        |"type": ["string", "number"]
        |}
      """.stripMargin)
    Mutate.forStorage(input) must_== expected
  }

  def common5 = {
    val input = SpecHelpers.parseSchema(
      """
        |{
        | "anyOf": [
        |   {"enum": ["x", "y", "z"]},
        |   {"enum": ["a", "b", "c"]}
        | ]
        |}
      """.stripMargin)

    val expected = SpecHelpers.parseSchema(
      """
        |{
        |"enum": ["x", "y", "z", "a", "b", "c"]
        |}
      """.stripMargin)
    Mutate.forStorage(input) must_== expected
  }


  def object1 = {
    val input = SpecHelpers.parseSchema(
      """
        |{
        |"type": "object",
        |"oneOf": [
        |  {
        |    "properties": {"a": {"type": "string"}}
        |  },
        |  {
        |    "properties": {"b": {"type": "string"}}
        |  }
        |]
        |}
      """.stripMargin)

    val expected = SpecHelpers.parseSchema(
      """
        |{
        |"type": "object",
        |"properties": {
        |  "a": {"type": "string"},
        |  "b": {"type": "string"}
        |},
        |"additionalProperties": false
        |}
      """.stripMargin)
    Mutate.forStorage(input) must_== expected
  }

  def object2 = {
    val input = SpecHelpers.parseSchema(
      """
        |{
        |"type": "object",
        |"oneOf": [
        |  {
        |    "properties": {"a": {"type": "string"}}
        |  },
        |  {
        |    "properties": {"a": {"type": "number"}}
        |  }
        |]
        |}
      """.stripMargin)

    val expected = SpecHelpers.parseSchema(
      """
        |{
        |"type": "object",
        |"properties": {
        |  "a": {"type": ["string", "number"]}
        |},
        |"additionalProperties": false
        |}
      """.stripMargin)
    Mutate.forStorage(input) must_== expected
  }

  def object3 = {
    val input = SpecHelpers.parseSchema(
      """
        |{
        |"type": "object",
        |"additionalProperties": false,
        |"oneOf": [
        |  {
        |    "properties": {
        |      "stringy": {
        |        "type": "string",
        |        "minLength": 10,
        |        "maxLength": 100
        |      }
        |    }
        |  },
        |  {
        |    "properties": {
        |      "stringy": {
        |        "type": "string",
        |        "minLength": 5,
        |        "maxLength": 105
        |      }
        |    }
        |  }
        |]
        |}
      """.stripMargin)

    val expected = SpecHelpers.parseSchema(
      """
        |{
        |"type": "object",
        |"additionalProperties": false,
        |"properties": {
        |  "stringy": {
        |    "type": "string",
        |    "minLength": 5,
        |    "maxLength": 105
        |  }
        |}
        |}
      """.stripMargin)
    Mutate.forStorage(input) must_== expected
  }

  def object4 = {
    val input = SpecHelpers.parseSchema(
      """
        |{
        |"type": "object",
        |"additionalProperties": false,
        |"oneOf": [
        |  {
        |    "properties": {
        |      "numeric": {
        |        "type": "integer",
        |        "minimum": 10,
        |        "maximum": 100
        |      }
        |    }
        |  },
        |  {
        |    "properties": {
        |      "numeric": {
        |        "type": "integer",
        |        "minimum": 5,
        |        "maximum": 105
        |      }
        |    }
        |  }
        |]
        |}
      """.stripMargin)

    val expected = SpecHelpers.parseSchema(
      """
        |{
        |"type": "object",
        |"additionalProperties": false,
        |"properties": {
        |  "numeric": {
        |    "type": "integer",
        |    "minimum": 5,
        |    "maximum": 105
        |  }
        |}
        |}
      """.stripMargin)
    Mutate.forStorage(input) must_== expected
  }

  def object5 = {
    val input = SpecHelpers.parseSchema(
      """
        |{
        |"type": "object",
        |"additionalProperties": false,
        |"oneOf": [
        |  {
        |    "properties": {
        |      "a": {"type": "string"},
        |      "b": {"type": "string"}
        |    },
        |    "required": ["a", "b"]
        |  },
        |  {
        |    "properties": {
        |      "a": {"type": "string"},
        |      "b": {"type": "string"}
        |    },
        |    "required": ["a"]
        |  },
        |  {
        |    "properties": {
        |      "a": {"type": "string"},
        |      "b": {"type": "string"},
        |      "c": {"type": "string"}
        |    },
        |    "required": ["a", "c"]
        |  }
        |]
        |}
      """.stripMargin)

    val expected = SpecHelpers.parseSchema(
      """
        |{
        |"type": "object",
        |"additionalProperties": false,
        |"properties": {
        |  "a": {"type": "string"},
        |  "b": {"type": "string"},
        |  "c": {"type": "string"}
        |},
        |"required": ["a"]
        |}
      """.stripMargin)
    Mutate.forStorage(input) must_== expected
  }

  def object6 = {
    val input = SpecHelpers.parseSchema(
      """
        |{
        |"type": "object",
        |"patternProperties": {
        |  "^A_": {"type": "number"}
        |}
        |}
      """.stripMargin)

    val expected = SpecHelpers.parseSchema(
      """
        |{
        |"type": "object",
        |"additionalProperties": false
        |}
      """.stripMargin)
    Mutate.forStorage(input) must_== expected
  }

  def object7 = {
    val input = SpecHelpers.parseSchema(
      """
        |{
        |"type": "object",
        |"oneOf": [
        |  {
        |    "properties": {"a": {"enum": ["x", "y", "z"]}}
        |  },
        |  {
        |    "properties": {"a": {"enum": ["a", "b", "c"]}}
        |  }
        |]
        |}
      """.stripMargin)

    val expected = SpecHelpers.parseSchema(
      """
        |{
        |"type": "object",
        |"properties": {
        |  "a": {"enum": ["x", "y", "z", "a", "b", "c"]}
        |},
        |"additionalProperties": false
        |}
      """.stripMargin)
    Mutate.forStorage(input) must_== expected
  }

  def array1 = {
    val input = SpecHelpers.parseSchema(
      """
        |{
        |"type": "array",
        |"oneOf": [
        |  {
        |    "items": {"type": "string"}
        |  },
        |  {
        |    "items": {"type": "number"}
        |  }
        |]
        |}
      """.stripMargin)

    val expected = SpecHelpers.parseSchema(
      """
        |{
        |"type": "array",
        |"items": {"type": ["string", "number"]}
        |}
      """.stripMargin)
    Mutate.forStorage(input) must_== expected
  }

  def array2 = {
    val input = SpecHelpers.parseSchema(
      """
        |{
        |"type": "array",
        |"oneOf": [
        |  {
        |    "maxItems": 42,
        |    "minItems": 10
        |  },
        |  {
        |    "maxItems": 100,
        |    "minItems": 2
        |  }
        |]
        |}
      """.stripMargin)

    val expected = SpecHelpers.parseSchema(
      """
        |{
        |"type": "array",
        |"maxItems": 100,
        |"minItems": 2
        |}
      """.stripMargin)
    Mutate.forStorage(input) must_== expected
  }

  def array3 = {
    val input = SpecHelpers.parseSchema(
      """
        |{
        |"type": "array",
        |"oneOf": [
        |  {
        |    "items": [
        |      {"type": "string"}
        |    ],
        |    "additionalItems": false
        |  },
        |  {
        |    "items": [
        |      {"type": "string"}
        |    ],
        |    "additionalItems": false
        |  }
        |]
        |}
      """.stripMargin)

    val expected = SpecHelpers.parseSchema(
      """
        |{
        |"type": "array",
        |"items": {"type": "string"}
        |}
      """.stripMargin)
    Mutate.forStorage(input) must_== expected
  }

  def numbers1 = {
    val input = SpecHelpers.parseSchema(
      """
        |{
        |"type": "number",
        |"oneOf": [
        |  {
        |    "multipleOf": 0.3,
        |    "minimum": 0.1,
        |    "maximum": 100
        |  },
        |  {
        |    "multipleOf": 0.3,
        |    "minimum": 42.0,
        |    "maximum": 123.45
        |  }
        |]
        |}
      """.stripMargin)

    val expected = SpecHelpers.parseSchema(
      """
        |{
        |"type": "number",
        |"multipleOf": 0.3,
        |"minimum": 0.1,
        |"maximum": 123.45
        |}
      """.stripMargin)
    Mutate.forStorage(input) must_== expected
  }

  def numbers2a = {
    val input = SpecHelpers.parseSchema(
      """
        |{
        |"type": "number",
        |"multipleOf": 0.3,
        |"minimum": 0.1,
        |"maximum": 12345,
        |"oneOf": [
        |  {
        |    "multipleOf": 0.3,
        |    "minimum": 42.0,
        |    "maximum": 1000
        |  }
        |]
        |}
      """.stripMargin)

    val expected = SpecHelpers.parseSchema(
      """
        |{
        |"type": "number",
        |"multipleOf": 0.3,
        |"minimum": 42.0,
        |"maximum": 1000
        |}
      """.stripMargin)
    Mutate.forStorage(input) must_== expected
  }

  def numbers2b = {
    val input = SpecHelpers.parseSchema(
      """
        |{
        |"type": "number",
        |"multipleOf": 0.3,
        |"minimum": 42.0,
        |"maximum": 1000,
        |"oneOf": [
        |  {
        |    "multipleOf": 0.3,
        |    "minimum": 0.1,
        |    "maximum": 12345
        |  }
        |]
        |}
      """.stripMargin)

    val expected = SpecHelpers.parseSchema(
      """
        |{
        |"type": "number",
        |"multipleOf": 0.3,
        |"minimum": 42.0,
        |"maximum": 1000
        |}
      """.stripMargin)
    Mutate.forStorage(input) must_== expected
  }

  def numbers3 = {
    val input = SpecHelpers.parseSchema(
      """
        |{
        |"type": "number",
        |"oneOf": [
        |  {
        |    "multipleOf": 0.3
        |  },
        |  {
        |    "multipleOf": 0.7
        |  }
        |]
        |}
      """.stripMargin)

    val expected = SpecHelpers.parseSchema(
      """
        |{
        |"type": "number"
        |}
      """.stripMargin)
    Mutate.forStorage(input) must_== expected
  }

  def strings1 = {
    val input = SpecHelpers.parseSchema(
      """
        |{
        |"type": "string",
        |"oneOf": [
        |  {
        |    "minLength": 5,
        |    "maxLength": 105
        |  },
        |  {
        |    "minLength": 10,
        |    "maxLength": 90
        |  }
        |]
        |}
      """.stripMargin)

    val expected = SpecHelpers.parseSchema(
      """
        |{
        |"type": "string",
        |"minLength": 5,
        |"maxLength": 105
        |}
      """.stripMargin)
    Mutate.forStorage(input) must_== expected
  }

  def strings2a = {
    val input = SpecHelpers.parseSchema(
      """
        |{
        |"type": "string",
        |"minLength": 5,
        |"maxLength": 105,
        |"oneOf": [
        |  {
        |    "minLength": 10,
        |    "maxLength": 90
        |  }
        |]
        |}
      """.stripMargin)

    val expected = SpecHelpers.parseSchema(
      """
        |{
        |"type": "string",
        |"minLength": 10,
        |"maxLength": 90
        |}
      """.stripMargin)
    Mutate.forStorage(input) must_== expected
  }

  def strings2b = {
    val input = SpecHelpers.parseSchema(
      """
        |{
        |"type": "string",
        |"minLength": 10,
        |"maxLength": 90,
        |"oneOf": [
        |  {
        |    "minLength": 5,
        |    "maxLength": 105
        |  }
        |]
        |}
      """.stripMargin)

    val expected = SpecHelpers.parseSchema(
      """
        |{
        |"type": "string",
        |"minLength": 10,
        |"maxLength": 90
        |}
      """.stripMargin)
    Mutate.forStorage(input) must_== expected
  }

  def strings3 = {
    val input = SpecHelpers.parseSchema(
      """
        |{
        |"type": "string",
        |"oneOf": [
        |  {
        |    "pattern": "^[a-z0-9-]*$",
        |    "format": "uuid"
        |  },
        |  {
        |    "pattern": "^[a-z0-9-]*$",
        |    "format": "uuid"
        |  }
        |]
        |}
      """.stripMargin)

    val expected = SpecHelpers.parseSchema(
      """
        |{
        |"type": "string",
        |"pattern": "^[a-z0-9-]*$",
        |"format": "uuid"
        |}
      """.stripMargin)
    Mutate.forStorage(input) must_== expected
  }

  def strings4 = {
    val input = SpecHelpers.parseSchema(
      """
        |{
        |"type": "string",
        |"oneOf": [
        |  {
        |    "pattern": "^[a-z0-9-]*$",
        |    "format": "uuid"
        |  },
        |  {
        |    "pattern": "^[0-9\\.]*$",
        |    "format": "ipv4"
        |  }
        |]
        |}
      """.stripMargin)

    val expected = SpecHelpers.parseSchema(
      """
        |{
        |"type": "string"
        |}
      """.stripMargin)
    Mutate.forStorage(input) must_== expected
  }

}
