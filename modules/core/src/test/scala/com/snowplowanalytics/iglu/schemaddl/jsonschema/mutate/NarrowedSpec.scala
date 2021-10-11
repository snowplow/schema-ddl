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

class NarrowedSpec extends org.specs2.Specification {

  def is = s2"""
    narrowed should not make any changes when narrowing two identical schemas $common1
    narrowed should find the intersect of schemas with union types $common2
    narrowed should merge incompatible types into an impossible schema $common3
    narrowed should merge enums into a narrowed enum $common4

    NUMBERS

    narrowed should take narrowest minimum $numbers1
    narrowed should take narrowest maximum $numbers2
    narrowed should take the minimum of two multipleOfs $numbers3

    STRINGS

    narrowed should take narrowest minLength $strings1
    narrowed should take narrowest maxLength $strings2
    narrowed should take the first of alternative pattern and format $strings3

    OBJECTS

    narrowed should narrow the schemas of common object properties $object1
    narrowed should take the union of non-overlapping object properties $object2
    narrowed should narrow the additionalProperties condition $object3
    narrowed should narrow the required condition $object4
    narrowed should narrow the patternProperties condition $object5

    ARRAYS

    narrowed should narrow the schemas of array items $array1
    narrowed should narrow the schemas of tuples with array items $array2
    narrowed should narrow the schemas of tuples with tuples $array3
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
        |     "items": {"type": "string"},
        |     "additionalItems": false,
        |     "minItems": 2,
        |     "minItems": 2
        |   },
        |   "status": {
        |     "enum": ["HAPPY", "SAD"]
        |   }
        | },
        | "additionalProperties": true,
        | "required": ["numeric", "stringy"],
        | "patternProperties": {
        |   "^S_": {"type": "number"}
        | }
        |}
      """.stripMargin)

    Narrowed(input, input) must_== input
  }

  def common2 = {
    val input1 = SpecHelpers.parseSchema(
      """
        |{
        |  "type": ["number", "string"]
        |}
      """.stripMargin)

    val input2 = SpecHelpers.parseSchema(
      """
        |{
        |  "type": ["boolean", "number"]
        |}
      """.stripMargin)

    val expected = SpecHelpers.parseSchema(
      """
        |{
        |  "type": ["number"]
        |}
      """.stripMargin)
    Narrowed(input1, input2) must_== expected
  }

  def common3 = {
    val input1 = SpecHelpers.parseSchema(
      """
        |{
        |  "type": "number"
        |}
      """.stripMargin)

    val input2 = SpecHelpers.parseSchema(
      """
        |{
        |  "type": "string"
        |}
      """.stripMargin)

    val expected = SpecHelpers.parseSchema(
      """
        |{
        |  "type": []
        |}
      """.stripMargin)
    Narrowed(input1, input2) must_== expected
  }

  def common4 = {
    val input1 = SpecHelpers.parseSchema(
      """
        |{
        |  "enum": ["x", "y", "z"]
        |}
      """.stripMargin)

    val input2 = SpecHelpers.parseSchema(
      """
        |{
        |  "enum": ["x", "YYYY", "z"]
        |}
      """.stripMargin)

    val expected = SpecHelpers.parseSchema(
      """
        |{
        |  "enum": ["x", "z"]
        |}
      """.stripMargin)
    Narrowed(input1, input2) must_== expected
  }

  def numbers1 = {
    def test(min1: String, min2: String, e1: String) = {
      val input1 = SpecHelpers.parseSchema(
        s"""
          |{
          | "type": "number",
          | "minimum": $min1
          |}
        """.stripMargin)

      val input2 = SpecHelpers.parseSchema(
        s"""
          |{
          | "type": "number",
          | "minimum": $min2
          |}
        """.stripMargin)

      val expected = SpecHelpers.parseSchema(
        s"""
          |{
          |"type": "number",
          |"minimum": $e1
          |}
        """.stripMargin)
      Narrowed(input1, input2) must_== expected
    }

    test("3", "8", "8") and
    test("8", "3", "8") and
    test("0.3", "8", "8") and
    test("8", "0.3", "8") and
    test("3", "8.8", "8.8") and
    test("8.8", "3", "8.8") and
    test("8.8", "3.3", "8.8") and
    test("3.3", "8.8", "8.8")
  }

  def numbers2 = {
    def test(max1: String, max2: String, e1: String) = {
      val input1 = SpecHelpers.parseSchema(
        s"""
          |{
          | "type": "number",
          | "maximum": $max1
          |}
        """.stripMargin)

      val input2 = SpecHelpers.parseSchema(
        s"""
          |{
          | "type": "number",
          | "maximum": $max2
          |}
        """.stripMargin)

      val expected = SpecHelpers.parseSchema(
        s"""
          |{
          |"type": "number",
          |"maximum": $e1
          |}
        """.stripMargin)
      Narrowed(input1, input2) must_== expected
    }

    test("3", "8", "3") and
    test("8", "3", "3") and
    test("0.3", "8", "0.3") and
    test("8", "0.3", "0.3") and
    test("3", "8.8", "3") and
    test("8.8", "3", "3") and
    test("8.8", "3.3", "3.3") and
    test("3.3", "null", "3.3") and
    test("null", "8.8", "8.8")
  }

  def numbers3 = {
    def test(mo1: String, mo2: String, e1: String) = {
      val input1 = SpecHelpers.parseSchema(
        s"""
          |{
          | "type": "number",
          | "multipleOf": $mo1
          |}
        """.stripMargin)

      val input2 = SpecHelpers.parseSchema(
        s"""
          |{
          | "type": "number",
          | "multipleOf": $mo2
          |}
        """.stripMargin)

      val expected = SpecHelpers.parseSchema(
        s"""
          |{
          |"type": "number",
          |"multipleOf": $e1
          |}
        """.stripMargin)
      Narrowed(input1, input2) must_== expected
    }

    test("3", "8", "3") and
    test("8", "3", "3") and
    test("0.3", "8", "0.3") and
    test("8", "0.3", "0.3") and
    test("8.8", "3.3", "3.3") and
    test("3.3", "8.8", "3.3") and
    test("3.3", "null", "3.3") and
    test("null", "3.3", "3.3")

  }


  def strings1 = {
    def test(min1: String, min2: String, e1: String) = {
      val input1 = SpecHelpers.parseSchema(
        s"""
          |{
          | "type": "string",
          | "minLength": $min1
          |}
        """.stripMargin)

      val input2 = SpecHelpers.parseSchema(
        s"""
          |{
          | "type": "string",
          | "minLength": $min2
          |}
        """.stripMargin)

      val expected = SpecHelpers.parseSchema(
        s"""
          |{
          |"type": "string",
          |"minLength": $e1
          |}
        """.stripMargin)
      Narrowed(input1, input2) must_== expected
    }

    test("3", "8", "8") and
    test("8", "3", "8") and
    test("8", "null", "8") and
    test("null", "8", "8")
  }

  def strings2 = {
    def test(max1: String, max2: String, e1: String) = {
      val input1 = SpecHelpers.parseSchema(
        s"""
          |{
          | "type": "string",
          | "maxLength": $max1
          |}
        """.stripMargin)

      val input2 = SpecHelpers.parseSchema(
        s"""
          |{
          | "type": "string",
          | "maxLength": $max2
          |}
        """.stripMargin)

      val expected = SpecHelpers.parseSchema(
        s"""
          |{
          |"type": "string",
          |"maxLength": $e1
          |}
        """.stripMargin)
      Narrowed(input1, input2) must_== expected
    }

    test("3", "8", "3") and
    test("8", "3", "3") and
    test("8", "null", "8") and
    test("null", "8", "8")
  }

  def strings3 = {
    val input1 = SpecHelpers.parseSchema(
      """
        |{
        | "type": "string",
        | "pattern": "^[a-z0-9-]*$",
        | "format": "uuid"
        |}
      """.stripMargin)

    val input2 = SpecHelpers.parseSchema(
      """
        |{
        | "type": "string",
        | "pattern": "^[0-9\\.]*$",
        | "format": "ipv4"
        |}
      """.stripMargin)

    (Narrowed(input1, input2) must_== input1) and (Narrowed(input2, input1) must_== input2)
  }

  def object1 = {
    val input1 = SpecHelpers.parseSchema(
      """
        |{
        | "type": "object",
        | "properties": {"a": {"type": ["number", "string"]}}
        |}
      """.stripMargin)

    val input2 = SpecHelpers.parseSchema(
      """
        |{
        | "type": "object",
        | "properties": {"a": {"type": ["boolean", "number"]}}
        |}
      """.stripMargin)

    val expected = SpecHelpers.parseSchema(
      """
        |{
        |"type": "object",
        | "properties": {"a": {"type": ["number"]}}
        |}
      """.stripMargin)
    Narrowed(input1, input2) must_== expected
  }

  def object2 = {
    val input1 = SpecHelpers.parseSchema(
      """
        |{
        | "type": "object",
        | "properties": {"a": {"type": "number"}}
        |}
      """.stripMargin)

    val input2 = SpecHelpers.parseSchema(
      """
        |{
        | "type": "object",
        | "properties": {"b": {"type": "string"}}
        |}
      """.stripMargin)

    val expected = SpecHelpers.parseSchema(
      """
        |{
        |"type": "object",
        | "properties": {
        |    "a": {"type": "number"},
        |    "b": {"type": "string"}
        |  }
        |}
      """.stripMargin)
    Narrowed(input1, input2) must_== expected
  }

  def object3 = {

    def test(a1: String, a2: String, e1: String) = {
      val input1 = SpecHelpers.parseSchema(
        s"""
          |{
          | "type": "object",
          | "properties": {"a": {"type": "number"}},
          | "additionalProperties": $a1
          |}
        """.stripMargin)

      val input2 = SpecHelpers.parseSchema(
        s"""
          |{
          | "type": "object",
          | "properties": {"a": {"type": "number"}},
          | "additionalProperties": $a2
          |}
        """.stripMargin)

      val expected = SpecHelpers.parseSchema(
        s"""
          |{
          |"type": "object",
          | "properties": {"a": {"type": "number"}},
          | "additionalProperties": $e1
          |}
        """.stripMargin)
      Narrowed(input1, input2) must_== expected
    }
    test("true", "true", "true") and
    test("true", "false", "false") and
    test("false", "true", "false") and
    test("false", "false", "false") and
    test("true", """{"type": "number"}""", """{"type": "number"}""") and
    test("false", """{"type": "number"}""", "false") and
    test("""{"type": ["number", "boolean"]}""", """{"type": ["string", "number"]}""", """{"type": ["number"]}""")
  }

  def object4 = {

    def test(a1: String, a2: String, e1: String) = {
      val input1 = SpecHelpers.parseSchema(
        s"""
          |{
          | "type": "object",
          | "properties": {
          |    "a": {"type": "number"},
          |    "b": {"type": "number"},
          |    "c": {"type": "number"}
          | },
          | "required": $a1
          |}
        """.stripMargin)

      val input2 = SpecHelpers.parseSchema(
        s"""
          |{
          | "type": "object",
          | "properties": {
          |    "a": {"type": "number"},
          |    "b": {"type": "number"},
          |    "c": {"type": "number"}
          | },
          | "required": $a2
          |}
        """.stripMargin)

      val expected = SpecHelpers.parseSchema(
        s"""
          |{
          | "type": "object",
          | "properties": {
          |    "a": {"type": "number"},
          |    "b": {"type": "number"},
          |    "c": {"type": "number"}
          | },
          | "required": $e1
          |}
        """.stripMargin)
      Narrowed(input1, input2) must_== expected
    }
    test("""["a"]""", """["a"]""", """["a"]""") and
    test("""["a"]""", """["b"]""", """["a", "b"]""") and
    test("""["b"]""", """["a"]""", """["b", "a"]""") and
    test("""["b"]""", """[]""", """["b"]""") and
    test("""["b"]""", "null", """["b"]""")
  }

  def object5 = {

    def test(a1: String, a2: String, e1: String) = {
      val input1 = SpecHelpers.parseSchema(
        s"""
          |{
          | "type": "object",
          | "patternProperties": $a1
          |}
        """.stripMargin)

      val input2 = SpecHelpers.parseSchema(
        s"""
          |{
          | "type": "object",
          | "patternProperties": $a2
          |}
        """.stripMargin)

      val expected = SpecHelpers.parseSchema(
        s"""
          |{
          |"type": "object",
          | "patternProperties": $e1
          |}
        """.stripMargin)
      Narrowed(input1, input2) must_== expected
    }

    test(
      """{"^a": {"type": ["number", "boolean"]}}""",
      """{"^a": {"type": ["string", "number"]}}""",
      """{"^a": {"type": ["number"]}}"""
    ) and
    test(
      """{"^a": {"type": "number"}}""",
      """{"^b": {"type": "string"}}""",
      """{"^a": {"type": "number"}, "^b": {"type": "string"}}"""
    )
  }

  def array1 = {
    val input1 = SpecHelpers.parseSchema(
      """
        |{
        | "type": "array",
        | "items": {"type": ["number", "string"]}
        |}
      """.stripMargin)

    val input2 = SpecHelpers.parseSchema(
      """
        |{
        | "type": "array",
        | "items": {"type": ["boolean", "number"]}
        |}
      """.stripMargin)

    val expected = SpecHelpers.parseSchema(
      """
        |{
        |"type": "array",
        | "items": {"type": ["number"]}
        |}
      """.stripMargin)
    Narrowed(input1, input2) must_== expected
  }

  def array2 = {
      val input1 = SpecHelpers.parseSchema(
        """
          |{
          | "type": "array",
          | "items": {"type": "number"}
          |}
        """.stripMargin)

      val input2 = SpecHelpers.parseSchema(
        s"""
          |{
          | "type": "array",
          | "items": [
          |  {"type": "number"}
          | ]
          |}
        """.stripMargin)

      val expected = SpecHelpers.parseSchema(
        s"""
          |{
          | "type": "array",
          | "items": {}
          |}
        """.stripMargin)
    Narrowed(input1, input2) must_== expected
  }

  def array3 = {
    val input1 = SpecHelpers.parseSchema(
      s"""
        |{
        | "type": "array",
        | "items": [
        |  {"type": "boolean"}
        | ]
        |}
      """.stripMargin)

    val input2 = SpecHelpers.parseSchema(
      s"""
        |{
        | "type": "array",
        | "items": [
        |  {"type": "boolean"}
        | ]
        |}
      """.stripMargin)

    val expected = SpecHelpers.parseSchema(
      s"""
        |{
        |"type": "array",
        | "items": {}
        |}
      """.stripMargin)
    Narrowed(input1, input2) must_== expected
  }
}
