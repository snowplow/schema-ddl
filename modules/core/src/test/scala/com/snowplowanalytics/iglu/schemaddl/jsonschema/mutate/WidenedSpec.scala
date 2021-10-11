/*
 * Copyright (c) 2018-2021 Snowplow Analytics Ltd. All rights reserved.
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

class WidenedSpec extends org.specs2.Specification {

  def is = s2"""
    widened should not make any changes when widening two identical schemas $common1
    widened should take the union of schemas with different types $common2
    widened should merge enums into a widened enum $common3

    NUMBERS

    widened should take widenest minimum $numbers1
    widened should take widenest maximum $numbers2
    widened should drop incompatible multipleOfs $numbers3

    STRINGS

    widened should take widenest minLength $strings1
    widened should take widenest maxLength $strings2
    widened should drop incompatible format $strings3
    widened should drop incompatible patterns $strings4

    OBJECTS

    widened should widen the schemas of common object properties $object1
    widened should take the union of non-overlapping object properties $object2
    widened should take the union of non-overlapping object patterhProperties $object3
    widened should widen the required condition $object4

    ARRAYS

    widened should widen the schemas of array items $array1
    widened should widen the schemas of tuples with array items $array2
    widened should widen the schemas of tuples with tuples $array3
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

    Widened(input, input) must_== input
  }

  def common2 = {
    val input1 = SpecHelpers.parseSchema(
      """
        |{
        |  "type": "string"
        |}
      """.stripMargin)

    val input2 = SpecHelpers.parseSchema(
      """
        |{
        |  "type": "number"
        |}
      """.stripMargin)

    val expected = SpecHelpers.parseSchema(
      """
        |{
        |  "type": ["string", "number"]
        |}
      """.stripMargin)
    Widened(input1, input2) must_== expected
  }

  def common3 = {
    val input1 = SpecHelpers.parseSchema(
      """
        |{
        |  "enum": ["x", "y", "z"]
        |}
      """.stripMargin)

    val input2 = SpecHelpers.parseSchema(
      """
        |{
        |  "enum": ["a", "b", "z"]
        |}
      """.stripMargin)

    val expected = SpecHelpers.parseSchema(
      """
        |{
        |  "enum": ["x", "y", "z", "a", "b"]
        |}
      """.stripMargin)
    Widened(input1, input2) must_== expected
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
      Widened(input1, input2) must_== expected
    }

    test("3", "8", "3") and
    test("8", "3", "3") and
    test("0.3", "8", "0.3") and
    test("8", "0.3", "0.3") and
    test("3", "8.8", "3") and
    test("8.8", "3", "3") and
    test("8.8", "3.3", "3.3") and
    test("3.3", "8.8", "3.3") and
    test("3.3", "null", "null") and
    test("null", "8.8", "null")
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
      Widened(input1, input2) must_== expected
    }

    test("3", "8", "8") and
    test("8", "3", "8") and
    test("0.3", "8", "8") and
    test("8", "0.3", "8") and
    test("3", "8.8", "8.8") and
    test("8.8", "3", "8.8") and
    test("8.8", "3.3", "8.8") and
    test("3.3", "null", "null") and
    test("null", "8.8", "null")
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
      Widened(input1, input2) must_== expected
    }

    test("3", "3", "3") and
    test("3", "3.0", "3") and
    test("0.3", "0.3", "0.3") and
    test("3", "8", "null") and
    test("8", "3", "null") and
    test("0.3", "8", "null") and
    test("8", "0.3", "null") and
    test("3.3", "null", "null") and
    test("null", "3.3", "null")

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
      Widened(input1, input2) must_== expected
    }

    test("3", "8", "3") and
    test("8", "3", "3") and
    test("8", "null", "null") and
    test("null", "8", "null")
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
      Widened(input1, input2) must_== expected
    }

    test("3", "8", "8") and
    test("8", "3", "8") and
    test("8", "null", "null") and
    test("null", "8", "null")
  }

  def strings3 = {
    def test(format1: String, format2: String, e1: String) = {
      val input1 = SpecHelpers.parseSchema(
        s"""
          |{
          | "type": "string",
          | "format": $format1
          |}
        """.stripMargin)

      val input2 = SpecHelpers.parseSchema(
        s"""
          |{
          | "type": "string",
          | "format": $format2
          |}
        """.stripMargin)

      val expected = SpecHelpers.parseSchema(
        s"""
          |{
          | "type": "string",
          | "format": $e1
          |}
        """.stripMargin)

      Widened(input1, input2) must_== expected
    }
    test(""""uuid"""", """"uuid"""", """"uuid"""") and
    test(""""ipv4"""", """"ipv4"""", """"ipv4"""") and
    test(""""uuid"""", "null", "null") and
    test("null", """"uuid"""", "null") and
    test(""""ipv4"""", """"uuid"""", "null")
  }

  def strings4 = {
    def test(pattern1: String, pattern2: String, e1: String) = {
      val input1 = SpecHelpers.parseSchema(
        s"""
          |{
          | "type": "string",
          | "pattern": $pattern1
          |}
        """.stripMargin)

      val input2 = SpecHelpers.parseSchema(
        s"""
          |{
          | "type": "string",
          | "pattern": $pattern2
          |}
        """.stripMargin)

      val expected = SpecHelpers.parseSchema(
        s"""
          |{
          | "type": "string",
          | "pattern": $e1
          |}
        """.stripMargin)

      Widened(input1, input2) must_== expected
    }
    test(""""^abc$"""", """"^abc$"""", """"^abc$"""") and
    test(""""^xyz$"""", """"^xyz$"""", """"^xyz$"""") and
    test(""""^abc$"""", "null", "null") and
    test("null", """"^abc$"""", "null") and
    test(""""^abc$"""", """"^xyz$"""", "null")

  }

  def object1 = {
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
        | "properties": {"a": {"type": "boolean"}}
        |}
      """.stripMargin)

    val expected = SpecHelpers.parseSchema(
      """
        |{
        |"type": "object",
        | "properties": {"a": {"type": ["number", "boolean"]}}
        |}
      """.stripMargin)
    Widened(input1, input2) must_== expected
  }

  def object2 = {
    def test(ap1: String, ap2: String, expectedA: String, expectedB: String, expectedAP: String) = {
      val input1 = SpecHelpers.parseSchema(
        s"""
          |{
          | "type": "object",
          | "properties": {"a": {"type": "number"}},
          | "additionalProperties": $ap1
          |}
        """.stripMargin)

      val input2 = SpecHelpers.parseSchema(
        s"""
          |{
          | "type": "object",
          | "properties": {"b": {"type": "string"}},
          | "additionalProperties": $ap2
          |}
        """.stripMargin)

      val expected = SpecHelpers.parseSchema(
        s"""
          |{
          |"type": "object",
          | "properties": {
          |    "a": $expectedA,
          |    "b": $expectedB
          |  },
          | "additionalProperties": $expectedAP
          |}
        """.stripMargin)
      Widened(input1, input2) must_== expected
    }
    test("false", "false", """{"type": "number"}""", """{"type": "string"}""", "false") and
    test("false", "true", """{}""", """{"type": "string"}""", "true") and
    test("true", "false", """{"type": "number"}""", """{}""", "true") and
    test("true", "true", """{}""", """{}""", "true") and
    test("""{"type": "string"}""", "false", """{"type": "number"}""", """{"type": "string"}""", """{"type": "string"}""") and
    test("""{"type": "number"}""", "false", """{"type": "number"}""", """{"type": ["string", "number"]}""", """{"type": "number"}""")
  }

  def object3 = {
    def test(ap1: String, ap2: String, expectedA: String, expectedB: String, expectedAP: String) = {
      val input1 = SpecHelpers.parseSchema(
        s"""
          |{
          | "type": "object",
          | "patternProperties": {"a": {"type": "number"}},
          | "additionalProperties": $ap1
          |}
        """.stripMargin)

      val input2 = SpecHelpers.parseSchema(
        s"""
          |{
          | "type": "object",
          | "patternProperties": {"b": {"type": "string"}},
          | "additionalProperties": $ap2
          |}
        """.stripMargin)

      val expected = SpecHelpers.parseSchema(
        s"""
          |{
          |"type": "object",
          | "patternProperties": {
          |    "a": $expectedA,
          |    "b": $expectedB
          |  },
          | "additionalProperties": $expectedAP
          |}
        """.stripMargin)
      Widened(input1, input2) must_== expected
    }
    test("false", "false", """{"type": "number"}""", """{"type": "string"}""", "false") and
    test("false", "true", """{}""", """{"type": "string"}""", "true") and
    test("true", "false", """{"type": "number"}""", """{}""", "true") and
    test("true", "true", """{}""", """{}""", "true") and
    test("""{"type": "string"}""", "false", """{"type": "number"}""", """{"type": "string"}""", """{"type": "string"}""") and
    test("""{"type": "number"}""", "false", """{"type": "number"}""", """{"type": ["string", "number"]}""", """{"type": "number"}""")
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
      Widened(input1, input2) must_== expected
    }
    test("""["a"]""", """["a"]""", """["a"]""") and
    test("""["a"]""", """["b"]""", """[]""") and
    test("""["b"]""", """["a"]""", """[]""") and
    test("""["b"]""", """[]""", """[]""") and
    test("""["b"]""", "null", "null")
  }

  def array1 = {
    val input1 = SpecHelpers.parseSchema(
      """
        |{
        | "type": "array",
        | "items": {"type": "string"}
        |}
      """.stripMargin)

    val input2 = SpecHelpers.parseSchema(
      """
        |{
        | "type": "array",
        | "items": {"type": "number"}
        |}
      """.stripMargin)

    val expected = SpecHelpers.parseSchema(
      """
        |{
        |"type": "array",
        | "items": {"type": ["string", "number"]}
        |}
      """.stripMargin)
    Widened(input1, input2) must_== expected
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
    Widened(input1, input2) must_== expected
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
    Widened(input1, input2) must_== expected
  }

}
