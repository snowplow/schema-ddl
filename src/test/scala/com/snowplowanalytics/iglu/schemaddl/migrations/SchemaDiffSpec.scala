/*
 * Copyright (c) 2014-2019 Snowplow Analytics Ltd. All rights reserved.
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

import org.specs2.Specification

import com.snowplowanalytics.iglu.schemaddl.Core.VersionPoint
import com.snowplowanalytics.iglu.schemaddl.SpecHelpers._


class SchemaDiffSpec extends Specification { def is = s2"""
  required point check recognizes added required property $e1
  required point check skips added optional (via enum) property $e2
  required point check recognizes changed property being made required $e3
  required point check skips changed optional -> optional property $e4
  getPointer identifies type widening as a revision $e5
  getPointer identifies constraint change as a revision $e6
  getPointer identifies added optional property AND constraint change as a revision $e7
  getPointer identifies added optional property as an addition $e8
  """

  def e1 = {
    val diff = SchemaDiff.empty.copy(added = List(
      "/properties/foo".jsonPointer -> json"""{"type": "string"}""".schema
    ))

    SchemaDiff.required(diff) must beTrue
  }

  def e2 = {
    val diff = SchemaDiff.empty.copy(added = List(
      "/properties/foo".jsonPointer -> json"""{"type": "string", "enum": ["foo", null]}""".schema
    ))

    SchemaDiff.required(diff) must beFalse
  }


  def e3 = {
    val modified = SchemaDiff.Modified(
      "/properties/foo".jsonPointer,
      json"""{"type": "string", "enum": ["foo", null]}""".schema,
      json"""{"type": "string"}""".schema)

    val diff = SchemaDiff.empty.copy(modified = Set(modified))

    SchemaDiff.required(diff) must beTrue
  }

  def e4 = {
    val modified = SchemaDiff.Modified(
      "/properties/foo".jsonPointer,
      json"""{"type": "string"}""".schema,
      json"""{"type": ["string", "integer"]}""".schema)

    val diff = SchemaDiff.empty.copy(modified = Set(modified))

    SchemaDiff.required(diff) must beFalse
  }

  def e5 = {
    val modified = SchemaDiff.Modified(
      "/properties/foo".jsonPointer,
      json"""{"type": "string"}""".schema,
      json"""{"type": ["string", "integer"]}""".schema)

    val diff = SchemaDiff.empty.copy(modified = Set(modified))

    SchemaDiff.getPointer(diff) must beSome(VersionPoint.Revision)
  }

  def e6 = {
    val modified = SchemaDiff.Modified(
      "/properties/foo".jsonPointer,
      json"""{"type": "string", "maxLength": 10}""".schema,
      json"""{"type": "string", "maxLength": 12}""".schema)

    val diff = SchemaDiff.empty.copy(modified = Set(modified))

    SchemaDiff.getPointer(diff) must beSome(VersionPoint.Revision)
  }

  def e7 = {
    val addedProps = "/properties/bar".jsonPointer -> json"""{"type": ["string", "null"]}""".schema

    val modified = SchemaDiff.Modified(
      "/properties/foo".jsonPointer,
      json"""{"type": "string", "maxLength": 10}""".schema,
      json"""{"type": "string", "maxLength": 12}""".schema)

    val diff = SchemaDiff.empty.copy(added = List(addedProps), modified = Set(modified))

    SchemaDiff.getPointer(diff) must beSome(VersionPoint.Revision)
  }

  def e8 = {
    val addedProps = "/properties/bar".jsonPointer -> json"""{"type": ["string", "null"]}""".schema
    val diff = SchemaDiff.empty.copy(added = List(addedProps))

    SchemaDiff.getPointer(diff) must beSome(VersionPoint.Addition)
  }
}
