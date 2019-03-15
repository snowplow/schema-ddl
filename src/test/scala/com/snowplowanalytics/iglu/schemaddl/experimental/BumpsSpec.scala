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
package com.snowplowanalytics.iglu.schemaddl.experimental

import com.snowplowanalytics.iglu.core.VersionKind
import com.snowplowanalytics.iglu.schemaddl.SpecHelpers._
import com.snowplowanalytics.iglu.schemaddl.migrations.SchemaDiff

import io.circe.literal._
import org.specs2.mutable.Specification


class BumpsSpec extends Specification {
  "required" should {
    "recognize added required property" >> {
      val diff = SchemaDiff.empty.copy(added = List(
        "/properties/foo".jsonPointer -> json"""{"type": "string"}""".schema
      ))

      Bumps.required(diff) must beTrue
    }

    "skip added optional (via enum) property" >> {
      val diff = SchemaDiff.empty.copy(added = List(
        "/properties/foo".jsonPointer -> json"""{"type": "string", "enum": ["foo", null]}""".schema
      ))

      Bumps.required(diff) must beFalse
    }

    "recognize changed property being made required" >> {
      val modified = SchemaDiff.Modified(
        "/properties/foo".jsonPointer,
        json"""{"type": "string", "enum": ["foo", null]}""".schema,
        json"""{"type": "string"}""".schema)

      val diff = SchemaDiff.empty.copy(modified = Set(modified))

      Bumps.required(diff) must beTrue
    }

    "skip changed optional -> optional property" >> {
      val modified = SchemaDiff.Modified(
        "/properties/foo".jsonPointer,
        json"""{"type": "string"}""".schema,
        json"""{"type": ["string", "integer"]}""".schema)

      val diff = SchemaDiff.empty.copy(modified = Set(modified))

      Bumps.required(diff) must beFalse
    }
  }

  "getPointer" should {
    "identify type widening as a revision" >> {
      val modified = SchemaDiff.Modified(
        "/properties/foo".jsonPointer,
        json"""{"type": "string"}""".schema,
        json"""{"type": ["string", "integer"]}""".schema)

      val diff = SchemaDiff.empty.copy(modified = Set(modified))

      Bumps.getPointer(diff) must beSome(VersionKind.Revision)
    }

    "identify constraint change as a revision" >> {
      val modified = SchemaDiff.Modified(
        "/properties/foo".jsonPointer,
        json"""{"type": "string", "maxLength": 10}""".schema,
        json"""{"type": "string", "maxLength": 12}""".schema)

      val diff = SchemaDiff.empty.copy(modified = Set(modified))

      Bumps.getPointer(diff) must beSome(VersionKind.Revision)
    }

    "identify added optional property AND constraint change as a revision" >> {
      val addedProps = "/properties/bar".jsonPointer -> json"""{"type": ["string", "null"]}""".schema

      val modified = SchemaDiff.Modified(
        "/properties/foo".jsonPointer,
        json"""{"type": "string", "maxLength": 10}""".schema,
        json"""{"type": "string", "maxLength": 12}""".schema)

      val diff = SchemaDiff.empty.copy(added = List(addedProps), modified = Set(modified))

      Bumps.getPointer(diff) must beSome(VersionKind.Revision)
    }

    "identify added optional property as an addition" >> {
      val addedProps = "/properties/bar".jsonPointer -> json"""{"type": ["string", "null"]}""".schema
      val diff = SchemaDiff.empty.copy(added = List(addedProps))

      Bumps.getPointer(diff) must beSome(VersionKind.Addition)
    }
  }
}
