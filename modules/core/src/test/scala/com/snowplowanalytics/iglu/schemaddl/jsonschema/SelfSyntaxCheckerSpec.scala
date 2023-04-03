/*
 * Copyright (c) 2014-2023 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.iglu.schemaddl.jsonschema

import cats.data.NonEmptyList

import io.circe.literal._

import org.specs2.mutable.Specification
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Pointer.Cursor.{DownField, DownProperty}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Pointer.SchemaProperty.Properties
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Linter.Level.{Error, Warning}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Linter.Message

class SelfSyntaxCheckerSpec extends Specification {
  "validateSchema" should {
    "recognize invalid schema property" in {
      val jsonSchema =
        json"""{
        "$$schema" : "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
        "description": "Schema for an example event",
        "self": {
            "vendor": "com.snowplowanalytics",
            "name": "example_event",
            "format": "jsonschema",
            "version": "1-0-0"
        },
        "type": "object",
        "properties": {
            "example_field_1": {
                "type": "string",
                "description": "the example_field_1 means x",
                "maxLength": 128
            },
            "example_field_2": {
                "type": ["string", "null"],
                "description": "the example_field_2 means y",
                "maxLength": 128
            },
            "example_field_3": {
                "type": "array",
                "description": "the example_field_3 is a collection of user names",
                "users": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                            "maxLength": 128
                        }
                    },
                    "required": [
                        "id"
                    ],
                    "additionalProperties": false
                }
            }
        },
        "additionalProperties": false,
        "required": [
            "example_field_1",
            "example_field_3"
        ]
      }"""

      SelfSyntaxChecker.validateSchema(jsonSchema).toEither must beLeft.like {
        case NonEmptyList
          (Message(
            pointer,
            "$.properties.example_field_3.users: is not defined in the schema and the schema does not allow additional properties",
            Warning
          ),
          Nil
        ) if pointer.value === List(DownField("example_field_3"), DownProperty(Properties)) => ok
      }
    }

    "recognize invalid maxLength type" in {
      val jsonSchema =
        json"""{
        "$$schema" : "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
        "description": "Schema for an example event",
        "self": {
            "vendor": "com.snowplowanalytics",
            "name": "example_event",
            "format": "jsonschema",
            "version": "1-0-0"
        },
        "type": "object",
        "properties": {
          "invalidMaxLength": {
            "maxLength": "string"
          }
        }
      }"""

      SelfSyntaxChecker.validateSchema(jsonSchema).toEither must beLeft.like {
        case NonEmptyList(
          Message(
            pointer,
            "$.properties.invalidMaxLength.maxLength: string found, integer expected",
            Error
          ),
          Nil
        ) if pointer.value === List(DownField("maxLength"), DownField("invalidMaxLength"), DownProperty(Properties)) => ok
      }
    }

    "complain about unknown self if valid meta-schema is not specified" in {
      val jsonSchema =
        json"""{
          "$$schema" : "http://something.com/unexpected#",
          "self": {
              "vendor": "com.snowplowanalytics",
              "name": "example_event",
              "format": "jsonschema",
              "version": "1-0-0"
          },
          "type": "object",
          "properties": { }
        }"""

      SelfSyntaxChecker.validateSchema(jsonSchema).toEither must beLeft.like {
        case NonEmptyList
          (Message(
            pointer,
            msg,
            Error
          ),
          Nil
        ) =>
            (pointer.value must beEmpty) and
            (msg must beEqualTo("$.$schema: does not have a value in the enumeration [http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#]"))
      }
    }

    "disallow vendors and names with invalid characters" in {

      val pairs = List(
        ("com.snowplowanalytics", "example event"),
        ("com.snowplowanalytics", " example_event"),
        ("com.snowplowanalytics", "example_event "),
        ("com.snowplowanalytics", "example❤️❤️event"),
        ("com.snowplow analytics", "example_event"),
        ("com.snowplowanalytics ", "example_event"),
        (" com.snowplowanalytics", "example_event"),
        ("com.snowplow❤️❤️analytics", "example_event"),
      )

      pairs.map { case (vendor, name) =>

        val jsonSchema =
          json"""{
            "$$schema" : "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
            "self": {
                "vendor": $vendor,
                "name": $name,
                "format": "jsonschema",
                "version": "1-0-0"
            },
            "type": "object",
            "properties": { }
          }"""

        SelfSyntaxChecker.validateSchema(jsonSchema).toEither must beLeft.like {
          case NonEmptyList(Message(_, msg, Error), Nil) =>
            msg must contain("does not match the regex pattern")
        }

      }.reduce(_ and _)
    }

    "disallow invalid schema versions" in {

      val versions = List(
        "1-0-0 ",
        "0-0-0",
        "1.0.0",
        "1-01-0",
        "1-1",
        "1-1-1-1"
      )

      versions.map { version =>

        val jsonSchema =
          json"""{
            "$$schema" : "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
            "self": {
                "vendor": "com.example",
                "name": "myschema",
                "format": "jsonschema",
                "version": $version
            },
            "type": "object",
            "properties": { }
          }"""

        SelfSyntaxChecker.validateSchema(jsonSchema).toEither must beLeft.like {
          case NonEmptyList(Message(_, msg, Error), Nil) =>
            msg must contain("does not match the regex pattern")
        }

      }.reduce(_ and _)
    }

    "recognize invalid 'supersededBy' type" in {
      val jsonSchema =
        json"""{
        "$$schema" : "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
        "$$supersededBy": "1-0",
        "description": "Schema for an example event",
        "self": {
            "vendor": "com.snowplowanalytics",
            "name": "example_event",
            "format": "jsonschema",
            "version": "1-0-0"
        },
        "type": "object",
        "properties": { }
      }"""

      SelfSyntaxChecker.validateSchema(jsonSchema).toEither must beLeft.like {
        case NonEmptyList(Message(_, msg, Error), Nil) =>
          msg must contain("does not match the regex pattern")
      }
    }

    "recognize invalid 'supersedes' type" in {
      val jsonSchema =
        json"""{
        "$$schema" : "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
        "$$supersedes": ["1-0-1", "1-0"],
        "description": "Schema for an example event",
        "self": {
            "vendor": "com.snowplowanalytics",
            "name": "example_event",
            "format": "jsonschema",
            "version": "1-0-0"
        },
        "type": "object",
        "properties": { }
      }"""

      SelfSyntaxChecker.validateSchema(jsonSchema).toEither must beLeft.like {
        case NonEmptyList(Message(_, msg, Error), Nil) =>
          msg must contain("does not match the regex pattern")
      }
    }

    "allow valid 'supersedes' and 'supersededBy' fields" in {
      val jsonSchema =
        json"""{
        "$$schema" : "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
        "$$supersededBy": "1-0-4",
        "$$supersedes": ["1-0-1", "1-0-2"],
        "description": "Schema for an example event",
        "self": {
            "vendor": "com.snowplowanalytics",
            "name": "example_event",
            "format": "jsonschema",
            "version": "1-0-0"
        },
        "type": "object",
        "properties": { }
      }"""

      SelfSyntaxChecker.validateSchema(jsonSchema).toEither must beRight
    }
  }
}
