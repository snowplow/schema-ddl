/*
 * Copyright (c) 2014-2022 Snowplow Analytics Ltd. All rights reserved.
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

import scala.jdk.CollectionConverters._

import cats.data.{Validated, ValidatedNel, NonEmptyList}
import cats.syntax.validated._

import com.fasterxml.jackson.databind.ObjectMapper
import com.networknt.schema.{SpecVersion, JsonSchema, JsonSchemaFactory, SchemaValidatorsConfig}

import io.circe.jackson.circeToJackson

import com.snowplowanalytics.iglu.core.SelfDescribingSchema.SelfDescribingUri
import com.snowplowanalytics.iglu.core.circe.MetaSchemas

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Linter.Level

// circe
import io.circe.Json

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Linter.Message

/**
 * Linting self-describing schemas against their meta-schemas
 * Useful mostly for identifying user-defined properties at a wrong level
 * and unexpected validation types, i.e. `maxLength: "foo"`
 */
object SelfSyntaxChecker {

  private val MetaSchemaUri = SelfDescribingUri.toString

  private val V4SchemaText =
    """{
      |"definitions":{
      |  "schemaArray":{"type":"array","minItems":1,"items":{"$ref":"#"}},
      |  "positiveInteger":{"type":"integer","minimum":0},
      |  "positiveIntegerDefault0":{"allOf":[{"$ref":"#/definitions/positiveInteger"},{"default":0}]},
      |  "simpleTypes":{"enum":["array","boolean","integer","null","number","object","string"]},
      |  "stringArray":{"type":"array","items":{"type":"string"},"minItems":1,"uniqueItems":true}
      |},
      |"type":"object",
      |"properties":{
      |  "self":{
      |    "required": ["vendor", "name", "format", "version"],
      |    "properties":{
      |      "vendor": {"type":"string"},
      |      "name": {"type":"string"},
      |      "format": {"type":"string"},
      |      "version": {"type":"string"}
      |    },
      |    "additionalProperties": false
      |  },
      |  "id":{"type":"string"},
      |  "$schema":{"type":"string"},
      |  "title":{"type":"string"},
      |  "description":{"type":"string"},
      |  "default":{},
      |  "multipleOf":{"type":"number","minimum":0},
      |  "maximum":{"type":"number"},
      |  "minimum":{"type":"number"},
      |  "maxLength":{"$ref":"#/definitions/positiveInteger"},
      |  "minLength":{"$ref":"#/definitions/positiveIntegerDefault0"},
      |  "pattern":{"type":"string","format":"regex"},
      |  "additionalItems":{"anyOf":[{"type":"boolean"},{"$ref":"#"}],"default":{}},
      |  "items":{"anyOf":[{"$ref":"#"},{"$ref":"#/definitions/schemaArray"}],"default":{}},
      |  "maxItems":{"$ref":"#/definitions/positiveInteger"},
      |  "minItems":{"$ref":"#/definitions/positiveIntegerDefault0"},
      |  "uniqueItems":{"type":"boolean","default":false},
      |  "maxProperties":{"$ref":"#/definitions/positiveInteger"},
      |  "minProperties":{"$ref":"#/definitions/positiveIntegerDefault0"},
      |  "required":{"$ref":"#/definitions/stringArray"},
      |  "additionalProperties":{"anyOf":[{"type":"boolean"},{"$ref":"#"}],"default":{}},
      |  "definitions":{"type":"object","additionalProperties":{"$ref":"#"},"default":{}},
      |  "properties":{"type":"object","additionalProperties":{"$ref":"#"},"default":{}},
      |  "patternProperties":{"type":"object","additionalProperties":{"$ref":"#"},"default":{}},
      |  "enum":{"type":"array","minItems":1,"uniqueItems":true},
      |  "type":{"anyOf":[{"$ref":"#/definitions/simpleTypes"},{"type":"array","items":{"$ref":"#/definitions/simpleTypes"},"minItems":1,"uniqueItems":true}]},
      |  "format":{"type":"string"},
      |  "allOf":{"$ref":"#/definitions/schemaArray"},
      |  "anyOf":{"$ref":"#/definitions/schemaArray"},
      |  "oneOf":{"$ref":"#/definitions/schemaArray"},
      |  "not":{"$ref":"#"}
      |},
      |"additionalProperties": false,
      |"default":{}}""".stripMargin

  private val SelfSchemaText = {
    val vendorRegex = "^[a-zA-Z0-9-_.]+$"
    val nameRegex = "^[a-zA-Z0-9-_]+$"
    val versionRegex = "^[1-9][0-9]*(-(0|[1-9][0-9]*)){2}$"
    s"""{
      | "type":"object",
      | "properties":{
      |   "$$schema":{
      |     "type":"string",
      |     "enum": ["$MetaSchemaUri"]
      |   },
      |   "self":{
      |     "required": ["vendor", "name", "format", "version"],
      |     "properties":{
      |       "vendor": {
      |         "type":"string",
      |         "pattern": "$vendorRegex"
      |       },
      |       "name": {
      |         "type":"string",
      |         "pattern": "$nameRegex"
      |       },
      |       "format": {
      |         "type":"string",
      |         "pattern": "$nameRegex"
      |       },
      |       "version": {
      |         "type":"string",
      |         "pattern": "$versionRegex"
      |       }
      |     }
      |   }
      | },
      | "additionalProperties": true
      |}|""".stripMargin
  }

  private val V4SchemaInstance: JsonSchemaFactory =
    JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V4)

  val SchemaValidatorsConfig: SchemaValidatorsConfig = {
    val config = new SchemaValidatorsConfig()
    // typeLoose is OpenAPI workaround to cast stringly typed properties
    // e.g, with default true "5" string would validate against integer type
    config.setTypeLoose(false)
    config
  }

  /** An excessively strict Json Schema
   *
   *  In particular, this version disallows additionalProperties. It is helpful for raising warnings when a schema contains typos.
   */
  private lazy val V4SchemaStrict: JsonSchema =
    JsonSchemaFactory
      .builder(V4SchemaInstance)
      .build()
      .getSchema(new ObjectMapper().readTree(V4SchemaText))

  /** The more permissive JsonSchema allowed by Iglu Core
   *
   *  Schemas valid against this definition are generally considered valid throughout the Iglu System.
   */
  private lazy val V4SchemaIgluCore: JsonSchema =
    JsonSchemaFactory
      .builder(V4SchemaInstance)
      .build()
      .getSchema(new ObjectMapper().readTree(MetaSchemas.JsonSchemaV4Text))

  /** A Json Schema which validates Iglu's `$schema` and `self` properties
   *
   *  A schema MUST validate against this schema in order to be generally valid throughout the Iglu System
   */
  private lazy val V4SchemaSelfSyntax: JsonSchema =
    JsonSchemaFactory
      .builder(V4SchemaInstance)
      .build()
      .getSchema(new ObjectMapper().readTree(SelfSchemaText))

  def validateSchema(schema: Json): ValidatedNel[Message, Unit] = {
    val jacksonJson = circeToJackson(schema)
    val laxValidation = V4SchemaIgluCore
      .validate(jacksonJson)
      .asScala
      .map(_ -> Linter.Level.Error) // It is an error to fail validation against v4 spec
      .toMap
    val selfValidation = V4SchemaSelfSyntax
      .validate(jacksonJson)
      .asScala
      .map(_ -> Linter.Level.Error) // It is an error to fail validation of Iglu's `$schema` and `self` properties
      .toMap
    val strictValidation = V4SchemaStrict
      .validate(jacksonJson)
      .asScala
      .map(_ -> Linter.Level.Warning) // It is a warning to fail the strict validation
      .toMap

    (strictValidation ++ laxValidation ++ selfValidation) // Order is important: Errors override Warnings for identical messages
      .toList
      .map { case (message, level) =>
        val pointer = JsonPath.parse(message.getPath).map(JsonPath.toPointer) match {
          case Right(Right(value)) => value
          case Right(Left(inComplete)) => inComplete
          case Left(_) => Pointer.Root
        }
        Message(pointer, message.getMessage, level)
      }.valid.swap match {
      case Validated.Invalid(Nil) =>
        ().validNel
      case Validated.Invalid(h :: t) =>
        NonEmptyList(h, t).invalid
      case Validated.Valid(_) =>
        ().validNel
    }
  }

  /**
   * Validates that a self-describing JSON contains the correct schema keyword.
   *
   * A previous JSON Schema validator was not able to recognize $$schema keyword, so this was implemented as a workaround.
   * This method will be removed in future versions becuase `SelfSyntaxCheck.validateSchema` encompasses this check.
   *
   * @param schema JSON node with a schema
   * @return linting result
   */
  @deprecated("Use `SelfSyntaxChecker.validateSchema`", "0.17.0")
  def checkMetaSchema(schema: Json): ValidatedNel[Message, Unit] =
    schema.asObject.map(_.toMap) match {
      case None =>
        Message(Pointer.Root, "JSON Schema must be a JSON object", Level.Error).invalidNel
      case Some(obj) =>
        obj.get("self") match {
          case None =>
            ().validNel // Don't care if schema is not self-describing
          case Some(_) =>
            obj.get(s"$$schema") match {
              case Some(schema) if schema.asString.contains(MetaSchemaUri) =>
                ().validNel
              case Some(schema) =>
                Message(Pointer.Root, s"self is unknown keyword for a $$schema ${schema.noSpaces}, use $MetaSchemaUri", Level.Error).invalidNel
              case None =>
                Message(Pointer.Root, s"self is unknown keyword for vanilla $$schema, use $MetaSchemaUri", Level.Error).invalidNel
            }

        }
    }
}
