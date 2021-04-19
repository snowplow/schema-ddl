/*
 * Copyright (c) 2014-2021 Snowplow Analytics Ltd. All rights reserved.
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
import cats.syntax.apply._
import cats.syntax.validated._

import com.fasterxml.jackson.databind.ObjectMapper
import com.networknt.schema.{SpecVersion, JsonSchema, JsonSchemaFactory, SchemaValidatorsConfig}

import io.circe.jackson.circeToJackson

import com.snowplowanalytics.iglu.core.SelfDescribingSchema.SelfDescribingUri

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Linter.Level

// circe
import io.circe.Json

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Linter.Message

/**
 * Linting self-describing schemas gainst their meta-schemas
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
      |    }
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

  private val V4SchemaInstance: JsonSchemaFactory =
    JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V4)

  val SchemaValidatorsConfig: SchemaValidatorsConfig = {
    val config = new SchemaValidatorsConfig()
    // typeLoose is OpenAPI workaround to cast stringly typed properties
    // e.g, with default true "5" string would validate against integer type
    config.setTypeLoose(false)
    config
  }

  private lazy val V4Schema: JsonSchema =
    JsonSchemaFactory
      .builder(V4SchemaInstance)
      .build()
      .getSchema(new ObjectMapper().readTree(V4SchemaText))

  def validateSchema(schema: Json): ValidatedNel[Message, Unit] = {
    val jacksonJson = circeToJackson(schema)
    val validation = V4Schema
      .validate(jacksonJson)
      .asScala
      .toList
      .map { message =>
        val pointer = JsonPath.parse(message.getPath).map(JsonPath.toPointer) match {
          case Right(Right(value)) => value
          case Right(Left(inComplete)) => inComplete
          case Left(_) => Pointer.Root
        }
        Message(pointer, message.getMessage, Linter.Level.Warning)
      }.valid.swap match {
      case Validated.Invalid(Nil) =>
        ().validNel
      case Validated.Invalid(h :: t) =>
        NonEmptyList(h, t).invalid
      case Validated.Valid(_) =>
        ().validNel
    }

    checkMetaSchema(schema) *> validation
  }

  /**
   * Previous JSON Schema validator was able to recognize $$schema keyword,
   * now we have to implement this check manually
   *
   * @param schema JSON node with a schema
   * @return linting result
   */
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
