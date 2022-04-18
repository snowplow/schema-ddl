/*
 * Copyright (c) 2021-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.iglu.schemaddl.parquet

import com.snowplowanalytics.iglu.schemaddl.StringUtils
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.{CommonProperties,ArrayProperty}

/**
  * Type-safe AST proxy to `org.apache.spark.types.StructField` schema type
  * Up to client's code to convert it
  */
case class Field(name: String, fieldType: Type, nullable: Boolean) {

  def normalName: String =
    StringUtils.snakeCase
      .andThen(Field.replaceDisallowedCharacters)(name)

  def normalized: Field = fieldType match {
    case Type.Struct(fields) => Field(normalName, Type.Struct(fields.map(_.normalized)), nullable)
    case other => Field(normalName, other, nullable)
  }
}

object Field {
  def build(name: String, topSchema: Schema, required: Boolean): Field = {
    topSchema.`type` match {
      case Some(types) if types.possiblyWithNull(CommonProperties.Type.Object) =>
        val subfields = topSchema.properties.map(_.value).getOrElse(Map.empty)
        if (subfields.isEmpty) {
          Suggestion.finalSuggestion(topSchema, required)(name)
        } else {
          val requiredKeys = topSchema.required.toList.flatMap(_.value)
          val structFields = subfields.toList.map { case (key, schema) =>
            build(key, schema, requiredKeys.contains(key))
          }.sortBy(_.name)
          Field(name, Type.Struct(structFields), !required || types.nullable)
        }
      case Some(types) if types.possiblyWithNull(CommonProperties.Type.Array) =>
        topSchema.items match {
          case Some(ArrayProperty.Items.ListItems(schema)) =>
            val inner = build("", schema, true)
            Field(name, Type.Array(inner.fieldType, inner.nullable), !required || types.nullable)
          case _ =>
            Field(name, Type.Array(Type.String, true), !required || types.nullable)
        }
      case _ =>
        Suggestion.suggestions
          .find(suggestion => suggestion(topSchema, required).isDefined)
          .flatMap(_.apply(topSchema, required))
          .getOrElse(Suggestion.finalSuggestion(topSchema, required))
          .apply(name)
    }
  }

  /**
   * Replaces disallowed parquet column characters with underscore
   */
  val replaceDisallowedCharacters: String => String = str =>
    str.replaceAll("[^A-Za-z0-9_]", "_")
}
