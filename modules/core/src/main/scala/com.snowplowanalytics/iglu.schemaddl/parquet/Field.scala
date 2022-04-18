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
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.{ArrayProperty, CommonProperties}

case class Field(name: String, 
                 fieldType: Type, 
                 nullable: Type.Nullability)

object Field {

  def build(name: String, topSchema: Schema, enforceValuePresence: Boolean): Field = {
    val constructedType = buildType(topSchema)

    val isFieldNullable = constructedType.nullability match {
      case JsonNullability.ExplicitlyNullable =>
        Type.Nullability.Optional
      case JsonNullability.NoNull if enforceValuePresence =>
        Type.Nullability.Required
      case JsonNullability.NoNull =>
        Type.Nullability.Optional
    }

    Field(name, constructedType.value, isFieldNullable)
  }

  def normalize(field: Field): Field = {
    val fieldType = field.fieldType match {
      case Type.Struct(fields) => Type.Struct(fields.map(normalize))
      case other => other
    }
    field.copy(name = normalizeName(field), fieldType = fieldType)
  }

  private[parquet] def normalizeName(field: Field): String =
    StringUtils.snakeCase
      .andThen(replaceDisallowedCharacters)(field.name)

  /**
   * Replaces disallowed parquet column characters with underscore
   */
  private val replaceDisallowedCharacters: String => String = str =>
    str.replaceAll("[^A-Za-z0-9_]", "_")

  private[parquet] final case class NullableType(value: Type, nullability: JsonNullability)

  /* An interim nullability type, which should not be public beyond schema-ddl.
   *
   * It represents whether the JsonSchema "type" field allows explicit nulls. Semantically it has a
   * different meaning to the parquet [[Type.Nullability]], which is also concerned with missing values.
   */
  private[parquet] sealed trait JsonNullability

  private[parquet] object JsonNullability {
    case object ExplicitlyNullable extends JsonNullability
    case object NoNull extends JsonNullability

    def extractFrom(`type`: CommonProperties.Type): JsonNullability = {
      if (`type`.nullable) {
        JsonNullability.ExplicitlyNullable
      } else
        JsonNullability.NoNull
    }
  }

  private[parquet] def buildType(topSchema: Schema): NullableType = {
    topSchema.`type` match {
      case Some(types) if types.possiblyWithNull(CommonProperties.Type.Object) =>
        NullableType(
          value = buildObject(topSchema), 
          nullability = JsonNullability.extractFrom(types)
        )

      case Some(types) if types.possiblyWithNull(CommonProperties.Type.Array) =>
        NullableType( 
          value = buildArray(topSchema), 
          nullability = JsonNullability.extractFrom(types)
        )

      case _ =>
        provideSuggestions(topSchema) match {
          case Some(matchingSuggestion) => matchingSuggestion
          case None => jsonType(topSchema)
        }
    }
  }

  private def buildObject(topSchema: Schema): Type = {
    val subfields = topSchema.properties.map(_.value).getOrElse(Map.empty)
    if (subfields.nonEmpty) {
      val requiredKeys = topSchema.required.toList.flatMap(_.value)
      val structFields = subfields
        .toList
        .map { case (key, schema) =>
          val isSubfieldRequired = requiredKeys.contains(key)
          Field.build(key, schema, isSubfieldRequired)
        }
        .sortBy(_.name)
      Type.Struct(structFields)
    } else {
      Type.Json
    }
  }

  private def buildArray(topSchema: Schema): Type.Array = {
    topSchema.items match {
      case Some(ArrayProperty.Items.ListItems(schema)) =>
        val typeOfArrayItem = buildType(schema)
        val nullability = typeOfArrayItem.nullability match {
          case JsonNullability.ExplicitlyNullable =>
            Type.Nullability.Optional
          case JsonNullability.NoNull =>
            Type.Nullability.Required
        }
        Type.Array(typeOfArrayItem.value, nullability)
      case _ =>
        Type.Array(Type.Json, Type.Nullability.Optional)
    }
  }

  private def provideSuggestions(topSchema: Schema): Option[NullableType] = {
    Suggestion.allSuggestions
      .find(suggestion => suggestion(topSchema).isDefined)
      .flatMap(_.apply(topSchema))
  }

  private def jsonType(schema: Schema): NullableType = {
    val nullability = schema.`type`.map(JsonNullability.extractFrom).getOrElse(JsonNullability.ExplicitlyNullable)
    NullableType(Type.Json, nullability)
  }
}
