/*
 * Copyright (c) 2021-2023 Snowplow Analytics Ltd. All rights reserved.
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

import cats.data.NonEmptyList
import cats.implicits._

import com.snowplowanalytics.iglu.schemaddl.StringUtils
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.{ArrayProperty, CommonProperties, ObjectProperty}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.mutate.Mutate

case class Field(name: String,
                 fieldType: Type,
                 nullability: Type.Nullability,
                 accessors: Set[String])

object Field {

  def apply(name: String,
            fieldType: Type,
            nullability: Type.Nullability): Field = Field(name, fieldType, nullability, Set(name))

  def build(name: String, topSchema: Schema, enforceValuePresence: Boolean): Option[Field] =
    buildType(Mutate.forStorage(topSchema)).map { constructedType =>
      val nullability = isFieldNullable(constructedType.nullability, enforceValuePresence)
      Field(name, constructedType.value, nullability)
    }

  def buildRepeated(name: String, itemSchema: Schema, enforceItemPresence: Boolean, nullability: Type.Nullability): Option[Field] =
    buildType(Mutate.forStorage(itemSchema)).map { constructedType =>
      val itemNullability = isFieldNullable(constructedType.nullability, enforceItemPresence)
      Field(name, Type.Array(constructedType.value, itemNullability), nullability)
    }

  def normalize(field: Field): Field = {
    val fieldType = field.fieldType match {
      case Type.Struct(fields) => Type.Struct(collapseDuplicateFields(fields.map(normalize)))
      case Type.Array(Type.Struct(fields), nullability) => Type.Array(
        Type.Struct(collapseDuplicateFields(fields.map(normalize)))
        , nullability)
      case other => other
    }
    field.copy(name = normalizeName(field), fieldType = fieldType)
  }

  private def collapseDuplicateFields(normFields: NonEmptyList[Field]): NonEmptyList[Field] = {
    val endMap = normFields
      .groupBy(_.name)
      .map { case (key, fs) =>
        // Use `min` to deterministically pick the same accessor each time when choosing the type
        val lowest = fs.minimumBy(f => f.accessors.min)
        (key, lowest.copy(accessors = fs.iterator.flatMap(_.accessors).toSet))
      }
    normFields
      .map(_.name)
      .distinct
      .map(endMap(_))
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
    case object NoExplicitNull extends JsonNullability

    def extractFrom(`type`: CommonProperties.Type): JsonNullability = {
      if (`type`.nullable) {
        JsonNullability.ExplicitlyNullable
      } else
        JsonNullability.NoExplicitNull
    }
  }

  private def buildType(topSchema: Schema): Option[NullableType] = {
    topSchema.`type` match {
      case Some(types) if types.possiblyWithNull(CommonProperties.Type.Object) =>
        buildObjectType(topSchema).map { objectType =>
          NullableType(
            value = objectType,
            nullability = JsonNullability.extractFrom(types)
          )
        }

      case Some(types) if types.possiblyWithNull(CommonProperties.Type.Array) =>
        buildArrayType(topSchema).map { arrayType =>
          NullableType( 
            value = arrayType,
            nullability = JsonNullability.extractFrom(types)
          )
        }

      case _ =>
        provideSuggestions(topSchema) match {
          case Some(matchingSuggestion) => Some(matchingSuggestion)
          case None => Some(jsonType(topSchema))
        }
    }
  }

  private def buildObjectType(topSchema: Schema): Option[Type] = {
    val requiredKeys = topSchema.required.toList.flatMap(_.value)
    val subfields = topSchema.properties
      .iterator
      .flatMap(_.value.toList)
      .flatMap { case (key, schema) =>
        buildType(schema).map(key -> _)
      }.map { case (key, constructedType) =>
        val isSubfieldRequired = requiredKeys.contains(key)
        val nullability = isFieldNullable(constructedType.nullability, isSubfieldRequired)
        Field(key, constructedType.value, nullability)
      }
      .toList
      .sortBy(_.name)

    NonEmptyList.fromList(subfields) match {
      case Some(nel) =>
        Some(Type.Struct(nel))
      case None =>
        topSchema.additionalProperties match {
          case Some(ObjectProperty.AdditionalProperties.AdditionalPropertiesAllowed(false)) =>
            None
          case _ =>
            Some(Type.Json)
        }
    }
  }

  private def buildArrayType(topSchema: Schema): Option[Type.Array] = {
    topSchema.items match {
      case Some(ArrayProperty.Items.ListItems(schema)) =>
        buildType(schema).map { typeOfArrayItem =>
          val nullability = isFieldNullable(typeOfArrayItem.nullability, true)
          Type.Array(typeOfArrayItem.value, nullability)
        }
      case _ =>
        Some(Type.Array(Type.Json, Type.Nullability.Nullable))
    }
  }

  private def isFieldNullable(jsonNullability: JsonNullability, enforceItemPresence: Boolean): Type.Nullability =
    jsonNullability match {
      case JsonNullability.ExplicitlyNullable =>
        Type.Nullability.Nullable
      case JsonNullability.NoExplicitNull if enforceItemPresence =>
        Type.Nullability.Required
      case JsonNullability.NoExplicitNull =>
        Type.Nullability.Nullable
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
