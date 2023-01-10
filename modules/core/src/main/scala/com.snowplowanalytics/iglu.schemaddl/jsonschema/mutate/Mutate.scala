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
package com.snowplowanalytics.iglu.schemaddl.jsonschema.mutate

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties._

private[schemaddl] object Mutate {

  /**
  * Mutates a schema to be more amenable for converting into a storage AST.
  *
  * Some JsonSchema properties are helpful for strict validation, but have no meaning when storing
  * the data in a strictly typed storage warehouse. It is helpful to remove these jsonschema features:
  * - oneOf / anyOf properties
  * - additionalProperties of an object
  * - an array represented as a tuple of different types
  * 
  * _Most_ data that validates against the input schema will also validate against the output schema.
  * There is one _edge case_ where data that validates the input schema does not validate against the
  * output schema.
  *
  * **Cautionary example 1**
  *
  * In the following example, the validData is technically valid against the input schema, because of
  * the first sub-schema in the `oneOf` array. But it is invalid agains the output schema, because
  * the `b` field is a string.
  *
  * ```
  * inputSchema = {
  *   "type": "object",
  *   "oneOf": [
  *     {"properties": {"a": {"type": "string"}}},
  *     {"properties": {"b": {"type": "number"}}}
  *   ]
  * }
  *
  * outputSchema = {
  *   "type": "object",
  *   "properties": {
  *     "a": {"type": "string"},
  *     "b": {"type": "number"}
  *   },
  *   "additionalProperties": false
  * }
  *
  * validData = {"a": "ok", "b": "surprising"}
  * ```
  *
  * **Cautionary example 2**
  *
  * In the following example, the validData is technically valid against the input schema because
  * extra array items are allowed to be any type.  It is invalid against the output schema because
  * the 3rd item in the array is not a string.
  *
  * ```
  * inputSchema = {
  *   "type": "array",
  *   "items": [
  *     {"type": "string"},
  *     {"type": "string"}
  *   ],
  *   "additionalItems": true
  * }
  *
  * outputSchema = {
  *   "type": "array",
  *   "items": {"type": "string"},
  * }
  *
  * validData = ["x", "y", 42.0]
  * ```
  *
  * Nonetheless, it is helpful for storage warehouse ASTs to be based on this mutated output schema.
  * Storage loaders using the ASTs should be aware that not all data can be cast to the output schema.
   */
  def forStorage(schema: Schema): Schema =
    (noTupleItems(_))
      .andThen(noAdditionalProperties(_))
      .andThen(noAlternatives(_))
      .apply(schema)

  /**
   * Alters a schema to be expressed in terms of union types instead of oneOf/anyOf alternatives.
   *
   * Data that validates against the output schema will also validate against the input schema.
   * Conversely, data that validates against the input schema will not necessarily validate against
   * the output schema.
   *
   * ```
   * inputSchema = {
   *   "oneOf": [
   *     {"type": "string"},
   *     {"type": "number"},
   *   ]
   * }
   *
   * outputSchema = {
   *   "type": ["string", "number"]
   * }
   * ```
   */
  private def noAlternatives(schema: Schema): Schema = {
    val items = schema.items.map {
        case ArrayProperty.Items.ListItems(li) => ArrayProperty.Items.ListItems(noAlternatives(li))
        case ArrayProperty.Items.TupleItems(ti) => ArrayProperty.Items.TupleItems(ti.map(noAlternatives))
      }

    val properties = schema.properties.map { properties =>
        properties.value.map { case (k, v) => k -> noAlternatives(v) }
      }.map(ObjectProperty.Properties(_))

    val patternProperties = schema.patternProperties.map {pp =>
      pp.value.map { case (k, v) => k -> noAlternatives(v) }
    }.map(ObjectProperty.PatternProperties(_))

    val additionalProperties = schema.additionalProperties.map {
      case ObjectProperty.AdditionalProperties.AdditionalPropertiesSchema(aps) =>
        ObjectProperty.AdditionalProperties.AdditionalPropertiesSchema(noAlternatives(aps))
      case other => other
    }
    val additionalItems = schema.additionalItems.map {
      case ArrayProperty.AdditionalItems.AdditionalItemsSchema(ais) =>
        ArrayProperty.AdditionalItems.AdditionalItemsSchema(noAlternatives(ais))
      case other => other
    }

    val withoutAlts = schema.copy(
      items = items,
      properties = properties,
      patternProperties = patternProperties,
      additionalProperties = additionalProperties,
      additionalItems = additionalItems,
      oneOf = None,
      anyOf = None
    )

    val alternatives = schema.oneOf.toList.flatMap(_.value) ++ schema.anyOf.toList.flatMap(_.value)

    alternatives
      .map(noAlternatives)
      .map(Narrowed(withoutAlts, _))
      .reduceOption(Widened(_, _))
      .getOrElse(withoutAlts)
  }

  /**
   * Alters a schema to collapse array items if they are expressed as a tuples.
   *
   * ```
   * inputSchema = {
   *   "type": "array",
   *   "items": [{"type": "string"}, {"type": "number"}],
   * }
   *
   * outputSchema = {
   *   "type": "array",
   *   "items": {"type": ["string", "number"]}
   * }
   * ```
   *
   * It is good to call this mutation before calling the [[noAlternatives]] mutation. The latter
   * can be more aggressive in merging alternatives if objects don't allow tuple items or additional items.
   *
   * **Warning**: some data that validates against the input schema _might_ not validate against the
   * output schema. For example, using the example schemas above, the data `["hello", 42, true]`
   * is technically valid against `inputSchema` but invalid against `outputSchema`.
   */
  private def noTupleItems(schema: Schema): Schema = {
    val items = schema.items.flatMap {
      case ArrayProperty.Items.ListItems(li) =>
        Some(ArrayProperty.Items.ListItems(noTupleItems(li)))
      case ArrayProperty.Items.TupleItems(ti) =>
        schema.additionalItems match {
          case Some(ArrayProperty.AdditionalItems.AdditionalItemsSchema(ais)) =>
            Some(ArrayProperty.Items.ListItems {
              ti
                .map(noTupleItems)
                .foldLeft(ais)(Widened(_, _))
            })
          case _ =>
            ti
              .map(noTupleItems)
              .reduceOption(Widened(_, _))
              .map(ArrayProperty.Items.ListItems(_))
        }
    }
    val additionalProperties = schema.additionalProperties.map {
      case ObjectProperty.AdditionalProperties.AdditionalPropertiesSchema(aps) =>
        ObjectProperty.AdditionalProperties.AdditionalPropertiesSchema(noTupleItems(aps))
      case other => other
    }
    val patternProperties = schema.patternProperties.map { pps =>
      pps.value.map {
        case (k, s) => (k, noTupleItems(s))
      }
    }.map(ObjectProperty.PatternProperties(_))
    
    val properties = schema.properties.map { properties =>
      properties.value.map {
        case (k, v) => k -> noTupleItems(v)
      }
    }.map(ObjectProperty.Properties(_))

    schema.copy(
      items = items,
      properties = properties,
      additionalProperties = additionalProperties,
      additionalItems = None,
      patternProperties = patternProperties,
      oneOf = schema.oneOf.map(oneOf => CommonProperties.OneOf(oneOf.value.map(noTupleItems))),
      anyOf = schema.anyOf.map(anyOf => CommonProperties.AnyOf(anyOf.value.map(noTupleItems)))
    )
  }

  /**
   * Alters a schema to explicitly disallow additionalProperties of an object.
   *
   * ```
   * inputSchema = {
   *   "type": "object",
   *   "properties": {"a": {"type": "string"}}
   * }
   *
   * outputSchema = {
   *   "type": "object",
   *   "properties": {"a": {"type": "string"}},
   *   "additionalProperties": false
   * }
   * ```
   *
   * It is good to call this mutation before calling the [[noAlternatives]] mutation. The latter
   * can be more aggressive in merging alternatives if objects don't allow additional properties.
   *
   * **Warning**: some data that validates against the input schema _might_ not validate against the
   * output schema. For example, using the example schemas above, the data `{"b": true}` is
   * technically valid against `inputSchema` but invalid against `outputSchema`.
   *
   */
  private def noAdditionalProperties(schema: Schema): Schema = {
    val items = schema.items.map {
      case ArrayProperty.Items.ListItems(li) =>
        ArrayProperty.Items.ListItems(noAdditionalProperties(li))
      case ArrayProperty.Items.TupleItems(ti) =>
        ArrayProperty.Items.TupleItems(ti.map(noAdditionalProperties))
    }
    val additionalItems = schema.additionalItems.map {
      case ArrayProperty.AdditionalItems.AdditionalItemsAllowed(ail) =>
        ArrayProperty.AdditionalItems.AdditionalItemsAllowed(ail)
      case ArrayProperty.AdditionalItems.AdditionalItemsSchema(ais) =>
        ArrayProperty.AdditionalItems.AdditionalItemsSchema(noAdditionalProperties(ais))
    }
    val properties = schema.properties.map { properties =>
      properties.value.map {
        case (k, v) => k -> noAdditionalProperties(v)
      }
    }.map(ObjectProperty.Properties(_))

    val additionalProperties = schema.`type`.map(_.asUnion.value) match {
      case Some(set) if set.contains(CommonProperties.Type.Object) =>
        Some(ObjectProperty.AdditionalProperties.AdditionalPropertiesAllowed(false))
      case _ => None
    }

    schema.copy(
      items = items,
      properties = properties,
      additionalProperties = additionalProperties,
      patternProperties = None,
      additionalItems = additionalItems,
      oneOf = schema.oneOf.map(oneOf => CommonProperties.OneOf(oneOf.value.map(noAdditionalProperties))),
      anyOf = schema.anyOf.map(anyOf => CommonProperties.AnyOf(anyOf.value.map(noAdditionalProperties)))
    )
  }

}
