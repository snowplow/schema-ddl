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
package com.snowplowanalytics.iglu.schemaddl.migrations

import com.snowplowanalytics.iglu.core.SelfDescribingSchema

import com.snowplowanalytics.iglu.schemaddl._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.{Pointer, Schema}
import com.snowplowanalytics.iglu.schemaddl.migrations.SchemaList._

/**
  * This class represents differences between *two* Schemas. Preserves no order.
  * The essence of [[Migration]]
  *
  * @param added list of properties sorted by their appearance in JSON Schemas
  * @param modified list of properties changed in target Schema;
  *                 if some property was added in successive Schema and modified
  *                 after that, it should appear in [[added]]
  * @param removed set of keys removed in target Schema
  */
case class SchemaDiff(added: List[(Pointer.SchemaPointer, Schema)],
                      modified: Set[SchemaDiff.Modified],
                      removed: List[(Pointer.SchemaPointer, Schema)]) {

  def merge(other: SchemaDiff): SchemaDiff =
    SchemaDiff(added ++ FlatSchema.postProcess(other.added.toSet), modified ++ other.modified, removed ++ other.removed)
}

object SchemaDiff {

  val empty = SchemaDiff(List.empty, Set.empty, List.empty)

  case class Modified(pointer: Pointer.SchemaPointer, from: Schema, to: Schema) {
    /** Show only properties that were changed */
    def getDelta = jsonschema.Delta.build(from, to)
  }

  // We should assume a property that if two particular schemas delta result in X pointer,
  // No two schemas between them can give pointer higher than X

  /**
    * Generate diff from source list of properties to target though sequence of intermediate
    *
    * @param source source list of JSON Schema properties
    * @param target non-empty list of successive JSON Schema properties including target
    * @return diff between two Schmea
    */
  def diff(source: SubSchemas, target: SubSchemas): SchemaDiff = {
    val addedKeys = getAddedKeys(source, target).toList
    val modified = getModifiedProperties(source, target)
    val removedKeys = getRemovedProperties(source, target).toList
    SchemaDiff(addedKeys, modified, removedKeys)
  }

  /** Build `SchemaDiff` from list of schemas */
  def build(source: Segment): SchemaDiff = {
    val result = source.schemas.tail.foldLeft(DiffMerge.init(source.schemas.head)) {
      case (acc, SelfDescribingSchema(_, schema)) =>
        val subschemas = FlatSchema.build(schema).subschemas
        val diff = SchemaDiff.diff(acc.previous, subschemas)
        DiffMerge(acc.diff.merge(diff), subschemas)
    }
    result.diff
  }

  /**
    * Get list of new properties in order they appear in subsequent Schemas
    *
    * @param source original Schema
    * @param successive all subsequent Schemas
    * @return possibly empty list of keys in correct order
    */
  def getAddedKeys(source: SubSchemas, successive: SubSchemas): Set[(Pointer.SchemaPointer, Schema)] = {
    val sourceKeys = source.map(_._1)
    successive.foldLeft(Set.empty[(Pointer.SchemaPointer, Schema)]) { case (acc, (pointer, schema)) =>
      if (sourceKeys.contains(pointer)) acc
      else acc + (pointer -> schema)
    }
  }

  /**
    * Get list of JSON Schema properties modified between two versions
    *
    * @param source original list of JSON Schema properties
    * @param target final list of JSON Schema properties
    * @return set of properties changed in target Schema
    */
  def getModifiedProperties(source: SubSchemas, target: SubSchemas): Set[Modified] =
    target.flatMap { case (pointer, targetSchema) =>
      source.find { case (p, _) => p == pointer } match {
        case None => Set.empty[Modified]
        case Some((_, sourceSchema)) if sourceSchema == targetSchema => Set.empty[Modified]
        case Some((_, sourceSchema)) => Set(Modified(pointer, sourceSchema, targetSchema))
      }
    }

  def getRemovedProperties(source: SubSchemas, target: SubSchemas): SubSchemas =
    source.foldLeft(Set.empty[(Pointer.SchemaPointer, Schema)]) {
      case (acc, (pointer, s)) =>
        val removed = !target.exists { case (p, _) => pointer == p }
        if (removed) acc + (pointer -> s) else acc
    }

  private case class DiffMerge(diff: SchemaDiff, previous: SubSchemas)

  private object DiffMerge {
    def init(origin: IgluSchema): DiffMerge =
      DiffMerge(SchemaDiff.empty, FlatSchema.build(origin.schema).subschemas)
  }
}
