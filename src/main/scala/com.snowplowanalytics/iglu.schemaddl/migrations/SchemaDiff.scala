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

import com.snowplowanalytics.iglu.core.SelfDescribingSchema

import com.snowplowanalytics.iglu.schemaddl._
import com.snowplowanalytics.iglu.schemaddl.Core.VersionPoint
import com.snowplowanalytics.iglu.schemaddl.jsonschema.{Delta, Pointer, Schema}
import com.snowplowanalytics.iglu.schemaddl.migrations.SchemaList._

/**
  * This class represents differences between *two* Schemas
  * Preserves no order because its up to `FinalDiff`
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
    SchemaDiff(added ++ FlatSchema.order(other.added.toSet), modified ++ other.modified, removed ++ other.removed)
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
  def build(source: SchemaListSegment): SchemaDiff = {
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

  /** A function deciding if `SchemaDiff` matches certain SchemaVer convention */
  type VersionPredicate = SchemaDiff => Boolean

  def becameRequired[A](getter: Delta => Delta.Changed[A])(m: Modified): Boolean =
    getter(m.getDelta) match {
      case d @ Delta.Changed(_, _) if d.nonEmpty =>
        val wasOptional = m.from.canBeNull
        val becameRequired = !m.to.canBeNull
        wasOptional && becameRequired
      case _ => false
    }

  /** New required property added or existing one became required */
  def required(diff: SchemaDiff): Boolean = {
    val newProperties = !diff.added.forall { case (_, schema) => schema.canBeNull }
    val becameRequiredType = diff.modified.exists(becameRequired(_.`type`))
    val becameRequiredEnum = diff.modified.exists(becameRequired(_.enum))
    newProperties || becameRequiredType || becameRequiredEnum
  }

  /** Changed or restricted type */
  def typeChange(diff: SchemaDiff): Boolean =
    diff.modified.exists { modified =>
      modified.getDelta.`type` match {
        case Delta.Changed(Some(from), Some(to)) => to.isSubsetOf(from) && from != to
        case Delta.Changed(None, Some(_)) => true
        case _ => false
      }
    }

  /** Revisioned type */
  def typeWidening(diff: SchemaDiff): Boolean =
    diff.modified.exists { modified =>
      modified.getDelta.`type` match {
        case Delta.Changed(Some(_), None) => true
        case Delta.Changed(Some(from), Some(to)) => from.isSubsetOf(to) && from != to
        case Delta.Changed(None, None) => false
        case Delta.Changed(None, Some(_)) => false
      }
    }

  /** Any constraints changed */
  def constraintWidening(diff: SchemaDiff): Boolean =
    diff.modified
      .map(_.getDelta)
      .exists { delta =>
        delta.multipleOf.nonEmpty ||
          delta.minimum.nonEmpty ||
          delta.maximum.nonEmpty ||
          delta.maxLength.nonEmpty ||
          delta.minLength.nonEmpty
      }

  def optionalAdded(diff: SchemaDiff): Boolean =
    diff.added.forall(_._2.canBeNull)

  val ModelChecks: List[VersionPredicate] =
    List(required, typeChange)

  val RevisionChecks: List[VersionPredicate] =
    List(typeWidening, constraintWidening)

  val AdditionChecks: List[VersionPredicate] =
    List(optionalAdded)

  def getPointer(diff: SchemaDiff): Option[VersionPoint] =
    if (ModelChecks.exists(p => p(diff))) Some(VersionPoint.Model)
    else if (RevisionChecks.exists(p => p(diff))) Some(VersionPoint.Revision)
    else if (AdditionChecks.exists(p => p(diff))) Some(VersionPoint.Addition)
    else None

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