/*
 * Copyright (c) 2016-2023 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.iglu.schemaddl.redshift.internal

import cats.data.State
import cats.instances.list._
import cats.instances.option._
import cats.syntax.alternative._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Pointer.SchemaPointer
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.CommonProperties
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.CommonProperties.Type
import com.snowplowanalytics.iglu.schemaddl.jsonschema.{Pointer, Schema}

import scala.annotation.tailrec

/**
 * An object represents flattened JSON Schema, i.e. JSON Schema processed with an algorithm
 * that unfolds nested JSON structure into sequence of typed pointers - JSON Pointers,
 * pointing to leaf schemas - schemas that cannot be flattened further.
 * Leaf schemas are mostly primitive values (strings, booleans etc), but also can be something
 * that could not be flattened
 *
 * This is mostly a transitive tool and should not be used by user-code and instead user
 * should be creating *ordered* list of typed pointers from multiple schema via `FlatSchema.extractProperties`
 *
 * @param subschemas set of typed pointers (order should not matter at this point)
 * @param required   keys listed in `required` property, whose parents also listed in `required`
 *                   some of parent properties still can be `null` and thus not required
 * @param parents    keys that are not primitive, but can contain important information (e.g. nullability)
 */
private[redshift] final case class FlatSchema(
                                     subschemas: FlatSchema.SubSchemas,
                                     required: Set[SchemaPointer],
                                     parents: FlatSchema.SubSchemas
                                   ) {
  // TODO: remove parents - we can preserve nullability without them

  /** Add a JSON Pointer that can be converted into a separate column */
  def withLeaf(pointer: SchemaPointer, schema: Schema): FlatSchema = {
    val updatedSchema =
      if ((pointer.value.nonEmpty && !required.contains(pointer)) || schema.canBeNull)
        schema.copy(
          `type` = schema.`type` match {
            case None => Some(Type.Null)
            case Some(t) => Some(t.withNull)
          }
        )
      else schema
    FlatSchema(subschemas + (pointer -> updatedSchema), required, parents)
  }

  def withRequired(pointer: SchemaPointer, schema: Schema): FlatSchema = {
    val currentRequired = FlatSchema.getRequired(pointer, schema).filter(nestedRequired)
    this.copy(required = currentRequired ++ required)
  }

  def withParent(pointer: SchemaPointer, schema: Schema): FlatSchema =
    FlatSchema(subschemas, required, parents + (pointer -> schema))

  /** All parents are required */
  @tailrec def nestedRequired(current: SchemaPointer): Boolean =
    current.parent.flatMap(_.parent) match {
      case None | Some(Pointer.Root) => true // Technically None should not be reached
      case Some(parent) => required.contains(parent) && nestedRequired(parent)
    }

  def checkUnionSubSchema(pointer: SchemaPointer): Boolean =
    subschemas
      .filter { case (p, _) => p.isParentOf(pointer) }
      .foldLeft(false) { case (acc, (_, schema)) =>
        schema.`type`.exists(_.isUnion) || acc
      }
}

object FlatSchema {

  /**
   * Set of Schemas properties attached to corresponding JSON Pointers
   * Unlike their original Schemas, these have `null` among types if they're not required
   */
  private[redshift] type SubSchemas = Set[(Pointer.SchemaPointer, Schema)]

  /**
   * Main function for flattening multiple schemas, preserving their lineage
   * in their properties. If user software (e.g. RDB Shredder) produces
   * unexpected list of columns - this is the culprit
   *
   * Builds subschemas which are ordered according to nullness of field,
   * name of field and which version field is added
   *
   * @param schema Top level schema to create ordered subschemas
   * @return list of typed pointers which are ordered according to criterias specified
   *         above
   */
  def extractProperties(schema: Schema): List[ShredModelEntry] = postProcess(build(schema).subschemas)
    .map(pair => ShredModelEntry.apply(pair._1, pair._2))


  /** Build FlatSchema from a single schema. Must be used only if there's only one schema */
  def build(schema: Schema): FlatSchema =
    Schema.traverse(schema, FlatSchema.save).runS(FlatSchema.empty).value

  /** Check if `current` JSON Pointer has all parent elements also required */

  /** Redshift-specific */
  // TODO: type object with properties can be primitive if properties are empty
  private def isLeaf(schema: Schema): Boolean = {
    val isNested = schema.withType(CommonProperties.Type.Object) && schema.properties.isDefined
    isHeterogeneousUnion(schema) || !isNested
  }

  /** This property shouldn't have been added (FlatSchemaSpec.e4) */
  private def shouldBeIgnored(pointer: SchemaPointer, flatSchema: FlatSchema): Boolean =
    pointer.value.exists {
      case Pointer.Cursor.DownProperty(Pointer.SchemaProperty.Items) => true
      case Pointer.Cursor.DownProperty(Pointer.SchemaProperty.PatternProperties) => true
      case Pointer.Cursor.DownProperty(Pointer.SchemaProperty.OneOf) => true
      case Pointer.Cursor.DownProperty(Pointer.SchemaProperty.AdditionalProperties) => true
      case _ => false
    } || (pointer.value.lastOption match {
      case Some(Pointer.Cursor.DownProperty(Pointer.SchemaProperty.OneOf)) =>
        true
      case _ => false
    }) || flatSchema.checkUnionSubSchema(pointer)

  private def getRequired(cur: SchemaPointer, schema: Schema): Set[SchemaPointer] =
    schema
      .required.map(_.value.toSet)
      .getOrElse(Set.empty)
      .map(prop => cur.downProperty(Pointer.SchemaProperty.Properties).downField(prop))

  val empty: FlatSchema = FlatSchema(Set.empty, Set.empty, Set.empty)

  private def save(pointer: SchemaPointer, schema: Schema): State[FlatSchema, Unit] =
    State.modify[FlatSchema] { flatSchema =>
      if (shouldBeIgnored(pointer, flatSchema))
        flatSchema
      else if (isLeaf(schema))
        flatSchema
          .withRequired(pointer, schema)
          .withLeaf(pointer, schema)
      else
        flatSchema
          .withRequired(pointer, schema)
          .withParent(pointer, schema)
    }

  /** Sort and clean-up */
  private def postProcess(subschemas: SubSchemas): List[(Pointer.SchemaPointer, Schema)] =
    subschemas.toList.sortBy { case (pointer, schema) =>
      (schema.canBeNull, pointer.getName)
    } match {
      case List((SchemaPointer(Nil), s)) if s.properties.forall(_.value.isEmpty) =>
        List()
      case other => other
    }

  /** Check if schema contains `oneOf` with different types */
  private[schemaddl] def isHeterogeneousUnion(schema: Schema): Boolean = {
    val oneOfCheck = schema.oneOf match {
      case Some(oneOf) =>
        val types = oneOf.value.map(_.`type`).unite.flatMap {
          case CommonProperties.Type.Union(set) => set.toList
          case singular => List(singular)
        }
        types.distinct.filterNot(_ == CommonProperties.Type.Null).length > 1
      case None => false
    }
    val unionTypeCheck = schema.`type`.forall(t => t.isUnion)
    oneOfCheck || unionTypeCheck
  }
}
