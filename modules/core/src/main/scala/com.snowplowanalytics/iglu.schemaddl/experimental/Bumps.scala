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
package com.snowplowanalytics.iglu.schemaddl.experimental

import com.snowplowanalytics.iglu.core.VersionKind
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Delta
import com.snowplowanalytics.iglu.schemaddl.migrations.SchemaDiff

/** Module responsible for version-bump recognition */
object Bumps {

  /** A function deciding if `SchemaDiff` matches certain SchemaVer convention */
  type VersionPredicate = SchemaDiff => Boolean

  val ModelChecks: List[VersionPredicate] =
    List(required, typeChange)
  val RevisionChecks: List[VersionPredicate] =
    List(typeWidening, constraintWidening)
  val AdditionChecks: List[VersionPredicate] =
    List(optionalAdded)

  /** Get a difference between two schemas and return a version part that must be bumped */
  def getPointer(diff: SchemaDiff): Option[VersionKind] =
    if (ModelChecks.exists(p => p(diff))) Some(VersionKind.Model)
    else if (RevisionChecks.exists(p => p(diff))) Some(VersionKind.Revision)
    else if (AdditionChecks.exists(p => p(diff))) Some(VersionKind.Addition)
    else None

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

  def becameRequired[A](getter: Delta => Delta.Changed[A])(m: SchemaDiff.Modified): Boolean =
    getter(m.getDelta) match {
      case d @ Delta.Changed(_, _) if d.nonEmpty =>
        val wasOptional = m.from.canBeNull
        val becameRequired = !m.to.canBeNull
        wasOptional && becameRequired
      case _ => false
    }

  def optionalAdded(diff: SchemaDiff): Boolean =
    diff.added.forall(_._2.canBeNull)
}
