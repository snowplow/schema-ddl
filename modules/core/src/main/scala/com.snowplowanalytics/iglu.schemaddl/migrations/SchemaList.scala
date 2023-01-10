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

package com.snowplowanalytics.iglu.schemaddl.migrations

// cats
import cats.{ Functor, Monad }
import cats.data.{ EitherT, NonEmptyList, Ior }
import cats.implicits._
import cats.kernel.Order

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaMap, SchemaVer, SchemaList => SchemaKeyList}

import com.snowplowanalytics.iglu.schemaddl.{IgluSchema, ModelGroup}
import com.snowplowanalytics.iglu.schemaddl.experimental.VersionTree

/**
  * Properly grouped and ordered list of schemas
  * Core migration data structure- no valid migrations can be constructed
  * without knowing it Schemas should always belong to the same vendor/name/model
  * and sorted by schema-creation time (static order can be known only for
  * unambiguous groups) and have all known versions (no gaps) Isomorphic to
  * Iglu Core's `SchemaList`
  */
sealed trait SchemaList extends Product with Serializable {
  /** Get the latest `SchemaMap` in a group */
  def latest: SchemaMap = this match {
    case SchemaList.Full(schemas) => schemas.last.self
    case SchemaList.Single(schema) => schema.self
  }

  /** Drop all schemas *after* certain version; Return `None` if vendor/name do not match */
  def until(schemaKey: SchemaKey): Option[SchemaList] = {
    val vendor = latest.schemaKey.vendor
    val name = latest.schemaKey.name
    val model = latest.schemaKey.version.model

    if (vendor != schemaKey.vendor || name != schemaKey.name || model != schemaKey.version.model) None
    else this match {
      case _: SchemaList.Single => this.some
      case SchemaList.Full(schemas) =>
        val list = schemas.toList.span(_.self.schemaKey != schemaKey) match {
          case (before, after) => NonEmptyList.fromListUnsafe(before ::: after.take(1))
        }
        SchemaList.Full(list).some
    }
  }
}

object SchemaList {

  /** Multiple schemas, grouped by model and in canonical order */
  sealed abstract case class Full(schemas: NonEmptyList[IgluSchema]) extends SchemaList {

    /** Create [[Segment]] from schemas in current [[SchemaList.Full]] */
    def toSegment: Segment = Segment(schemas)

    /** Create segments from all possible combinations of NonEmptyList(source,between,destination), */
    def extractSegments: NonEmptyList[Segment] = {
      val res = SchemaList.buildMatrix(schemas.toList).map { case (_, _, nel) => Segment(nel) }
      NonEmptyList.fromListUnsafe(res)
    }

    /** Create new segment with items after index */
    private[migrations] def afterIndex(i: Int): Option[Segment] =
      schemas.zipWithIndex.collect { case (s, c) if c >= i => s } match {
        case h :: t => Some(Segment(NonEmptyList(h, t)))
        case _ => None
      }
  }
  object Full {
    private[migrations] def apply(schemas: NonEmptyList[IgluSchema]): Full = new Full(schemas) {}
  }

  /** Single init schema (e.g. `1-0-0`, `3-0-0`). No migrations should be involved */
  sealed abstract case class Single(schema: IgluSchema) extends SchemaList
  object Single {
    private[migrations] def apply(schema: IgluSchema): Single = new Single(schema) {}
  }

  // Incomplete SchemaLists

  /** Has all properties of [[SchemaList.Full]], except absence of gaps */
  sealed abstract case class Segment(schemas: NonEmptyList[IgluSchema])
  object Segment {
    private[migrations] def apply(schemas: NonEmptyList[IgluSchema]): Segment = new Segment(schemas) {}
  }

  /** Has all properties of [[SchemaList.Full]], except canonical order */
  sealed abstract case class ModelGroupSet(schemas: NonEmptyList[IgluSchema])
  object ModelGroupSet {
    /** Split schemas into a lists grouped by model group (still no order implied) */
    def groupSchemas(schemas: NonEmptyList[IgluSchema]): NonEmptyList[ModelGroupSet] =
      schemas.groupByNem(schema => getModelGroup(schema.self)).toNel.map {
        case (_, nel) => new ModelGroupSet(nel) {}
      }
  }

  // Constructors

  /**
    * Fetch from Iglu Server and parse each schema from `SchemaKeyList`, using generic resolution function
    * (IO-dependent) valid constructor of `SchemaList`
    * @param keys non-empty properly ordered list of `SchemaKey`s, fetched from Iglu Server
    * @param fetch resolution function
    * @return properly ordered list of parsed JSON Schemas
    */
  def fromSchemaList[F[_]: Monad, E](keys: SchemaKeyList, fetch: SchemaKey => EitherT[F, E, IgluSchema]): EitherT[F, E, SchemaList] =
    keys.schemas.traverse(key => fetch(key)).map {
      case Nil => throw new IllegalStateException("Result list can not be empty")
      case h :: Nil => Single(h)
      case h :: t => Full(NonEmptyList(h, t))
    }

  /**
    * Build SchemaLists from fetched IgluSchemas. Given EitherT should
    * wrap fetching schemas from /schemas endpoint of Iglu Server
    * because they need to ordered.
    * @param fetch EitherT which wraps list of ordered Iglu Schemas
    * @return list of SchemaLists which created from fetched schemas
    */
  def fromFetchedSchemas[F[_]: Functor, E](fetch: EitherT[F, E,  NonEmptyList[IgluSchema]]): EitherT[F, E, NonEmptyList[SchemaList]] =
    fetch.map(ModelGroupSet.groupSchemas(_).map(buildWithoutReorder))

  /**
    * Construct [[SchemaList]] from list of schemas, but only if order is unambiguous and no gaps
    * If order is ambiguous (left returned) then the only safe order can be retrieved from
    * Iglu Server (by `fromSchemaList`), use other constructors on your own risk
    * @param modelGroup non-empty list of schema belonging to the same [[ModelGroup]]
    * @return error object as Either.left in case of transformation is not successful or
    *         created SchemaList as Either.right if everything is okay
    */
  def fromUnambiguous(modelGroup: ModelGroupSet): Either[BuildError, SchemaList] =
    modelGroup.schemas match {
      case NonEmptyList(h, Nil) =>
        Single(h).asRight
      case schemas if ambiguos(schemas.map(key)) =>
        BuildError.AmbiguousOrder(modelGroup).asLeft
      case schemas if !noGapsInModelGroup(schemas.map(key)) =>
        BuildError.GapInModelGroup(modelGroup).asLeft
      case schemas if withinRevision(schemas.map(key)) =>
        Full(schemas.sortBy(_.self.schemaKey.version.addition)).asRight
      case schemas if onlyInits(schemas.map(key)) =>
        Full(schemas.sortBy(_.self.schemaKey.version.revision)).asRight
      case _ => BuildError.UnexpectedState(modelGroup).asLeft
    }

  /**
    * Construct `SchemaList` from list of schemas, if there is no gaps.
    * Order given model group according to their schema key and resulting
    * ordering might not be correct if given schema list ambiguous
    * therefore it is not safe to use this function with ambiguous schema list.
    * @param modelGroup non-empty list of schema belonging to the same `ModelGroup`
    * @return error object as Either.left in case of transformation is not successful or
    *         created SchemaList as Either.right if everything is okay
    */
  def unsafeBuildWithReorder(modelGroup: ModelGroupSet): Either[BuildError, SchemaList] = {
    val sortedSchemas = modelGroup.schemas.sortBy(_.self.schemaKey)(Order.fromOrdering(SchemaKey.ordering))
    sortedSchemas match {
      case NonEmptyList(h, Nil) =>
        Single(h).asRight
      case schemas if !noGapsInModelGroup(schemas.map(key)) =>
        BuildError.GapInModelGroup(modelGroup).asLeft
      case _ =>
        Full(sortedSchemas).asRight
    }
  }

  /**
    * Construct `SchemaList`s from unordered list
    *
    * @param schemas non-empty list of schemas which can belong to different model groups
    * @return non-empty list of errors while creating SchemaLists in Ior.left and
    *         non-empty list of SchemaList which created from given schemas in Ior.right
    */
  def buildMultiple(schemas: NonEmptyList[IgluSchema]): Ior[NonEmptyList[BuildError], NonEmptyList[SchemaList]] =
    ModelGroupSet.groupSchemas(schemas).nonEmptyPartition(fromUnambiguous)

  /**
    * Construct SingleSchema from given Schema if it is first version of its model group
    * @param schema IgluSchems to create SingleSchema
    * @return None if given schema is not first version of its model group
    *         Some(SingleSchema(schema)) otherwise
    */
  def buildSingleSchema(schema: IgluSchema): Option[SchemaList] = {
    val version = schema.self.schemaKey.version
    if (version.model >= 1 && version.revision == 0 && version.addition == 0)
      Some(Single(schema))
    else
      None
  }

  /**
    * Construct SchemaList from given model group without reordering
    * its schema list
    * @param modelGroup ModelGroup to create SchemaList
    * @return created SchemaList from given model group
    */
  private def buildWithoutReorder(modelGroup: ModelGroupSet): SchemaList =
    modelGroup.schemas match {
      case NonEmptyList(h, Nil) => Single(h)
      case schemas => Full(schemas)
    }

  /** [[SchemaList]] construction errors */
  sealed trait BuildError extends Product with Serializable

  object BuildError {
    /**
      * Given model group have schemas which could not be ordered unambiguously.
      * For example, [1-0-0, 1-1-0, 1-0-1] schema list could not be ordered
      * unambiguously because it could be either `[1-0-0, 1-0-1, 1-1-0]` or
      * `[1-0-0, 1-1-0, 1-0-1]`
      */
    case class AmbiguousOrder(schemas: ModelGroupSet) extends BuildError

    /** Gap in the schema list, e.g. `[1-0-0, 1-0-2]` is missing 1-0-1 version */
    case class GapInModelGroup(schemas: ModelGroupSet) extends BuildError

    /** Unknown error, should never be reached */
    case class UnexpectedState(schemas: ModelGroupSet) extends BuildError
  }

  /**
    * Get list of all possible combinations of (source, destination, List(source,between,destination)),
    *
    * {{{
    * >>> buildMatrix(List(1,2,3)
    * List((1,2,NonEmptyList(1, 2)), (1,3,NonEmptyList(1, 2, 3)), (2,3,NonEmptyList(2, 3)))
    * }}}
    */
  private def buildMatrix[A](as: List[A]) = {
    val ordered = as.zipWithIndex
    for {
      (from, fromIdx) <- ordered
      (to,   toIdx)   <- ordered
      cell <- NonEmptyList.fromList(ordered.filter { case (_, i) => i >= fromIdx && i <= toIdx }) match {
        case None => Nil
        case Some(NonEmptyList(_, Nil)) => Nil
        case Some(nel) => List((from, to, nel.map(_._1)))
      }
    } yield cell
  }

  /** Extract meaningful schema group */
  private def getModelGroup(schemaMap: SchemaMap): ModelGroup =
    (schemaMap.schemaKey.vendor, schemaMap.schemaKey.name, schemaMap.schemaKey.version.model)

  // helper functions
  private def ambiguos(keys: NonEmptyList[SchemaKey]): Boolean =
    !withinRevision(keys) && !onlyInits(keys)
  private def noGapsInModelGroup(keys: NonEmptyList[SchemaKey]): Boolean = {
    val initialSchema = SchemaVer.Full(1, 0, 0)
    val initialVersions = keys.map(_.version).toList
    // since gaps in the model groups tried to be detected in this function,
    // some of the groups' model number can be 2, in that case initial schema (1-0-0)
    // is added in order to not get missing init schema error
    val versions = if (keys.map(model).toList.distinct.contains(1)) initialVersions else initialSchema :: initialVersions
    VersionTree.build(versions).fold(_ => false, _ => true)
  }
  private def withinRevision(keys: NonEmptyList[SchemaKey]): Boolean =
    keys.map(model).toList.distinct.lengthCompare(1) == 0 &&
      keys.map(revision).toList.distinct.lengthCompare(1) == 0
  private def onlyInits(keys: NonEmptyList[SchemaKey]): Boolean =
    keys.map(model).toList.distinct.lengthCompare(1) == 0 &&
      keys.map(addition).forall(a => a == 0)
  private def model(key: SchemaKey): Int = key.version.model
  private def revision(key: SchemaKey): Int = key.version.revision
  private def addition(key: SchemaKey): Int = key.version.addition
  private def key(schema: IgluSchema): SchemaKey = schema.self.schemaKey
}
