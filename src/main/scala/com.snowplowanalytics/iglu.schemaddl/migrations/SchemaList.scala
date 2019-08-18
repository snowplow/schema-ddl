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

// cats
import cats.Monad
import cats.data._
import cats.implicits._
import cats.kernel.Order

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaList => SchemaKeyList, SchemaMap, SchemaVer}

import com.snowplowanalytics.iglu.schemaddl.{IgluSchema, ModelGroup, VersionTree}
import SchemaList.FullListCreationError._

/**
  * Always belong to the same vendor/name/model triple.
  * Schemas in the list will be in correct order.
  */
sealed trait SchemaList extends Product with Serializable

object SchemaList {

  /**
    * Always belong to the same vendor/name/model triple,
    * always have at least two elements: head (initial x-0-0 schema) and latest schema
    */
  case class SchemaListFull private(schemas: NonEmptyList[IgluSchema]) extends SchemaList {

    /**
      * Create SchemaListSegment from schemas in current SchemaListFull
      */
    def toSegment: SchemaListSegment = SchemaListSegment(schemas)

    /**
      * Create segments from all possible combinations of NonEmptyList(source,between,destination),
      */
    def extractSegments: NonEmptyList[SchemaListSegment] = {
      val res = SchemaList.buildMatrix(schemas.toList).map { case(_, _, nel) => SchemaListSegment(nel) }
      NonEmptyList.fromListUnsafe(res)
    }

    /**
      * Create new segment with items after index
      */
    def afterIndex(i: Int): Option[SchemaListSegment] =
      schemas.zipWithIndex.filter { case (_, c) => c >= i }.map(_._1) match {
        case h :: t => Some(SchemaListSegment(NonEmptyList(h, t)))
        case _ => None
      }
  }
  private[migrations] object SchemaListFull

  /**
    * Represents schema list with single item.
    * It's version is always first of the model group
    * such that 1-0-0, 2-0-0 etc.
    */
  case class SingleSchema private(schema: IgluSchema) extends SchemaList
  private[migrations] object SingleSchema

  /**
    * Has all properties of SchemaListFull except that it can miss initial or last schemas
    */
  case class SchemaListSegment private(schemas: NonEmptyList[IgluSchema]) extends AnyVal
  private[migrations] object SchemaListSegment

  /**
    * Represents errors while creating SchemaFullList
    */
  sealed trait FullListCreationError extends Product with Serializable

  object FullListCreationError {

    /**
      * Returned when given model group have schemas which
      * they could not be ordered unambiguously.
      * For example, [1-0-0, 1-1-0, 1-0-2] schema list
      * could not be ordered unambiguously because it could be
      * either [1-0-0, 1-0-2, 1-1-0] or [1-0-0, 1-1-0, 1-0-2]
      */
    case class AmbiguousOrder(schemas: ModelGroupList) extends FullListCreationError

    /**
      * Returned when there is a gap in the schema list.
      * [1-0-0, 1-0-1, 1-0-3] is missing 1-0-2 version,
      * therefore gap error will be returned in this case
      */
    case class GapInModelGroup(schemas: ModelGroupList) extends FullListCreationError

    /**
      * Represents unexpected error cases while creating SchemaFullList
      */
    case class UnexpectedError(schemas: ModelGroupList) extends FullListCreationError
  }

  /**
    * Always belong to the same vendor/name/model triple,
    * however different from SchemaList, no ordering is implied.
    */
  sealed trait ModelGroupList {
    val schemas: NonEmptyList[IgluSchema]

    override def equals(that: Any): Boolean =
      that match {
        case that: ModelGroupList => {
          that.schemas == schemas
        }
        case _ => false
      }
  }

  object ModelGroupList {

    /** Split schemas into a lists grouped by model group (still no order implied) */
    def groupSchemas(schemas: NonEmptyList[IgluSchema]): NonEmptyList[ModelGroupList] =
      schemas.groupByNem(schema => getModelGroup(schema.self)).toNel.map { case (_, nel) => new ModelGroupList { val schemas = nel } }
  }

  /**
    * Fetch from Iglu Server and parse each schema from `SchemaKeyList`, using generic resolution function
    * (IO-dependent) valid constructor of `SchemaList`
    * @param keys non-empty properly ordered list of `SchemaKey`s, fetched from Iglu Server
    * @param fetch resolution function
    * @return properly ordered list of parsed JSON Schemas
    */
  def fromSchemaList[F[_]: Monad, E](keys: SchemaKeyList, fetch: SchemaKey => EitherT[F, E, IgluSchema]): EitherT[F, E, SchemaList] =
    for {
      schemas <- keys.schemas.traverse { key => fetch(key) }
    } yield {
      schemas match {
        case Nil => throw new IllegalStateException("Result list can not be empty")
        case h :: Nil => SingleSchema(h)
        case h :: t => SchemaListFull(NonEmptyList(h, t))
      }
    }

  /**
    * Fetch NonEmptyList of IgluSchemas from Iglu Server, using generic resolution function
    * @param fetch resolution function
    * @return list of SchemaLists which created from fetched schemas
    */
  def buildMultipleFromFetchedSchemas[F[_]: Monad, E](fetch: => EitherT[F, E,  NonEmptyList[IgluSchema]]): EitherT[F, E, NonEmptyList[SchemaList]] =
    for {
      schemas <- fetch
      res = ModelGroupList.groupSchemas(schemas)
        .map(buildWithoutReorder)
    } yield res

  /**
    * Construct `SchemaList` from list of schemas, but only if order is unambiguous (and no gaps)
    * (static) valid constructor of `SchemaList`
    * If order is ambiguous (left returned) then the only safe order can be retrieved from
    * Iglu Server (by `fromSchemaList`), use other constructors on your own risk
    * @param modelGroup non-empty list of schema belonging to the same `ModelGroup`
    * @return error object as Either.left in case of transformation is not successful or
    *         created SchemaList as Either.right if everything is okay
    */
  def fromUnambiguous(modelGroup: ModelGroupList): Either[FullListCreationError, SchemaList] = {
    modelGroup.schemas match {
      case NonEmptyList(h, Nil) => SingleSchema(h).asRight
      case schemas if ambiguos(schemas.map(key)) => AmbiguousOrder(modelGroup).asLeft
      case schemas if !noGapsInModelGroup(schemas.map(key)) => GapInModelGroup(modelGroup).asLeft
      case schemas if withinRevision(schemas.map(key)) => SchemaListFull(schemas.sortBy(_.self.schemaKey.version.addition)).asRight
      case schemas if onlyInits(schemas.map(key)) => SchemaListFull(schemas.sortBy(_.self.schemaKey.version.revision)).asRight
      case _ => UnexpectedError(modelGroup).asLeft
    }
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
  def unsafeBuildWithReorder(modelGroup: ModelGroupList): Either[FullListCreationError, SchemaList] = {
    val sortedSchemas = modelGroup.schemas.sortBy(_.self.schemaKey)(Order.fromOrdering(SchemaKey.ordering))
    sortedSchemas match {
      case NonEmptyList(h, Nil) => SingleSchema(h).asRight
      case schemas if !noGapsInModelGroup(schemas.map(key)) => GapInModelGroup(modelGroup).asLeft
      case _ => SchemaListFull(sortedSchemas).asRight
    }
  }

  /**
    * Construct `SchemaList`s from unordered list
    *
    * @param schemas non-empty list of schemas which can belong to different model groups
    * @return non-empty list of errors while creating SchemaLists in Ior.left and
    *         non-empty list of SchemaList which created from given schemas in Ior.right
    */
  def buildMultiple(schemas: NonEmptyList[IgluSchema]): Ior[NonEmptyList[FullListCreationError], NonEmptyList[SchemaList]] =
    ModelGroupList.groupSchemas(schemas).nonEmptyPartition(fromUnambiguous)

  /**
    * Construct SingleSchema from given Schema if it is first version of its model group
    * @param schema IgluSchems to create SingleSchema
    * @return None if given schema is not first version of its model group
    *         Some(SingleSchema(schema)) otherwise
    */
  def buildSingleSchema(schema: IgluSchema): Option[SchemaList] = {
    val version = schema.self.schemaKey.version
    if (version.model >= 1 && version.revision == 0 && version.addition == 0)
      Some(SingleSchema(schema))
    else
      None
  }

  /**
    * Construct SchemaList from given model group without reordering
    * its schema list
    * @param modelGroup ModelGroup to create SchemaList
    * @return created SchemaList from given model group
    */
  private def buildWithoutReorder(modelGroup: ModelGroupList): SchemaList = {
    modelGroup.schemas match {
      case NonEmptyList(h, Nil) => SingleSchema(h)
      case schemas => SchemaListFull(schemas)
    }
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
    val versions = if (keys.map(model).toList.distinct.contains(1)) initialVersions else  initialSchema :: initialVersions
    VersionTree.build(versions) match {
      case Left(_) => false
      case Right(_) => true
    }
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
