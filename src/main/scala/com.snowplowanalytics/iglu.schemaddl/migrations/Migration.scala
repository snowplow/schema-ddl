/*
 * Copyright (c) 2014-2016 Snowplow Analytics Ltd. All rights reserved.
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

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaList, SchemaMap, SchemaVer, SelfDescribingSchema}
import com.snowplowanalytics.iglu.schemaddl.{IgluSchema, MigrationMap, ModelGroup, OrderedSubSchemas}

/**
 * Class representing common information about Schema change, without details
 * about specific DDLs
 *
 * @param vendor Schema vendor
 * @param name Schema name
 * @param from source Schema version
 * @param to target Schema version
 * @param diff ordered map of added Schema properties
 */
case class Migration(vendor: String, name: String, from: SchemaVer.Full, to: SchemaVer.Full, diff: SchemaDiff) {
  override def toString: String = s"Migration of $vendor/$name from ${from.asString} to ${to.asString} with $diff"
}

object Migration {

  /**
    * List of schemas to build migration from
    * Always belong to the same vendor/name/model triple,
    * always have at least two element (head - initial schema, last - destination)
    */
  case class OrderedSchemas private(schemas: NonEmptyList[IgluSchema]) extends AnyVal

  /** Represents error cases which can be get from `MigrateFrom` function */
  sealed trait MigrateFromError extends Product with Serializable
  case object MigrateFromError {

    /**
      * Returned when current schema is not found in the
      * given schemas
      */
    case object SchemaKeyNotFoundInSchemas extends MigrateFromError

    /**
      * Returned when current schema is last version of
      * given schemas
      */
    case object SchemaInLatestState extends MigrateFromError
  }

  object OrderedSchemas {
    /**
      * Filter out a single `SchemaList` containing all schemas
      * @param matrix whole migration matrix (where each version is mapped to some subsequent version)
      * @param last in case we need not a fullest matrix
      */
    def getComplete(matrix: NonEmptyList[OrderedSchemas], last: Option[SchemaVer.Full]): OrderedSchemas =
      last match {
        case Some(version) =>
          matrix
            .find(m => m.schemas.head.self.schemaKey.version == SchemaVer.Full(1,0,0) && m.schemas.last.self.schemaKey.version == version)
            .getOrElse(throw new IllegalStateException(s"Matrix $matrix does not contain either 1-0-0 or ${version.asString}"))
        case None =>
          matrix.toList.maxBy(_.schemas.length)
      }

    def fromSchemaList[F[_]: Monad, E](keys: SchemaList, fetch: SchemaKey => EitherT[F, E, IgluSchema]): EitherT[F, E, OrderedSchemas] =
      for {
        schemas <- keys.schemas.traverse { key => fetch(key) }
      } yield OrderedSchemas(NonEmptyList.fromListUnsafe(schemas))
  }

  /**
   * Build migration from a `sourceSchema` to the last schema in list of `successiveSchemas`
   * This method requires all intermediate schemas because we need to keep an order of properties
   */
  def buildMigration(source: OrderedSchemas): Migration = {
    val base = source.schemas.head.self.schemaKey
    val diff = SchemaDiff.build(source)
    Migration(base.vendor, base.name, base.version, source.schemas.last.self.schemaKey.version, diff)
  }

  // NEL has minimum 2 elements
  def buildMatrix[A](as: List[A]) = {
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

  /**
    * Creates model groups of schemas and returns them as OrderedSchemas
    * @param schemas List of schema to create migration matrix
    * @return List of IgluSchema as Ior.left if some groups in schemas contain
    *         only one schema and no meaningful migrations can be provided
    *         or List of OrderedSchemas as Ior.right if there are more than
    *         one schemas in some groups and migrations can be provided
    */
  def buildMigrationMatrix(schemas: NonEmptyList[IgluSchema]): Ior[NonEmptyList[IgluSchema], NonEmptyList[OrderedSchemas]] = {
    schemas
      .groupByNem { case SelfDescribingSchema(SchemaMap(SchemaKey(v, n, _, SchemaVer.Full(model, _, _))), _) => (v, n, model) }
      .toNel
      .nonEmptyPartition { case (_, s) =>
        buildMatrix(s.toList).map { case (_, _, ss) => OrderedSchemas(ss) } match {
          case Nil => Left(s)
          case matrix => Right(NonEmptyList.fromListUnsafe(matrix))
        }
      }.leftMap(_.flatten).map(_.flatten)
  }

  /**
   * Build [[MigrationMap]], a map of source Schema to it's migrations,
   * where all source Schemas belong to a single model-revision Schema criterion
   *
   * @param schemas source Schemas belong to a single model-revision criterion
   * @return List of IgluSchema as Ior.left if some groups in schemas contain
    *        only one schema and no meaningful migrations can be provided
    *        or Ior.right of migration map of each Schema to list of all available
    *        migrations if there are more than one schemas in some groups and
    *        migrations can be provided
   */
  def buildMigrationMap(schemas: NonEmptyList[IgluSchema]): Ior[NonEmptyList[IgluSchema], MigrationMap] =
    buildMigrationMatrix(schemas).map { l =>
      // groupBy of NonEmptyList requires cats.Order of SchemaMap
      // however there is no dependency like this in groupBy of Scala List.
      // Therefore, it is converted to Scala List initially and reconverted
      // to NonEmptyList afterward.
      l.toList.map(source => (source.schemas.head.self, buildMigration(source)))
        .groupBy(_._1)
        .mapValues(m => NonEmptyList.fromListUnsafe(m.map(_._2)))
    }

  /**
    * Build a map of source Schema to its OrderedSubSchemas, where all source Schemas
    * are last version of their model group
    * @param schemas source Schemas
    * @return map of last version of Schema model group to its OrderedSubSchemas
    */
  def buildOrderedSubSchemasMap(schemas: NonEmptyList[IgluSchema]): Map[SchemaMap, OrderedSubSchemas] = {
    val migrationMap = buildMigrationMap(schemas).right.getOrElse(Map.empty)
    val flatSchemaMap = groupWithLastFlatSchema(schemas)
    val orderingMap = createOrderingMap(migrationMap)
    flatSchemaMap.map {
      case (schemaMap, flatSchema) =>
        val order = orderingMap.getOrElse(modelGroup(schemaMap), Nil)
        val orderedSubSchemas = sortColumns(flatSchema, order)
        (schemaMap, orderedSubSchemas)
    }
  }

  /**
    * Get a migration from current state to the latest known schema
    * where error can be if schema key does not belong to these schemas
    * or schema key is already a latest state
    * @param current schemaKey of current state
    * @param schemas schemas of model group which ordered according to
    *                their version
    * @return return Either.left in case of error cases which is specified
    *         above or Migration as Either.right
    */
  def migrateFrom(current: SchemaKey, schemas: OrderedSchemas): Either[MigrateFromError, Migration] = {
    if (!schemas.schemas.map(_.self).toList.contains(SchemaMap(current)))
      MigrateFromError.SchemaKeyNotFoundInSchemas.asLeft
    else {
      val newerSchemas = schemas.schemas.filter { schema =>
        val schemaKey = schema.self.schemaKey
        val haveSameModelGroup = modelGroup(SchemaMap(schemaKey)).equals(modelGroup(SchemaMap(current)))
        val sameOrNewerThanCurrent = Ordering[SchemaVer].gteq(schemaKey.version, current.version)
        haveSameModelGroup && sameOrNewerThanCurrent
      }
      NonEmptyList.fromList(newerSchemas) match {
        case None => MigrateFromError.SchemaKeyNotFoundInSchemas.asLeft
        case Some(NonEmptyList(_, Nil)) => MigrateFromError.SchemaInLatestState.asLeft
        case Some(l) => buildMigration(OrderedSchemas(l)).asRight
      }
    }
  }

  /**
    * Groups given schemas with last version of their model group and its corresponding FlatSchema
    * @param schemas source Schemas
    * @return map of last version of Schema model group to its FlatSchema
    */
  private def groupWithLastFlatSchema(schemas: NonEmptyList[IgluSchema]): Map[SchemaMap, FlatSchema] = {
    val aggregated = schemas.foldLeft(Map.empty[ModelGroup, (SchemaMap, FlatSchema)]) {
      case (acc, igluSchema) =>
        acc.get(modelGroup(igluSchema.self)) match {
          case Some((desc, _)) if desc.schemaKey.version.revision < igluSchema.self.schemaKey.version.revision =>
            acc ++ Map((modelGroup(igluSchema.self), (igluSchema.self, FlatSchema.build(igluSchema.schema))))
          case Some((desc, _)) if desc.schemaKey.version.revision == igluSchema.self.schemaKey.version.revision &&
            desc.schemaKey.version.addition < igluSchema.self.schemaKey.version.addition =>
            acc ++ Map((modelGroup(igluSchema.self), (igluSchema.self, FlatSchema.build(igluSchema.schema))))
          case None =>
            acc ++ Map((modelGroup(igluSchema.self), (igluSchema.self, FlatSchema.build(igluSchema.schema))))
          case _ => acc
        }
    }
    aggregated.map { case (_, (desc, flatSchema)) => (desc, flatSchema) }
  }

  /**
    * Creates field ordering for changes after initial version using migrationMap
    * @param migrationMap migration map of each Schema to list of all available migrations
    * @return map of model group to its ordering of fields which are added after initial version of schema
    */
  private def createOrderingMap(migrationMap: MigrationMap): Map[ModelGroup, List[String]] = {
    migrationMap.collect {
      case (schemaMap, migrations) if schemaMap.schemaKey.version.addition == 0 => {
        val res = migrations.foldLeft(List.empty[String]) { (acc, migration) =>
          val currOrder = migration.diff.added.map { case(p, _) => FlatSchema.getName(p) }
          val remainingOrder = currOrder.diff(acc)
          acc ++ remainingOrder
        }
        (modelGroup(schemaMap), res)
      }
    }
  }

  /**
    * Sort columns of given FlatSchema to according to given ordering
    * @param lastVersionFlatSchema FlatSchema of one of model group's last version of schema
    * @param order order of fields which are added after initial version of schema
    * @return subschemas which ordered according to given ordering
    */
  private def sortColumns(lastVersionFlatSchema: FlatSchema, order: List[String]): OrderedSubSchemas = {
    val ordered = FlatSchema.order(lastVersionFlatSchema.subschemas).map { case (p, s) => (FlatSchema.getName(p), (p, s)) }
    val columns = ordered.map(_._1)
    val columnMap = ordered.toMap
    val columnsToSort = order intersect columns
    val addedOrderedColumns = order.flatMap(columnMap.get)
    val initialColumns = ordered.filterNot{ case (c, _) => columnsToSort.contains(c) }.map(_._2)
    initialColumns ++ addedOrderedColumns
  }

  /**
    * Extract from Schema description three elements defining MODEL
    * @param schemaMap Schema description
    * @return tuple of three values defining revision
    */
  private def modelGroup(schemaMap: SchemaMap): ModelGroup =
    (schemaMap.schemaKey.vendor, schemaMap.schemaKey.name, schemaMap.schemaKey.version.model)

}
