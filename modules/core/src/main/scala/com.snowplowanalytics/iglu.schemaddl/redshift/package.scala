package com.snowplowanalytics.iglu.schemaddl

import cats.data.NonEmptyList
import cats.syntax.option._
import cats.syntax.either._
import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.redshift.internal.Migrations

import scala.collection.mutable

package object redshift {

  // See the merge method scala doc for reference
  def assessRedshiftMigration(
                               srcKey: SchemaKey,
                               tgtKey: SchemaKey,
                               srcSchema: Schema,
                               tgtSchema: Schema
                             ): Either[NonEmptyList[Migrations.Breaking], List[Migrations.NonBreaking]] =
    ShredModel(srcKey, srcSchema).merge(ShredModel(tgtKey, tgtSchema))
      .map(_.allMigrations)
      .leftMap(_._2)

  def isRedshiftMigrationBreaking(srcKey: SchemaKey, tgtKey: SchemaKey, srcSchema: Schema, tgtSchema: Schema): Boolean =
    assessRedshiftMigration(srcKey, tgtKey, srcSchema, tgtSchema).isRight

  def getFinalMergedModel(schemas: NonEmptyList[(SchemaKey, Schema)]): ShredModel =
    foldMapMergeRedshiftSchemas(schemas).values.collectFirst {
      case model: ShredModel if !model.isRecovery => model
    }.get // first schema always would be there due to Nel, so `get` is safe

  /**
   * Build a map between schema key and their models.
   *
   * @param schemas - ordered list of schemas for the same family
   * @return
   */
  def foldMapRedshiftSchemas(schemas: NonEmptyList[(SchemaKey, Schema)]): collection.Map[SchemaKey, ShredModel] = {
    var acc = mutable.Map.empty[SchemaKey, ShredModel]
    var maybeLastGoodModel = Option.empty[ShredModel]
    val models = schemas.map { case (k, s) => ShredModel(k, s) }

    // first pass to build the mapping between key and corresponding model
    models.toList.foreach(model => maybeLastGoodModel match {
      case Some(lastModel) => lastModel.merge(model) match {
        case Left(errors) => acc = acc.updated(model.schemaKey, errors._1)
        // We map original model here, as opposed to merged one.
        case Right(mergedModel) => acc = acc.updated(model.schemaKey, model)
          maybeLastGoodModel = mergedModel.some
      }
      case None =>
        acc = acc.updated(model.schemaKey, model)
        maybeLastGoodModel = model.some
    })

    acc
  }


  /**
   * Build a map between schema key and a merged or recovered model. For example if schemas X and Y and mergable, both 
   * would link to schema XY (product).
   *
   * @param schemas - ordered list of schemas for the same family
   * @return
   */
  def foldMapMergeRedshiftSchemas(schemas: NonEmptyList[(SchemaKey, Schema)]): collection.Map[SchemaKey, ShredModel] = {
    var acc = mutable.Map.empty[SchemaKey, ShredModel]
    var maybeLastGoodModel = Option.empty[ShredModel]
    val models = schemas.map { case (k, s) => ShredModel(k, s) }

    // first pass to build the mapping between key and accumulated model
    models.toList.foreach(model => maybeLastGoodModel match {
      case Some(lastModel) => lastModel.merge(model) match {
        case Left(errors) => acc = acc.updated(model.schemaKey, errors._1)
        case Right(mergedModel) => acc = acc.updated(mergedModel.schemaKey, mergedModel)
          maybeLastGoodModel = mergedModel.some
      }
      case None =>
        acc = acc.updated(model.schemaKey, model)
        maybeLastGoodModel = model.some
    })

    // seconds pass to backfill the last model version for initial keys.
    acc.mapValues(
      model =>
        if (!model.isRecovery)
          maybeLastGoodModel.get
        else
          model
    )
  }
}
