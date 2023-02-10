package com.snowplowanalytics.iglu.schemaddl

import cats.data.NonEmptyList
import cats.syntax.option._
import cats.syntax.either._
import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.schemaddl.redshift.ShredModel.GoodModel
import com.snowplowanalytics.iglu.schemaddl.redshift.internal.Migrations

import scala.collection.mutable

package object redshift {

  // See the merge method scala doc for reference
  def assessRedshiftMigration(
                               src: IgluSchema,
                               tgt: IgluSchema
                             ): Either[NonEmptyList[Migrations.Breaking], List[Migrations.NonBreaking]] =
    ShredModel.good(src).merge(ShredModel.good(tgt)).map(_.allMigrations).leftMap(_.errors)

  def isRedshiftMigrationBreaking(src: IgluSchema, tgt: IgluSchema): Boolean =
    assessRedshiftMigration(src, tgt).isRight

  def getFinalMergedModel(schemas: NonEmptyList[(IgluSchema)]): ShredModel =
    foldMapMergeRedshiftSchemas(schemas).values.collectFirst {
      case model: ShredModel if !model.isRecovery => model
    }.get // first schema always would be there due to Nel, so `get` is safe

  /**
   * Build a map between schema key and their models.
   *
   * @param schemas - ordered list of schemas for the same family
   * @return
   */
  def foldMapRedshiftSchemas(schemas: NonEmptyList[IgluSchema]): collection.Map[SchemaKey, ShredModel] = {
    val acc = mutable.Map.empty[SchemaKey, ShredModel]
    var maybeLastGoodModel = Option.empty[GoodModel]
    val models = schemas.map(ShredModel.good)

    // first pass to build the mapping between key and corresponding model
    models.toList.foreach(model => maybeLastGoodModel match {
      case Some(lastModel) => lastModel.merge(model) match {
        case Left(badModel) => acc.update(model.schemaKey, badModel)
        // We map original model here, as opposed to merged one.
        case Right(mergedModel) => acc.update(model.schemaKey, model)
          maybeLastGoodModel = mergedModel.some
      }
      case None =>
        acc.update(model.schemaKey, model)
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
  def foldMapMergeRedshiftSchemas(schemas: NonEmptyList[IgluSchema]): collection.Map[SchemaKey, ShredModel] = {
    val models = schemas.map(ShredModel.good)
    var lastGoodModel = models.head
    val acc: mutable.Map[SchemaKey, ShredModel] = mutable.Map(models.head.schemaKey -> models.head)

    // first pass to build the mapping between key and accumulated model
    models.tail.foreach { model =>
      lastGoodModel.merge(model) match {
        case Left(badModel) =>
          acc.update(model.schemaKey, badModel)
        case Right(mergedModel) =>
          acc.update(mergedModel.schemaKey, mergedModel)
          lastGoodModel = mergedModel
      }
    }

    // seconds pass to backfill the last model version for initial keys.
    acc.map {
      case (k, model) => (k, if (!model.isRecovery)
        lastGoodModel
      else
        model)
    }.toMap
  }
}
