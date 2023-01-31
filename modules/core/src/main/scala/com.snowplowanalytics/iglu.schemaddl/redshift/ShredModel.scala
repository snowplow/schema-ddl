package com.snowplowanalytics.iglu.schemaddl.redshift

import cats.data.{EitherNel, NonEmptyList}
import cats.syntax.parallel._
import cats.syntax.show._
import cats.syntax.either._
import io.circe.Json
import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.schemaddl.StringUtils.snakeCase
import com.snowplowanalytics.iglu.schemaddl.redshift.internal.ShredModelEntry
import com.snowplowanalytics.iglu.schemaddl.redshift.internal.ShredModelEntry.ColumnType
import com.snowplowanalytics.iglu.schemaddl.redshift.internal.ShredModelEntry.CompressionEncoding.Text255Encoding

case class ShredModel(entries: List[ShredModelEntry], lastSchemaKey: SchemaKey) {
  private def tableName(isRecovery: Boolean): String = if (isRecovery)
    s"${getBaseTableName}_${lastSchemaKey.version.addition}_${lastSchemaKey.version.revision}_recovered_${entries.hashCode()}"
  else
    getBaseTableName

  private def getBaseTableName: String = {
    // Split the vendor's reversed domain name using underscores rather than dots
    val snakeCaseOrganization = lastSchemaKey
      .vendor
      .replaceAll("""\.""", "_")
      .replaceAll("-", "_")
      .toLowerCase

    // Change the name from PascalCase to snake_case if necessary
    val snakeCaseName = snakeCase(lastSchemaKey.name)

    s"${snakeCaseOrganization}_${snakeCaseName}_${lastSchemaKey.version.model}"
  }


  def toSql(dbSchema: String, isRecovery: Boolean): String =
    s"""CREATE TABLE IF NOT EXISTS $dbSchema.${tableName(isRecovery)} (
       |  ${entries.show},
       |  FOREIGN KEY (root_id) REFERENCES atomic.events(event_id)
       |)
       |DISTSTYLE KEY
       |DISTKEY (root_id)
       |SORTKEY (root_tstamp);
       |
       |COMMENT ON TABLE  $dbSchema.${tableName(isRecovery)} IS '${lastSchemaKey.toSchemaUri};'
       |""".stripMargin


  def merge(that: ShredModel) = {
    val baseLookup = entries.groupBy(_.columnName).mapValues(_.head)
    val additions: List[ShredModelEntry] =
      that.entries
        .filter(col => !baseLookup.contains(col.columnName))
        .map(entry => (entry.ptr, entry.subSchema))
        .toSet // this toSet, toList preserves the order as it was in the older library versions < 0.18.0
        .toList
        .map(tup => ShredModelEntry(tup._1, tup._2))
    val additionsMigration: List[ShredModel.ColumnAddition] =
      additions
        .map(ShredModel.ColumnAddition.apply)
    val modifications: Either[NonEmptyList[ShredModel.Breaking], List[ShredModel.NonBreaking]] =
      that.entries
        .filter(col => baseLookup.contains(col.columnName))
        .parTraverse(newCol => {
          val oldCol = baseLookup(newCol.columnName)
          val (newType, newNullability, newEncoding) = (newCol.columnType, newCol.nullability, newCol.compressionEncoding)
          val (oldType, oldNullability, oldEncoding) = (oldCol.columnType, oldCol.nullability, oldCol.compressionEncoding)
          if (oldNullability.isNarrowing(newNullability))
            ShredModel.NullableRequired(oldCol).asLeft.toEitherNel
          else if (newEncoding != oldEncoding)
            ShredModel.IncompatibleEncoding(oldCol, newCol).asLeft.toEitherNel
          else newType match {
            case ColumnType.RedshiftVarchar(newSize) => oldType match {
              case ColumnType.RedshiftVarchar(oldSize) if (newSize > oldSize) & (oldEncoding != Text255Encoding) =>
                ShredModel.VarcharExtension(oldCol, newCol).asRight
              case _ if newType != oldType => ShredModel.IncompatibleTypes(oldCol, newCol).asLeft.toEitherNel
              case _ => ShredModel.NoChanges.asRight
            }
          }
        })

    for {
      mods <- modifications
      extensions = mods.collect { case e: ShredModel.VarcharExtension => e }
      modifedEntries = entries.map(
        entry => extensions.collectFirst {
          case s if s.old == entry => s.newEntry
        }.getOrElse(entry)
      )
    } yield (modifedEntries ++ additions, extensions ++ additionsMigration)
  }


  def tsvFactory(json: Json): EitherNel[NonEmptyList[FactoryError], List[String]] =
    entries.parTraverse(e => e.stringFactory(json).toEitherNel).toEitherNel

}

object ShredModel {
  sealed trait NonBreaking extends Product with Serializable {
    def asSql(tableName: String): String
  }

  case class VarcharExtension(old: ShredModelEntry, newEntry: ShredModelEntry) extends NonBreaking {
    override def asSql(tableName: String): String = s"""  ALTER TABLE $tableName\n     ALTER COLUMN "${old.columnName}" TYPE ${newEntry.columnType.show};"""
  }

  case class ColumnAddition(column: ShredModelEntry) extends NonBreaking {
    override def asSql(tableName: String): String = s"""  ALTER TABLE $tableName\n     ADD COLUMN "${column.columnName}" ${column.columnType.show} ${column.compressionEncoding.show};"""
  }

  private case object NoChanges extends NonBreaking {
    override def asSql(tableName: String): String = ""
  }

  sealed trait Breaking extends Product with Serializable

  case class IncompatibleTypes(old: ShredModelEntry, changed: ShredModelEntry) extends Breaking

  case class IncompatibleEncoding(old: ShredModelEntry, changed: ShredModelEntry) extends Breaking

  case class NullableRequired(old: ShredModelEntry) extends Breaking

}