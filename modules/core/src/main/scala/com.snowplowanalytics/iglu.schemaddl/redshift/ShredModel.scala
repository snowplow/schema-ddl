package com.snowplowanalytics.iglu.schemaddl.redshift

import cats.data.NonEmptyList
import cats.syntax.parallel._
import cats.syntax.show._
import cats.syntax.either._
import io.circe.Json
import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.schemaddl.IgluSchema
import com.snowplowanalytics.iglu.schemaddl.StringUtils.snakeCase
import com.snowplowanalytics.iglu.schemaddl.jsonschema.{Pointer, Schema}
import com.snowplowanalytics.iglu.schemaddl.redshift.internal.{FlatSchema, Migrations, ShredModelEntry}
import com.snowplowanalytics.iglu.schemaddl.redshift.internal.ShredModelEntry.ColumnType
import com.snowplowanalytics.iglu.schemaddl.redshift.internal.ShredModelEntry.CompressionEncoding.Text255Encoding
import com.snowplowanalytics.iglu.schemaddl.redshift.internal.Migrations._

import math.abs

/**
 * Shredded table model for redshift.
 *
 * @usecase Extracting data from json into tsv-ready List[String]
 * @usecase Warehouse table representation. "Good" version of this trait also supports migrations.
 *
 *          entries   - list of model entries, containing the schema pointers and bottom level sub schemas 
 *          schemaKey - schema key of corresponding top level schema
 */
sealed trait ShredModel extends Product with Serializable {
  private def entries: List[ShredModelEntry] = this match {
    case ShredModel.GoodModel(entries, _, _) => entries
    case ShredModel.RecoveryModel(entries, _, _) => entries
  }

  private def schemaKey: SchemaKey = this match {
    case ShredModel.GoodModel(_, schemaKey, _) => schemaKey
    case ShredModel.RecoveryModel(_, schemaKey, _) => schemaKey
  }

  final def isRecovery: Boolean = this match {
    case _: ShredModel.GoodModel => false
    case _: ShredModel.RecoveryModel => true
  }

  final lazy val tableName: String = if (isRecovery)
    s"${baseTableName}_${schemaKey.version.addition}_${schemaKey.version.revision}_recovered_${abs(entries.show.hashCode())}"
  else
    baseTableName


  final private lazy val baseTableName: String = {
    // Split the vendor's reversed domain name using underscores rather than dots
    val snakeCaseOrganization = schemaKey
      .vendor
      .replaceAll("""\.""", "_")
      .replaceAll("-", "_")
      .toLowerCase

    // Change the name from PascalCase to snake_case if necessary
    val snakeCaseName = snakeCase(schemaKey.name)

    s"${snakeCaseOrganization}_${snakeCaseName}_${schemaKey.version.model}"
  }

  // use this for the loader column expansion
  final def columnNamesQuoted: List[String] = entries.map(e => s""""${e.columnName}"""")

  /**
   *
   * @param dbSchema - name of the warehouse schema
   * @return
   */
  final def toTableSql(dbSchema: String): String =
    s"""CREATE TABLE IF NOT EXISTS $dbSchema.$tableName (
       |${entries.show},
       |  FOREIGN KEY (root_id) REFERENCES $dbSchema.events(event_id)
       |)
       |DISTSTYLE KEY
       |DISTKEY (root_id)
       |SORTKEY (root_tstamp);
       |
       |COMMENT ON TABLE  $dbSchema.$tableName IS '${schemaKey.toSchemaUri}';
       |""".stripMargin

  final def jsonToStrings(json: Json): List[String] = entries.map(e => e.stringFactory(json))

}

object ShredModel {

  case class GoodModel(
                        private[ShredModel] val entries: List[ShredModelEntry],
                        schemaKey: SchemaKey,
                        private val migrations: Migrations
                      ) extends ShredModel {

    /**
     *
     * @param dbSchema     - name of the warehouse schema
     * @param maybeBaseKey - base schema key, that is currently represented in the warehouse, None if schema wasn't 
     *                     created yet.
     * @return SQL script for upgrading schema
     */
    def migrationSql(dbSchema: String, maybeBaseKey: Option[SchemaKey]): String = migrations.toSql(tableName, dbSchema, maybeBaseKey)

    def migrationsInTransaction(maybeBase: Option[SchemaKey]): List[ColumnAddition] = migrations.inTransaction(maybeBase)

    def migrationsOutTransaction(maybeBase: Option[SchemaKey]): List[VarcharExtension] = migrations.outTransaction(maybeBase)

    def allMigrations: List[NonBreaking] = migrations.values.toList

    /**
     * Merge two good models, evaluating feasibility of this merge and updating migrations.  
     * Change vector could be column additions or varchar size expansion.
     *
     * @param that next schema model in the family, that would merge on top of this
     * @return either 
     *         Left ModelShred of the that schema tupled with non emtpy list of breaking changes that prevented the merge
     *         Right merged ModelShred of this with that schema tupled with list of non breaking changes required to make a
     *         perform a merge.
     */
    def merge(that: GoodModel): Either[RecoveryModel, GoodModel] = {
      val baseLookup = entries.map { e => (e.columnName, e) }.toMap
      val additions: List[ShredModelEntry] =
        that.entries
          .filter(col => !baseLookup.contains(col.columnName))
          .map(entry => (entry.ptr, entry.subSchema))
          .toSet[(Pointer.SchemaPointer, Schema)] // this toSet, toList preserves the order as it was in the older library versions < 0.18.0
          .toList
          .map { case (ptr, subSchema) => ShredModelEntry(ptr, subSchema, isLateAddition = true) }
      val additionsMigration: List[ColumnAddition] = additions.map(ColumnAddition.apply)
      val modifications: Either[NonEmptyList[Breaking], List[NonBreaking]] =
        that.entries
          .filter(col => baseLookup.contains(col.columnName))
          .parTraverse(newCol => {
            val oldCol = baseLookup(newCol.columnName)
            val (newType, newNullability, newEncoding) = (newCol.columnType, newCol.isNullable, newCol.compressionEncoding)
            val (oldType, oldNullability, oldEncoding) = (oldCol.columnType, oldCol.isNullable, oldCol.compressionEncoding)
            if (!oldNullability & newNullability)
              NullableRequired(oldCol).asLeft.toEitherNel
            else if (newEncoding != oldEncoding)
              IncompatibleEncoding(oldCol, newCol).asLeft.toEitherNel
            else newType match {
              case ColumnType.RedshiftVarchar(newSize) => oldType match {
                case ColumnType.RedshiftVarchar(oldSize) if (newSize > oldSize) & (oldEncoding != Text255Encoding) =>
                  VarcharExtension(oldCol, newCol).asRight
                case _ if newType != oldType => IncompatibleTypes(oldCol, newCol).asLeft.toEitherNel
                case _ => NoChanges.asRight
              }
              case _ => NoChanges.asRight
            }
          })

      (for {
        mods <- modifications
        extensions = mods.collect { case e: VarcharExtension => e }
        modifedEntries = entries.map(
          entry => extensions.collectFirst {
            case s if s.old == entry => s.newEntry
          }.getOrElse(entry)
        )
      } yield GoodModel(
        modifedEntries ++ additions,
        that.schemaKey,
        migrations ++ Migrations(that.schemaKey, extensions ++ additionsMigration)
      ))
        .leftMap(that.makeRecovery)
    }

    private[redshift] def makeRecovery(errors: NonEmptyList[Breaking]): RecoveryModel = RecoveryModel(entries, schemaKey, errors)
  }

  case class RecoveryModel(private[ShredModel] val entries: List[ShredModelEntry],
                           schemaKey: SchemaKey,
                           errors: NonEmptyList[Breaking]) extends ShredModel {
    def errorAsStrings: NonEmptyList[String] = errors.map(_.report)
  }


  def good(s: IgluSchema): GoodModel = good(s.self.schemaKey, s.schema)

  def good(k: SchemaKey, s: Schema): GoodModel = GoodModel(FlatSchema.extractProperties(s), k, Migrations.empty(k))

}