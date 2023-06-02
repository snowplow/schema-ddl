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
import com.snowplowanalytics.iglu.schemaddl.redshift.internal.{FlatSchema, Migrations}
import ShredModelEntry.ColumnType
import com.snowplowanalytics.iglu.schemaddl.redshift.internal.Migrations._

import math.abs

/**
 * Shredded table model for redshift.
 *
 * Extracting data from json into tsv-ready List[String]
 * Warehouse table representation. "Good" version of this trait also supports migrations.
 *
 * entries   - list of model entries, containing the schema pointers and bottom level sub schemas 
 * schemaKey - schema key of corresponding top level schema
 */
sealed trait ShredModel extends Product with Serializable {
  def entries: List[ShredModelEntry]

  def schemaKey: SchemaKey

  def tableName: String


  final lazy val baseTableName: String = {
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
       |COMMENT ON TABLE $dbSchema.$tableName IS '${schemaKey.toSchemaUri}';
       |""".stripMargin

  final def jsonToStrings(json: Json): List[String] = entries.map(e => e.stringFactory(json))

}

object ShredModel {

  case class GoodModel(entries: List[ShredModelEntry],
                       schemaKey: SchemaKey,
                       migrations: Migrations
                      ) extends ShredModel {

    /**
     * Generates a sql snippet for migration between lower and upper bounds, if no bounds provided migrates from the 
     * first schema in family to the last
     *
     * @param dbSchema        - name of the warehouse schema
     * @param maybeLowerBound - lower bound for schema versions
     * @param maybeUpperBound - upper bound for schema versions
     * @return SQL script for upgrading schema
     */
    def migrationSql(dbSchema: String, maybeLowerBound: Option[SchemaKey] = None, maybeUpperBound: Option[SchemaKey] = None): String = migrations.toSql(tableName, dbSchema, maybeLowerBound, maybeUpperBound)

    def migrationsInTransaction(maybeLowerBound: Option[SchemaKey] = None, maybeUpperBound: Option[SchemaKey] = None): List[ColumnAddition] = migrations.inTransaction(maybeLowerBound, maybeUpperBound)

    def migrationsOutTransaction(maybeLowerBound: Option[SchemaKey] = None, maybeUpperBound: Option[SchemaKey] = None): List[VarcharExtension] = migrations.outTransaction(maybeLowerBound, maybeUpperBound)

    def allMigrations: List[NonBreaking] = migrations.values.toList


    def getMigrationsFor(key: SchemaKey): List[NonBreaking] = migrations.getMigrationsFor(key)

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
      val thisLookup = entries.map { e => (e.columnName, e) }.toMap
      val thatLookup = that.entries.map { e => (e.columnName, e) }.toMap
      val additions: List[ShredModelEntry] =
        that.entries
          .filter(col => !thisLookup.contains(col.columnName))
          .map(entry => (entry.ptr, entry.subSchema))
          .foldLeft(Set.empty[(Pointer.SchemaPointer, Schema)])((acc, s) => acc + s)
          // this fold, toList preserves the order as it was in the older library versions < 0.18.0
          .toList
          .map { case (ptr, subSchema) => ShredModelEntry(ptr, subSchema, isLateAddition = true) }
      val additionsMigration: List[ColumnAddition] = additions.map(ColumnAddition.apply)
      val removals: Either[NonEmptyList[Breaking], List[NonBreaking]] = entries
        .filter(col => !thatLookup.contains(col.columnName))
        .parTraverse {
          case s if !s.isNullable => NullableRequired(s).asLeft.toEitherNel
          case _ => NoChanges.asRight
        }
      val modifications: Either[NonEmptyList[Breaking], List[NonBreaking]] =
        that.entries
          .filter(col => thisLookup.contains(col.columnName))
          .parTraverse(newCol => {
            val oldCol = thisLookup(newCol.columnName)
            val (newType, newNullability, newEncoding) = (newCol.columnType, newCol.isNullable, newCol.compressionEncoding)
            val (oldType, oldNullability, oldEncoding) = (oldCol.columnType, oldCol.isNullable, oldCol.compressionEncoding)
            if (!oldNullability & newNullability)
              NullableRequired(oldCol).asLeft.toEitherNel
            else if (newEncoding != oldEncoding)
              IncompatibleEncoding(oldCol, newCol).asLeft.toEitherNel
            else newType match {
              case ColumnType.RedshiftVarchar(newSize) => oldType match {
                case ColumnType.RedshiftVarchar(oldSize) if newSize > oldSize => VarcharExtension(oldCol, newCol).asRight
                case ColumnType.RedshiftVarchar(oldSize) if newSize <= oldSize => NoChanges.asRight
                case _ => IncompatibleTypes(oldCol, newCol).asLeft.toEitherNel
              }
              case _ if newType == oldType => NoChanges.asRight
              case _ => IncompatibleTypes(oldCol, newCol).asLeft.toEitherNel
            }
          })
      val allChanges: Either[NonEmptyList[Breaking], List[NonBreaking]] = (modifications, removals) match {
        case (Right(x), Right(y)) => (x ++ y).asRight[NonEmptyList[Breaking]]
        case (Right(_), l@Left(_)) => l
        case (l@Left(_), Right(_)) => l
        case (Left(x), Left(y)) => (x ::: y).asLeft[List[NonBreaking]]
      }
      (for {
        changes <- allChanges
        extensions = changes.collect { case e: VarcharExtension => e }
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

    val tableName: String = baseTableName

    private[redshift] def makeRecovery(errors: NonEmptyList[Breaking]): RecoveryModel = new RecoveryModel(entries, schemaKey, errors)
  }

  case class RecoveryModel(entries: List[ShredModelEntry],
                           schemaKey: SchemaKey,
                           errors: NonEmptyList[Breaking]) extends ShredModel {
    def errorAsStrings: NonEmptyList[String] = errors.map(_.report)

    val tableName = s"${baseTableName}_${schemaKey.version.addition}_${schemaKey.version.revision}_recovered_${abs(entries.show.hashCode())}"
  }

  def good(s: IgluSchema): GoodModel = good(s.self.schemaKey, s.schema)

  def good(k: SchemaKey, s: Schema): GoodModel = new GoodModel(FlatSchema.extractProperties(s), k, Migrations.empty(k))

}