package com.snowplowanalytics.iglu.schemaddl.redshift.internal

import cats.syntax.show._
import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.schemaddl.redshift.ShredModelEntry
import com.snowplowanalytics.iglu.schemaddl.redshift.internal.Migrations._
import com.snowplowanalytics.iglu.schemaddl.redshift.ShredModelEntry.ColumnType._


private[redshift] case class Migrations(private[Migrations] val migrations: List[(SchemaKey, List[Migrations.NonBreaking])]) {

  def values: Iterable[NonBreaking] = migrations.flatMap(_._2)

  def getMigrationsFor(key: SchemaKey): List[NonBreaking] = migrations.collectFirst {
    case (k, nonBreaking) if k == key => nonBreaking
  }.get

  def inTransaction(maybeExclLowerBound: Option[SchemaKey], maybeIncUpperBound: Option[SchemaKey] = None): List[Migrations.ColumnAddition] =
    migrations
      .reverse
      .dropWhile { case (k, _) => maybeIncUpperBound.getOrElse(migrations.last._1) != k }
      .takeWhile { case (k, _) => maybeExclLowerBound.getOrElse(migrations.head._1) != k }
      .reverse
      .flatMap { case (_, a) => a }
      .collect { case a: Migrations.ColumnAddition => a }

  def outTransaction(maybeExclLowerBound: Option[SchemaKey], maybeIncUpperBound: Option[SchemaKey] = None): List[Migrations.VarcharExtension] =
    migrations
      .reverse
      .dropWhile { case (k, _) => maybeIncUpperBound.getOrElse(migrations.last._1) != k }
      .takeWhile { case (k, _) => maybeExclLowerBound.getOrElse(migrations.head._1) != k }
      .reverse
      .flatMap { case (_, a) => a }
      .collect { case a: Migrations.VarcharExtension => a }

  def toSql(tableName: String, dbSchema: String, maybeExclLowerBound: Option[SchemaKey] = None, maybeIncUpperBound: Option[SchemaKey] = None): String =
    s"""|-- WARNING: only apply this file to your database if the following SQL returns the expected:
        |--
        |-- SELECT pg_catalog.obj_description(c.oid) FROM pg_catalog.pg_class c WHERE c.relname = '$tableName';
        |--  obj_description
        |-- -----------------
        |--  ${maybeExclLowerBound.getOrElse(migrations.head._1).toSchemaUri}
        |--  (1 row)
        |
        |""".stripMargin +
      outTransaction(maybeExclLowerBound, maybeIncUpperBound).map { case Migrations.VarcharExtension(old, newEntry) =>
        s"""  ALTER TABLE $dbSchema.$tableName
           |    ALTER COLUMN "${old.columnName}" TYPE ${newEntry.columnType.show};
           |""".stripMargin
      }.mkString +
      (inTransaction(maybeExclLowerBound, maybeIncUpperBound).map { case Migrations.ColumnAddition(column) =>
        s"""  ALTER TABLE $dbSchema.$tableName
           |    ADD COLUMN "${column.columnName}" ${column.columnType.show} ${column.compressionEncoding.show};
           |""".stripMargin
      } match {
        case Nil => s"""|
                        |-- NO ADDED COLUMNS CAN BE EXPRESSED IN SQL MIGRATION
                        |
                        |COMMENT ON TABLE $dbSchema.$tableName IS '${maybeIncUpperBound.getOrElse(migrations.last._1).toSchemaUri}';
                        |""".stripMargin
        case h :: t => s"""|
                          |BEGIN TRANSACTION;
                           |
                           |${(h :: t).mkString}
                           |  COMMENT ON TABLE $dbSchema.$tableName IS '${maybeIncUpperBound.getOrElse(migrations.last._1).toSchemaUri}';
                           |
                           |END TRANSACTION;""".stripMargin
      })

  def ++(that: Migrations): Migrations = Migrations(migrations ++ that.migrations)
}

object Migrations {

  def empty(k: SchemaKey): Migrations = Migrations(k, Nil)

  def apply(schemaKey: SchemaKey, migrations: List[Migrations.NonBreaking]): Migrations =
    Migrations(List((schemaKey, migrations)))

  implicit val ord: Ordering[SchemaKey] = SchemaKey.ordering

  sealed trait NonBreaking extends Product with Serializable

  case class VarcharExtension(old: ShredModelEntry, newEntry: ShredModelEntry) extends NonBreaking

  case class ColumnAddition(column: ShredModelEntry) extends NonBreaking

  case object NoChanges extends NonBreaking

  sealed trait Breaking extends Product with Serializable {
    def report: String = this match {
      case IncompatibleTypes(old, changed) =>
        s"Incompatible types in column ${old.columnName} old ${old.columnType} new ${changed.columnType}"
      case IncompatibleEncoding(old, changed) =>
        s"Incompatible encoding in column ${old.columnName} old type ${old.columnType}/${old.compressionEncoding} new type ${changed.columnType}/${changed.compressionEncoding}"
      case NullableRequired(old) => s"Making required column nullable ${old.columnName}"
    }
  }

  case class IncompatibleTypes(old: ShredModelEntry, changed: ShredModelEntry) extends Breaking

  case class IncompatibleEncoding(old: ShredModelEntry, changed: ShredModelEntry) extends Breaking

  case class NullableRequired(old: ShredModelEntry) extends Breaking
}