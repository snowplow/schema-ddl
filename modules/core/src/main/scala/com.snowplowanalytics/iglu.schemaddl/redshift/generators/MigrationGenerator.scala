/*
 * Copyright (c) 2014-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.iglu.schemaddl.redshift
package generators

// Iglu Core
import com.snowplowanalytics.iglu.core._
import com.snowplowanalytics.iglu.schemaddl.migrations.{ Migration, FlatSchema }

// This library
import com.snowplowanalytics.iglu.schemaddl.StringUtils._
import com.snowplowanalytics.iglu.schemaddl.jsonschema._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.CommonProperties
import com.snowplowanalytics.iglu.schemaddl.migrations.SchemaDiff

// This library
import DdlGenerator._

/**
 * Module containing all logic to generate DDL files with information required
 * to migration from one version of Schema to another
 */
object MigrationGenerator {

  /**
   * Generate full ready to be rendered DDL file containing all migration
   * statements and additional data like previous version of table
   *
   * @param migration common JSON Schema migration object with
   *                  path (from-to) and diff
   * @param varcharSize size VARCHARs by default
   * @param tableSchema DB schema for table (atomic by default)
   * @return DDL file containing list of statements ready to be printed
   */
  def generateMigration(migration: Migration, varcharSize: Int, tableSchema: Option[String]): DdlFile = {
    val schemaMap     = SchemaMap(migration.vendor, migration.name, "jsonschema", migration.to)
    val oldSchemaUri  = SchemaMap(migration.vendor, migration.name, "jsonschema", migration.from).schemaKey.toSchemaUri
    val tableName     = getTableName(schemaMap)                            // e.g. com_acme_event_1
    val tableNameFull = tableSchema.map(_ + ".").getOrElse("") + tableName   // e.g. atomic.com_acme_event_1

    val added =
      if (migration.diff.added.nonEmpty)
        migration.diff.added.map {
          case (pointer, schema) =>
            buildAlterTableAdd(tableNameFull, varcharSize, (pointer, schema))
        }
      else Nil

    val modified =
      migration.diff.modified.toList.flatMap {
        case modified if maxLengthIncreased(modified) || enumLonger(modified) =>
          buildAlterTableMaxLength(tableNameFull, varcharSize, modified)
        case _ =>
          None
      }

    val header = getHeader(tableName, oldSchemaUri)
    val comment = CommentOn(tableNameFull, schemaMap.schemaKey.toSchemaUri)

    val statements =
      if (modified.isEmpty && added.isEmpty) List(EmptyAdded, Empty, comment, Empty)
      else if (modified.isEmpty) List(Begin(None, None), Empty) ++ added :+ Empty :+ comment :+ Empty :+ End
      else if (added.isEmpty) modified ++ List(Empty, comment, Empty)
      else (modified :+ Empty) ++ List(Begin(None, None), Empty) ++ added :+ Empty :+ comment :+ Empty :+ End


    DdlFile(List(header, Empty) ++ statements)
  }

  val EmptyAdded = CommentBlock("NO ADDED COLUMNS CAN BE EXPRESSED IN SQL MIGRATION", 3)

  /**
   * Generate comment block for for migration file with information about
   * previous version of table
   *
   * @param tableName name of migrating table
   * @param oldSchemaUri Schema URI extracted from internal database store
   * @return DDL statement with header
   */
  def getHeader(tableName: String, oldSchemaUri: String): CommentBlock =
    CommentBlock(Vector(
      "WARNING: only apply this file to your database if the following SQL returns the expected:",
      "",
      s"SELECT pg_catalog.obj_description(c.oid) FROM pg_catalog.pg_class c WHERE c.relname = '$tableName';",
      " obj_description",
      "-----------------",
      s" $oldSchemaUri",
      " (1 row)"))

  /**
   * Generate single ALTER TABLE statement to add a new property
   *
   * @param tableName name of migrating table
   * @param varcharSize default size for VARCHAR
   * @param pair pair of property name and its Schema properties like
   *             length, maximum, etc
   * @return DDL statement altering single column in table by adding new property
   */
  def buildAlterTableAdd(tableName: String, varcharSize: Int, pair: (Pointer.SchemaPointer, Schema)): AlterTable =
    pair match {
      case (pointer, properties) =>
        val columnName = FlatSchema.getName(pointer)
        val dataType = getDataType(properties, varcharSize, columnName)
        val encoding = getEncoding(properties, dataType, columnName)
        val nullable = if (properties.canBeNull) None else Some(Nullability(NotNull))
        AlterTable(tableName, AddColumn(snakeCase(columnName), dataType, None, Some(encoding), nullable))
    }

  /**
   * Generate single ALTER TABLE statement that increases the length of a varchar in-place
   *
   * @param tableName name of migrating table
   * @param varcharSize default size for VARCHAR
   * @param modified field whose length gets increased
   * @return DDL statement altering single column in table by increasing the sieadding new property
   */
  def buildAlterTableMaxLength(tableName: String, varcharSize: Int, modified: SchemaDiff.Modified): Option[AlterTable] = {
    val columnName = FlatSchema.getName(modified.pointer)
    val dataType = getDataType(modified.to, varcharSize, columnName)
    val encodingFrom = getEncoding(modified.to, dataType, columnName)
    val encodingTo = getEncoding(modified.to, dataType, columnName)
    if (EncodingsForbiddingAlter.contains(encodingFrom.value) || EncodingsForbiddingAlter.contains(encodingTo.value)) None
    else Some(AlterTable(tableName, AlterType(columnName, dataType)))
  }

  /**
   * List of column encodings that don't support length extension
   * @see https://docs.aws.amazon.com/redshift/latest/dg/r_ALTER_TABLE.html
   */
  val EncodingsForbiddingAlter: List[CompressionEncodingValue] =
    List(ByteDictEncoding, RunLengthEncoding, Text255Encoding, Text32KEncoding)

  /** @return true if the field is string and its maxLength got increased */
  private[generators] def maxLengthIncreased(modified: SchemaDiff.Modified): Boolean =
    modified.from.`type`.exists(_.possiblyWithNull(CommonProperties.Type.String)) &&
      modified.to.`type`.exists(_.possiblyWithNull(CommonProperties.Type.String)) &&
      modified.getDelta.maxLength.was.exists { was =>
        modified.getDelta.maxLength.became.exists { became =>
          became.value > was.value
        }
      }

  /** @return true if the field is enum with new value longer than the existing ones */
  private[generators] def enumLonger(modified: SchemaDiff.Modified): Boolean =
    modified.getDelta.enum.was.exists { was =>
      modified.getDelta.enum.became.exists { became =>
        became.value.map(_.asString).collect { case Some(s) => s.length }.max > was.value.map(_.asString).collect { case Some(s) => s.length }.max
      }
    }
}
