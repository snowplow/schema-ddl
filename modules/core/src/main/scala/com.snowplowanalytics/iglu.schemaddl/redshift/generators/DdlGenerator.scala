/*
 * Copyright (c) 2014-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.iglu.schemaddl.redshift.generators

import scala.annotation.tailrec

import cats.data.NonEmptyList

import com.snowplowanalytics.iglu.core.SchemaMap

import com.snowplowanalytics.iglu.schemaddl.Properties

import com.snowplowanalytics.iglu.schemaddl.redshift._
import com.snowplowanalytics.iglu.schemaddl.redshift.generators.EncodeSuggestions._
import com.snowplowanalytics.iglu.schemaddl.redshift.generators.TypeSuggestions._

import com.snowplowanalytics.iglu.schemaddl.migrations.FlatSchema

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Pointer


/** Generates a Redshift DDL File from a Flattened JsonSchema */
object DdlGenerator {

  /**
   * Make a DDL header from the self-describing info
   *
   * @param schemaMap self-describing info
   * @param schemaName optional schema name
   * @return SQL comment
   */
  def getTableComment(tableName: String, schemaName: Option[String], schemaMap: SchemaMap): CommentOn = {
    val schema = schemaName.map(_ + ".").getOrElse("")
    CommentOn(schema + tableName, schemaMap.schemaKey.toSchemaUri)
  }

  /**
   * Generates Redshift CreateTable object with all columns, attributes and constraints
   *
   * @param orderedSubSchemas subschemas which are ordered wrt to updates, nullness and alphabetic order
   * @param name table name
   * @param dbSchema optional redshift schema name
   * @param raw do not produce any Snowplow specific columns (like root_id)
   * @param size default length for VARCHAR
   * @return CreateTable object with all data about table creation
   */
  def generateTableDdl(orderedSubSchemas: Properties, name: String, dbSchema: Option[String], size: Int, raw: Boolean): CreateTable = {
    val columns = getColumnsDdl(orderedSubSchemas, size)
    if (raw) getRawTableDdl(dbSchema, name, columns)
    else getAtomicTableDdl(dbSchema, name, columns)
  }

  // Columns with data taken from self-describing schema
  private[redshift] val selfDescSchemaColumns = List(
    Column("schema_vendor", RedshiftVarchar(128), Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(NotNull))),
    Column("schema_name", RedshiftVarchar(128), Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(NotNull))),
    Column("schema_format", RedshiftVarchar(128), Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(NotNull))),
    Column("schema_version", RedshiftVarchar(128), Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(NotNull)))
  )

  // Snowplow-specific columns
  private[redshift] val parentageColumns = List(
    Column("root_id", RedshiftChar(36), Set(CompressionEncoding(RawEncoding)), Set(Nullability(NotNull))),
    Column("root_tstamp", RedshiftTimestamp, Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(NotNull))),
    Column("ref_root", RedshiftVarchar(255), Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(NotNull))),
    Column("ref_tree", RedshiftVarchar(1500), Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(NotNull))),
    Column("ref_parent", RedshiftVarchar(255), Set(CompressionEncoding(ZstdEncoding)), Set(Nullability(NotNull)))
  )


  /**
   * Generate DDL for atomic (with Snowplow-specific columns and attributes) table
   *
   * @param dbSchema optional redshift schema name
   * @param name table name
   * @param columns list of generated DDLs for columns
   * @return full CREATE TABLE statement ready to be rendered
   */
  private def getAtomicTableDdl(dbSchema: Option[String], name: String, columns: List[Column]): CreateTable = {
    val schema           = dbSchema.getOrElse("atomic")
    val fullTableName    = schema + "." + name
    val tableConstraints = Set[TableConstraint](RedshiftDdlDefaultForeignKey(schema))
    val tableAttributes  = Set[TableAttribute]( // Snowplow-specific attributes
      Diststyle(Key),
      DistKeyTable("root_id"),
      SortKeyTable(None, NonEmptyList.of("root_tstamp"))
    )
    
    CreateTable(
      fullTableName,
      selfDescSchemaColumns ++ parentageColumns ++ columns,
      tableConstraints,
      tableAttributes
    )
  }

  /**
   * Generate DDL forraw (without Snowplow-specific columns and attributes) table
   *
   * @param dbSchema optional redshift schema name
   * @param name table name
   * @param columns list of generated DDLs for columns
   * @return full CREATE TABLE statement ready to be rendered
   */
  private def getRawTableDdl(dbSchema: Option[String], name: String, columns: List[Column]): CreateTable = {
    val fullTableName = dbSchema.map(_ + "." + name).getOrElse(name)
    CreateTable(fullTableName, columns)
  }

  /**
   * Get DDL for Foreign Key for specified schema
   *
   * @param schemaName Redshift's schema
   * @return ForeignKey constraint
   */
  private def RedshiftDdlDefaultForeignKey(schemaName: String) = {
    val reftable = RefTable(schemaName + ".events", Some("event_id"))
    ForeignKeyTable(NonEmptyList.of("root_id"), reftable)
  }

  /**
   * Processes the Map of Data elements pulled from the JsonSchema and
   * generates DDL object for it with it's name, constrains, attributes
   * data type, etc
   */
  private[schemaddl] def getColumnsDdl(orderedSubSchemas: Properties, varcharSize: Int): List[Column] =
    for {
      (jsonPointer, schema) <- orderedSubSchemas.filter { case (p, _) => !p.equals(Pointer.Root) }
      columnName = FlatSchema.getName(jsonPointer)
      dataType = getDataType(schema, varcharSize, columnName)
      encoding = getEncoding(schema, dataType, columnName)
      constraints = getConstraints(!schema.canBeNull)
    } yield Column(columnName, dataType, columnAttributes = Set(encoding), columnConstraints = constraints)


  // List of data type suggestions
  val dataTypeSuggestions: List[DataTypeSuggestion] = List(
    complexEnumSuggestion,
    productSuggestion,
    timestampSuggestion,
    dateSuggestion,
    arraySuggestion,
    integerSuggestion,
    numberSuggestion,
    booleanSuggestion,
    charSuggestion,
    uuidSuggestion,
    varcharSuggestion
  )

  // List of compression encoding suggestions
  val encodingSuggestions: List[EncodingSuggestion] = List(text255Suggestion, lzoSuggestion, zstdSuggestion)


  /**
   * Takes each suggestion out of ``dataTypeSuggesions`` and decide whether
   * current properties satisfy it, then return the data type
   * If nothing suggested VARCHAR with ``varcharSize`` returned as default
   *
   * @param properties is a string we need to recognize
   * @param varcharSize default size for unhandled properties and strings
   *                    without any data about length
   * @param columnName to produce warning
   * @param suggestions list of functions can recognize encode type
   * @return some format or none if nothing suites
   */
  @tailrec private[schemaddl] def getDataType(
      properties: Schema,
      varcharSize: Int,
      columnName: String,
      suggestions: List[DataTypeSuggestion] = dataTypeSuggestions)
  : DataType = {

    suggestions match {
      case Nil => RedshiftVarchar(varcharSize) // Generic
      case suggestion :: tail => suggestion(properties, columnName) match {
        case Some(format) => format
        case None => getDataType(properties, varcharSize, columnName, tail)
      }
    }
  }

  /**
   * Takes each suggestion out of ``compressionEncodingSuggestions`` and
   * decide whether current properties satisfy it, then return the compression
   * encoding.
   * If nothing suggested ZSTD Encoding returned as default
   *
   * @param properties is a string we need to recognize
   * @param dataType redshift data type for current column
   * @param columnName to produce warning
   * @param suggestions list of functions can recognize encode type
   * @return some format or none if nothing suites
   */
  @tailrec private[schemaddl] def getEncoding(
      properties: Schema,
      dataType: DataType,
      columnName: String,
      suggestions: List[EncodingSuggestion] = encodingSuggestions)
  : CompressionEncoding = {

    suggestions match {
      case Nil => CompressionEncoding(ZstdEncoding) // ZSTD is default for user-generated
      case suggestion :: tail => suggestion(properties, dataType, columnName) match {
        case Some(encoding) => CompressionEncoding(encoding)
        case None => getEncoding(properties, dataType, columnName, tail)
      }
    }
  }

  private[schemaddl] def getConstraints(notNull: Boolean) = {
    if (notNull) Set[ColumnConstraint](Nullability(NotNull))
    else Set.empty[ColumnConstraint]
  }
}
