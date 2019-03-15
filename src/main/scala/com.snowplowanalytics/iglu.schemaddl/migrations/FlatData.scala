/*
 * Copyright (c) 2016-2019 Snowplow Analytics Ltd. All rights reserved.
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

import io.circe.{ACursor, Json, JsonObject}

import cats.syntax.show._

import com.snowplowanalytics.iglu.schemaddl.migrations.Migration.OrderedSchemas
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Pointer.{JsonPointer, SchemaPointer}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.{Pointer, Schema}

object FlatData {

  /**
    * Transform JSON to TSV, where columns order match the table
    * @param data actual JSON data to transform
    * @param source state of schema, providing proper order
    * @param escape function used to escape string values, e.g. to fix newlines
    */
  def flatten(data: Json, source: OrderedSchemas, escape: Option[String => String]): List[String] =
    get(source).map(_._1.forData).map(pointer => getPath(pointer, data, escape))


  def get(source: OrderedSchemas): List[(SchemaPointer, Schema)] = {
    val origin = FlatSchema.build(source.schemas.head.schema)
    val originColumns = FlatSchema.order(origin.subschemas)
    val addedColumns = Migration.buildMigration(source).diff.added
    originColumns ++ addedColumns
  }

  /** Extract data from JSON payload using JsonPointer */
  def getPath(pointer: JsonPointer, json: Json, escape: Option[String => String]): String = {
    def go(cursor: List[Pointer.Cursor], data: ACursor): String =
      cursor match {
        case Nil =>
          data.focus.map(getString(escape)).getOrElse("")
        case Pointer.Cursor.DownField(field) :: t =>
          go(t, data.downField(field))
        case Pointer.Cursor.At(i) :: t =>
          go(t, data.downN(i))
        case Pointer.Cursor.DownProperty(_) :: _ =>
          throw new IllegalStateException(s"Iglu Schema DDL tried to use invalid pointer ${pointer.show} for payload ${json.noSpaces}")
      }

    go(pointer.get, json.hcursor)
  }

  private def getString(escapeString: Option[String => String])(json: Json): String =
    escapeString match {
      case None => json.fold("", transformBool, _ => json.show, identity, _ => json.noSpaces, _ => json.noSpaces)
      case Some(f) =>
        json.fold("",
          transformBool,
          _ => json.show,
          f,
          a => Json.fromValues(escapeArray(f)(a)).noSpaces,
          o => Json.fromJsonObject(escapeObject(f)(o)).noSpaces)
    }


  private def escapeJson(f: String => String)(json: Json): Json =
    json.fold(
      Json.Null,
      Json.fromBoolean,
      Json.fromJsonNumber,
      x => Json.fromString(f(x)),
      x => Json.fromValues(escapeArray(f)(x)),
      x => Json.fromJsonObject(escapeObject(f)(x)))

  private def escapeArray(f: String => String)(array: Vector[Json]): Vector[Json] =
    array.map(escapeJson(f))


  private def escapeObject(f: String => String)(obj: JsonObject): JsonObject =
    obj.mapValues(escapeJson(f))


  private def transformBool(b: Boolean): String = if (b) "1" else "0"
}
