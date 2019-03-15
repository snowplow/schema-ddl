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

import io.circe.{ACursor, Json}

import cats.syntax.show._

import com.snowplowanalytics.iglu.schemaddl.migrations.Migration.OrderedSchemas
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Pointer.{JsonPointer, SchemaPointer}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.{Pointer, Schema}

object FlatData {

  /** Transform JSON to TSV, where columns order match the table */
  def flatten(data: Json, source: OrderedSchemas): List[String] =
    get(source).map(_._1.forData).map(pointer => getPath(pointer, data))


  def get(source: OrderedSchemas): List[(SchemaPointer, Schema)] = {
    val origin = FlatSchema.build(source.schemas.head.schema)
    val originColumns = FlatSchema.order(origin.subschemas)
    val addedColumns = Migration.buildMigration(source).diff.added
    originColumns ++ addedColumns
  }

  /** Extract data from JSON payload using JsonPointer */
  def getPath(pointer: JsonPointer, json: Json): String = {
    def go(cursor: List[Pointer.Cursor], data: ACursor): String =
      cursor match {
        case Nil =>
          data.focus.map(getString).getOrElse("")
        case Pointer.Cursor.DownField(field) :: t =>
          go(t, data.downField(field))
        case Pointer.Cursor.At(i) :: t =>
          go(t, data.downN(i))
        case Pointer.Cursor.DownProperty(_) :: _ =>
          throw new IllegalStateException(s"Iglu Schema DDL tried to use invalid pointer ${pointer.show} for payload ${json.noSpaces}")
      }

    go(pointer.get, json.hcursor)
  }

  private def getString(json: Json): String =
    json.fold("", transformBool, _ => json.show, identity, _ => json.noSpaces, _ => json.noSpaces)

  private def transformBool(b: Boolean): String = if (b) "1" else "0"
}
