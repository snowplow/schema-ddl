/*
 * Copyright (c) 2016-2022 Snowplow Analytics Ltd. All rights reserved.
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

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Pointer.JsonPointer
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Pointer

object FlatData {

  /**
    * Transform JSON to TSV, where columns order match the table
    * @param data actual JSON data to transform
    * @param source state of schema, providing proper order
    * @param getValue function used to extract a custom type from JSON
    * @param default in case JsonPointer points to a missing key
    */
  def flatten[A](data: Json, source: SchemaList, getValue: Json => A, default: A): List[A] =
    FlatSchema.extractProperties(source).map { case (pointer, _) => getPath(pointer.forData, data, getValue, default) }

  /** Extract data from JSON payload using JsonPointer */
  def getPath[A](pointer: JsonPointer, json: Json, getValue: Json => A, default: A): A = {
    def go(cursor: List[Pointer.Cursor], data: ACursor): A =
      cursor match {
        case Nil =>
          data.focus.map(getValue).getOrElse(default)
        case Pointer.Cursor.DownField(field) :: t =>
          go(t, data.downField(field))
        case Pointer.Cursor.At(i) :: t =>
          go(t, data.downN(i))
        case Pointer.Cursor.DownProperty(_) :: _ =>
          throw new IllegalStateException(s"Iglu Schema DDL tried to use invalid pointer ${pointer.show} for payload ${json.noSpaces}")
      }

    go(pointer.get, json.hcursor)
  }

  /** Example of `getValue` for `flatten`. Makes no difference between empty string and null */
  def getString(escapeString: Option[String => String])(json: Json): String =
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


  def escapeJson(f: String => String)(json: Json): Json =
    json.fold(
      Json.Null,
      Json.fromBoolean,
      Json.fromJsonNumber,
      x => Json.fromString(f(x)),
      x => Json.fromValues(escapeArray(f)(x)),
      x => Json.fromJsonObject(escapeObject(f)(x)))

  def escapeArray(f: String => String)(array: Vector[Json]): Vector[Json] =
    array.map(escapeJson(f))

  def escapeObject(f: String => String)(obj: JsonObject): JsonObject =
    obj.mapValues(escapeJson(f))

  def transformBool(b: Boolean): String = if (b) "1" else "0"
}
