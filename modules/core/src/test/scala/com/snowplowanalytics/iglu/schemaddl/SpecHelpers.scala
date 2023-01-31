/*
 * Copyright (c) 2021-2023 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.iglu.schemaddl

import cats.syntax.either._
import cats.syntax.show._
import com.snowplowanalytics.iglu.schemaddl.redshift.internal.FlatSchema
import io.circe.Json
import io.circe.parser.{parse => parseJson}
import jsonschema.{Pointer, Schema}
import jsonschema.circe.implicits._

object SpecHelpers {
  def parseSchema(string: String): Schema =
    parseJson(string)
      .leftMap(_.show)
      .flatMap(json => Schema.parse[Json](json).toRight("SpecHelpers.parseSchema received invalid JSON Schema"))
      .fold(s => throw new RuntimeException(s), identity)

  def extractOrder(orderedSubSchemas: Properties): List[String] =
    orderedSubSchemas.map {
      case (p, _) => FlatSchema.getName(p)
    }

  implicit class JsonOps(json: Json) {
    def schema: Schema =
      Schema.parse(json).getOrElse(throw new RuntimeException("SpecHelpers.parseSchema received invalid JSON Schema"))
  }

  implicit class StringOps(str: String) {
    def jsonPointer: Pointer.SchemaPointer =
      Pointer.parseSchemaPointer(str).fold(x => x, x => x)
  }
}
