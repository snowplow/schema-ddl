/*
 * Copyright (c) 2016-2018 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.iglu.schemaddl.jsonschema
package circe

import io.circe.{ Decoder, Encoder, Json }
import io.circe.syntax._
import io.circe.generic.semiauto._

trait implicits extends StringCodecs with NumberCodecs with ObjectCodecs with ArrayCodecs with CommonCodecs {
  implicit lazy val schemaDecoder: Decoder[Schema] = deriveDecoder[Schema]
  implicit lazy val schemaEncoder: Encoder[Schema] = deriveEncoder[Schema]

  implicit lazy val toSchema: ToSchema[Json] =
    (json: Json) => json.as[Schema].toOption

  implicit lazy val fromSchema: FromSchema[Json] =
    (schema: Schema) => schema.asJson
}

object implicits extends implicits
