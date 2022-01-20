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
package com.snowplowanalytics.iglu.schemaddl.jsonschema
package json4s

import org.json4s._

object implicits {

  import com.snowplowanalytics.iglu.schemaddl.jsonschema.json4s.Formats.allFormats

  /**
    * Type class instance allowing to convert json4s JValue
    * into JSON Schema class
    *
    * So far this is single implementation, but still need
    * to be imported into scope to get Schema.parse method work
    */
  implicit lazy val json4sToSchema: ToSchema[JValue] = new ToSchema[JValue] {
    def parse(json: JValue): Option[Schema] =
      json match {
        case _: JObject =>
          val mf = implicitly[Manifest[Schema]]
          Some(json.extract[Schema](allFormats, mf))
        case _          => None
      }
  }

  /**
    * Type class instance allowing to convert `Schema` to JValue
    *
    * So far this is single implementation, but still need
    * to be imported into scope to get Schema.parse method work
    */
  implicit lazy val json4sFromSchema: FromSchema[JValue] = new FromSchema[JValue] {
    def normalize(schema: Schema): JValue =
      Extraction.decompose(schema)
  }
}
