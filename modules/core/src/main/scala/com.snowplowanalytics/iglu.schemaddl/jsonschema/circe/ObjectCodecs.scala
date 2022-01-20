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
package circe

import io.circe.{Encoder, Decoder, DecodingFailure}
import io.circe.syntax._

import cats.implicits._

import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.ObjectProperty._

trait ObjectCodecs {
  private val PropertiesExpectation = "properties expected to be a map of JSON schemas"
  private val PatternPropertiesExpectation = "patternProperties expected to be a map of JSON schemas"
  private val RequiredExpectation = "required expected to be an array of keys"

  implicit def schemaDecoder: Decoder[Schema]

  implicit def propertiesDecoder: Decoder[Properties] = Decoder.instance { cursor =>
    cursor.value.fold(
      DecodingFailure(s"$PropertiesExpectation, null found", cursor.history).asLeft,
      _ => DecodingFailure(s"$PropertiesExpectation, boolean found", cursor.history).asLeft,
      num => DecodingFailure(s"$PropertiesExpectation, number $num found", cursor.history).asLeft,
      str => DecodingFailure(s"$PropertiesExpectation, string $str found", cursor.history).asLeft,
      arr => DecodingFailure(s"$PropertiesExpectation, string $arr found", cursor.history).asLeft,
      obj =>
        obj.toList
          .traverse[Decoder.Result, (String, Schema)] { case (property, schema) => schema.as[Schema].map((property, _)) }
          .map(m => Properties.apply(m.toMap))
    )
  }

  implicit def additionalPropertiesDecoder: Decoder[AdditionalProperties] = Decoder.instance { cursor =>
    cursor
      .as[Schema]
      .map(AdditionalProperties.AdditionalPropertiesSchema)
      .orElse[DecodingFailure, AdditionalProperties](cursor.as[Boolean].map(AdditionalProperties.AdditionalPropertiesAllowed))
  }

  implicit val requiredDecoder: Decoder[Required] = Decoder.instance { cursor =>
    cursor.value.fold(
      DecodingFailure(s"$RequiredExpectation, null found", cursor.history).asLeft,
      _ => DecodingFailure(s"$RequiredExpectation, boolean found", cursor.history).asLeft,
      num => DecodingFailure(s"$RequiredExpectation, number $num found", cursor.history).asLeft,
      str => DecodingFailure(s"$RequiredExpectation, string $str found", cursor.history).asLeft,
      arr => arr.map(_.as[String]).sequence[Decoder.Result, String].map(_.toList).map(Required.apply),
      _ => DecodingFailure(s"$RequiredExpectation, object found", cursor.history).asLeft
    )
  }

  implicit def patternPropertiesDecoder: Decoder[PatternProperties] = Decoder.instance { cursor =>
    cursor.value.fold(
      DecodingFailure(s"$PatternPropertiesExpectation, null found", cursor.history).asLeft,
      _ => DecodingFailure(s"$PatternPropertiesExpectation, boolean found", cursor.history).asLeft,
      num => DecodingFailure(s"$PatternPropertiesExpectation, number $num found", cursor.history).asLeft,
      str => DecodingFailure(s"$PatternPropertiesExpectation, string $str found", cursor.history).asLeft,
      arr => DecodingFailure(s"$PatternPropertiesExpectation, string $arr found", cursor.history).asLeft,
      obj =>
        obj.toList
          .traverse[Decoder.Result, (String, Schema)] { case (property, schema) => schema.as[Schema].map((property, _)) }
          .map(m => PatternProperties.apply(m.toMap))
    )
  }


  implicit def schemaEncoder: Encoder[Schema]

  implicit def propertiesEncoder: Encoder[Properties] =
    Encoder.instance {
      case Properties(map) => map.asJson
    }

  implicit def additionalPropertiesEncoder: Encoder[AdditionalProperties] =
    Encoder.instance {
      case AdditionalProperties.AdditionalPropertiesAllowed(bool) => bool.asJson
      case AdditionalProperties.AdditionalPropertiesSchema(schema) => schema.asJson
    }

  implicit val requiredEncoder: Encoder[Required] =
    Encoder.instance(_.value.asJson)

  implicit def patternPropertiesEncoder: Encoder[PatternProperties] =
    Encoder.instance {
      case PatternProperties(map) => map.asJson
    }
}
