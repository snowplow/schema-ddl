/*
 * Copyright (c) 2016-2023 Snowplow Analytics Ltd. All rights reserved.
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

import cats.syntax.either._

import io.circe.{Encoder, Decoder, DecodingFailure}
import io.circe.syntax._

import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.ArrayProperty._

trait ArrayCodecs {

  // Delete after 2.12 dropped
  private[circe] val unusedImportHack1 = "".asRight

  implicit def schemaDecoder: Decoder[Schema]

  implicit def itemsDecoder: Decoder[Items] = Decoder.instance { cursor =>
    cursor
      .as[List[Schema]]
      .map(Items.TupleItems)
      .orElse[DecodingFailure, Items](cursor.as[Schema].map(Items.ListItems))
  }

  implicit def additionalItemsDecoder: Decoder[AdditionalItems] = Decoder.instance { cursor =>
    cursor
      .as[Schema]
      .map(AdditionalItems.AdditionalItemsSchema.apply)
      .orElse[DecodingFailure, AdditionalItems](cursor.as[Boolean].map(AdditionalItems.AdditionalItemsAllowed))
  }

  implicit val maxItemsDecoder: Decoder[MaxItems] = Decoder.instance { cursor =>
    cursor
      .value
      .asNumber
      .flatMap(_.toBigInt)
      .toRight(DecodingFailure("maxItems expected to be a natural number", cursor.history))
      .map(MaxItems.apply)
  }

  implicit val minItemsDecoder: Decoder[MinItems] = Decoder.instance { cursor =>
    cursor
      .value
      .asNumber
      .flatMap(_.toBigInt)
      .toRight(DecodingFailure("minItems expected to be a natural number", cursor.history))
      .map(MinItems.apply)
  }


  implicit def schemaEncoder: Encoder[Schema]

  implicit def itemsEncoder: Encoder[Items] =
    Encoder.instance {
      case Items.ListItems(schema) => schema.asJson
      case Items.TupleItems(schemas) => schemas.asJson
    }

  implicit def additionalItemsEncoder: Encoder[AdditionalItems] =
    Encoder.instance {
      case AdditionalItems.AdditionalItemsSchema(schema) => schema.asJson
      case AdditionalItems.AdditionalItemsAllowed(bool) => bool.asJson
    }

  implicit val maxItemsEncoder: Encoder[MaxItems] =
    Encoder.instance {
      case MaxItems(num) => num.asJson
    }

  implicit val minItemsEncoder: Encoder[MinItems] =
    Encoder.instance {
      case MinItems(num) => num.asJson
    }
}
