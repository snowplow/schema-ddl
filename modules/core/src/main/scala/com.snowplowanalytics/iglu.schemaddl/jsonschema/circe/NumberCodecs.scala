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

import cats.syntax.either._

import io.circe.{Encoder, Decoder, HCursor, DecodingFailure}
import io.circe.syntax._

import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.NumberProperty._

trait NumberCodecs {

  // Delete after 2.12 dropped
  private[circe] val unusedImportHack2 = "".asRight

  implicit val multipleOfSerializer: Decoder[MultipleOf] =
    Decoder.instance { cursor: HCursor =>
      cursor.as[BigInt].map(MultipleOf.IntegerMultipleOf)
        .orElse(cursor.as[BigDecimal].map(MultipleOf.NumberMultipleOf))
    }

  implicit val minimumSerializer: Decoder[Minimum] =
    Decoder.instance { cursor: HCursor =>
      val jsonNumber = cursor.value.asNumber
      val integer = jsonNumber
        .flatMap(_.toBigInt)
        .map(Minimum.IntegerMinimum)
      integer.orElse(jsonNumber
        .flatMap(_.toBigDecimal)
        .map(Minimum.NumberMinimum))
        .toRight(DecodingFailure("minimum expected to be a numeric value", cursor.history))
    }

  implicit val maximumSerializer: Decoder[Maximum] =
    Decoder.instance { cursor: HCursor =>
      val jsonNumber = cursor.value.asNumber
      val integer = jsonNumber
        .flatMap(_.toBigInt)
        .map(Maximum.IntegerMaximum)
      integer.orElse(jsonNumber
        .flatMap(_.toBigDecimal)
        .map(Maximum.NumberMaximum))
        .toRight(DecodingFailure("maximum expected to be a numeric value", cursor.history))
    }


  implicit val multipleOfEncoder: Encoder[MultipleOf] =
    Encoder.instance {
      case MultipleOf.NumberMultipleOf(num) => num.asJson
      case MultipleOf.IntegerMultipleOf(num) => num.asJson
    }

  implicit val minimumEncoder: Encoder[Minimum] =
    Encoder.instance {
      case Minimum.NumberMinimum(num) => num.asJson
      case Minimum.IntegerMinimum(num) => num.asJson
    }

  implicit val maximumEncoder: Encoder[Maximum] =
    Encoder.instance {
      case Maximum.NumberMaximum(num) => num.asJson
      case Maximum.IntegerMaximum(num) => num.asJson
    }
}
