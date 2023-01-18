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

import io.circe.{Encoder, Decoder, Json}
import io.circe.syntax._

import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.CommonProperties._

trait CommonCodecs {

  implicit def schemaDecoder: Decoder[Schema]

  implicit val typeDecoder: Decoder[Type] =
    Decoder[String]
      .emap(Type.fromString)
      .or(Decoder[List[String]].emap(Type.fromProduct))

  implicit val descriptionDecoder: Decoder[Description] =
    Decoder[String].map(Description)

  implicit def enumDecoder: Decoder[Enum] =
    Decoder[List[Json]].map(Enum.apply)

  implicit def oneOfDecoder: Decoder[OneOf] =
    Decoder[List[Schema]].map(OneOf.apply)

  implicit def anyOfDecoder: Decoder[AnyOf] =
    Decoder[List[Schema]].map(AnyOf.apply)

  implicit def allOfDecoder: Decoder[AllOf] =
    Decoder[List[Schema]].map(AllOf.apply)

  implicit def notDecoder: Decoder[Not] =
    Decoder[Schema].map(Not.apply)


  implicit def schemaEncoder: Encoder[Schema]

  implicit val typeEncoder: Encoder[Type] =
    Encoder.instance(_.asJson)

  implicit val descriptionEncoder: Encoder[Description] =
    Encoder.instance(_.value.asJson)

  implicit val enumEncoder: Encoder[Enum] =
    Encoder.instance(_.value.asJson)

  implicit def oneOfEncoder: Encoder[OneOf] =
    Encoder.instance(_.value.asJson)

  implicit def anyOfEncoder: Encoder[AnyOf] =
    Encoder.instance(_.value.asJson)

  implicit def allOfEncoder: Encoder[AllOf] =
    Encoder.instance(_.value.asJson)

  implicit def notEncoder: Encoder[Not] =
    Encoder.instance(_.value.asJson)


}
