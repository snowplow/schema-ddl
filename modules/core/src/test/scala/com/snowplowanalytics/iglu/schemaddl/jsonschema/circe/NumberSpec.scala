/*
 * Copyright (c) 2012-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.iglu.schemaddl.jsonschema.circe

// circe
import io.circe.literal._

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema

import implicits._

// specs2
import org.specs2.Specification

class NumberSpec extends Specification { def is = s2"""
  Check JSON Schema number-specific properties
    correctly transform big BigInt to BigDecimal $e1
    correctly transform small BigInt to BigDecimal $e2
    correctly extract and compare number and integer equal values $e3
    don't extract non-numeric values (null) $e4
  """

  def e1 = {
    val json = json"""{ "maximum": 9223372036854775807 } """

    Schema.parse(json).flatMap(_.maximum).map(_.getAsDecimal) must beSome(BigDecimal(9223372036854775807L))
  }

  def e2 = {
    val json = json"""{ "minimum": -9223372036854775806 } """

    Schema.parse(json).flatMap(_.minimum).map(_.getAsDecimal) must beSome(BigDecimal(-9223372036854775806L))
  }

  def e3 = {
    val json =
      json"""{
          "minimum": 25,
          "maximum": 25.0
        }"""

    val minimum = Schema.parse(json).flatMap(_.minimum).map(_.getAsDecimal).get
    val maximum = Schema.parse(json).flatMap(_.maximum).map(_.getAsDecimal).get

    minimum must beEqualTo(maximum)
  }

  def e4 = {
    val json = json"""{ "minimum": null } """
    val minimum = Schema.parse(json).flatMap(_.minimum).map(_.getAsDecimal)

    minimum must beNone
  }

}
