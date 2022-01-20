/*
 * Copyright (c) 2014-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.iglu.schemaddl.redshift.generators

import io.circe.literal._

import com.snowplowanalytics.iglu.schemaddl.redshift._

import com.snowplowanalytics.iglu.schemaddl.SpecHelpers._

// specs2
import org.specs2.mutable.Specification

class TypeSuggestionsSpec extends Specification {
  "getDataType" should {
    "suggest decimal for multipleOf == 0.01" in {
      val props = json"""{"type": "number", "multipleOf": 0.01}""".schema
      DdlGenerator.getDataType(props, 16, "somecolumn") must beEqualTo(RedshiftDecimal(Some(36), Some(2)))
    }
    "suggest integer for multipleOf == 1" in {
      val props = json"""{"type": "number", "multipleOf": 1}""".schema
      DdlGenerator.getDataType(props, 16, "somecolumn") must beEqualTo(RedshiftInteger)
    }
    "handle invalid enum" in {
      val props = json"""{"type": "integer", "multipleOf": 1, "enum": [2,3,5,"hello",32]}""".schema
      DdlGenerator.getDataType(props, 16, "somecolumn") must beEqualTo(RedshiftVarchar(7))
    }
    "recognize string,null maxLength == minLength as CHAR" in {
      val props = json"""{"type": ["string","null"], "minLength": "12", "maxLength": "12"}""".schema
      DdlGenerator.getDataType(props, 16, "somecolumn") must beEqualTo(RedshiftChar(12))
    }
    "recognize number with product type" in {
      val props = json"""{"type": ["number","null"]}""".schema
      DdlGenerator.getDataType(props, 16, "somecolumn") must beEqualTo(RedshiftDouble)
    }
    "recognize integer with product type" in {
      val props = json"""{"type": ["integer","null"]}""".schema
      DdlGenerator.getDataType(props, 16, "somecolumn") must beEqualTo(RedshiftBigInt)
    }
    "recognize timestamp" in {
      val props = json"""{"type": "string", "format": "date-time"}""".schema
      DdlGenerator.getDataType(props, 16, "somecolumn") must beEqualTo(RedshiftTimestamp)
    }
    "recognize full date" in {
      val props = json"""{"type": "string", "format": "date"}""".schema
      DdlGenerator.getDataType(props, 16, "somecolumn") must beEqualTo(RedshiftDate)
    }
    "recogninze numbers bigger than Long.MaxValue" in {
      val props = json"""{"type": "integer", "maximum": 9223372036854775808}""".schema
      DdlGenerator.getDataType(props, 16, "somecolumn") must beEqualTo(RedshiftBigInt)
    }
    "fallback to VARCHAR(65535) for arrays" in {
      val props = json"""{"type": "array"}""".schema
      DdlGenerator.getDataType(props, 16, "somecolumn") must beEqualTo(RedshiftVarchar(65535))
    }
  }
}
