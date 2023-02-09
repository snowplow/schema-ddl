/*
 * Copyright (c) 2014-2023 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.iglu.schemaddl.redshift.internal

import com.snowplowanalytics.iglu.schemaddl.SpecHelpers._
import com.snowplowanalytics.iglu.schemaddl.redshift.internal.ShredModelEntrySpec.{dummyPtr, refJson}
import io.circe.literal._
import cats.syntax.show._

// specs2
import org.specs2.mutable.Specification
import ShredModelEntry.ColumnType._
import ShredModelEntry.CompressionEncoding._

class ShredModelEntrySpec extends Specification {

  "type inference" should {
    "suggest decimal for multipleOf == 0.01" in {
      val props = json"""{"type": "number", "multipleOf": 0.01}""".schema
      ShredModelEntry(dummyPtr, props).columnType must beEqualTo(RedshiftDecimal(Some(36), Some(2)))
    }
    "suggest integer for multipleOf == 1" in {
      val props = json"""{"type": "number", "multipleOf": 1}""".schema
      ShredModelEntry(dummyPtr, props).columnType must beEqualTo(RedshiftInteger)
    }
    "handle string" in {
      val props = json"""{"type": "string"}""".schema
      ShredModelEntry(dummyPtr, props).columnType must beEqualTo(RedshiftVarchar(ShredModelEntry.VARCHAR_SIZE))
    }
    "handle string with maxLength" in {
      val props = json"""{"type": "string", "maxLength": 42}""".schema
      ShredModelEntry(dummyPtr, props).columnType must beEqualTo(RedshiftVarchar(42))
    }
    "handle string with enum" in {
      val props = json"""{"type": "string", "enum": ["one", "two"]}""".schema
      ShredModelEntry(dummyPtr, props).columnType must beEqualTo(RedshiftVarchar(3))
    }
    "handle string with enum and maxLength" in {
      val props = json"""{"type": "string", "enum": ["one", "two"], "maxLength": 42}""".schema
      ShredModelEntry(dummyPtr, props).columnType must beEqualTo(RedshiftVarchar(3))
    }
    "handle invalid enum" in {
      val props = json"""{"type": "integer", "multipleOf": 1, "enum": [2,3,5,"hello",32]}""".schema
      ShredModelEntry(dummyPtr, props).columnType must beEqualTo(RedshiftVarchar(7))
    }
    "recognize string,null maxLength == minLength as CHAR" in {
      val props = json"""{"type": ["string","null"], "minLength": "12", "maxLength": "12"}""".schema
      ShredModelEntry(dummyPtr, props).columnType must beEqualTo(RedshiftChar(12))
    }
    "recognize number with product type" in {
      val props = json"""{"type": ["number","null"]}""".schema
      ShredModelEntry(dummyPtr, props).columnType must beEqualTo(RedshiftDouble)
    }
    "recognize integer with product type" in {
      val props = json"""{"type": ["integer","null"]}""".schema
      ShredModelEntry(dummyPtr, props).columnType must beEqualTo(RedshiftBigInt)
    }
    "recognize timestamp" in {
      val props = json"""{"type": "string", "format": "date-time"}""".schema
      ShredModelEntry(dummyPtr, props).columnType must beEqualTo(RedshiftTimestamp)
    }
    "recognize full date" in {
      val props = json"""{"type": "string", "format": "date"}""".schema
      ShredModelEntry(dummyPtr, props).columnType must beEqualTo(RedshiftDate)
    }
    "recogninze numbers bigger than Long.MaxValue" in {
      val props = json"""{"type": "integer", "maximum": 9223372036854775808}""".schema
      ShredModelEntry(dummyPtr, props).columnType must beEqualTo(RedshiftBigInt)
    }
    "fallback to VARCHAR(65535) for arrays" in {
      val props = json"""{"type": "array"}""".schema
      ShredModelEntry(dummyPtr, props).columnType must beEqualTo(RedshiftVarchar(65535))
    }
  }

  "suggest compression" should {
    "suggest Text255Encoding for enums less then 255 in length" in {
      val props = json"""{"type": "string", "enum": ["one", "two"], "maxLength": 42}""".schema
      ShredModelEntry(dummyPtr, props).compressionEncoding must beEqualTo(Text255Encoding)
    }

    "suggest RunLengthEncoding for booleans" in {
      val props = json"""{"type": "boolean"}""".schema
      ShredModelEntry(dummyPtr, props).compressionEncoding must beEqualTo(RunLengthEncoding)
    }
    "suggest RawEncoding for doubles" in {
      val props = json"""{"type": ["number","null"]}""".schema
      ShredModelEntry(dummyPtr, props).compressionEncoding must beEqualTo(RawEncoding)
    }
    "suggest ZstdEncoding for everything else" in {
      val props = json"""{"type": "string"}""".schema
      ShredModelEntry(dummyPtr, props).compressionEncoding must beEqualTo(ZstdEncoding)
    }
  }

  "convert to column definition" should {
    "align column width" in {
      val cols = List(
        ShredModelEntry("/ptr".jsonPointer, json"""{"type": "array"}""".schema),
        ShredModelEntry("/ptrlooong".jsonPointer, json"""{"type": "boolean"}""".schema),
        ShredModelEntry("/ptrlooon".jsonPointer, json"""{"type": "string"}""".schema),
        ShredModelEntry("/ptrs".jsonPointer, json"""{"type": "string", "format": "date-time"}""".schema),
        ShredModelEntry("/ptrlooong1111".jsonPointer, json"""{"type": ["number","null"]}""".schema),
      )
      cols.show must beEqualTo(
        """|  "schema_vendor"     VARCHAR (128) ENCODE ZSTD      NOT NULL,
           |  "schema_name"       VARCHAR (128) ENCODE ZSTD      NOT NULL,
           |  "schema_format"     VARCHAR (128) ENCODE ZSTD      NOT NULL,
           |  "schema_version"    VARCHAR (128) ENCODE ZSTD      NOT NULL,
           |  "root_id"               CHAR (36) ENCODE RAW       NOT NULL,
           |  "root_tstamp"           TIMESTAMP ENCODE ZSTD      NOT NULL,
           |  "ref_root"          VARCHAR (255) ENCODE ZSTD      NOT NULL,
           |  "ref_tree"         VARCHAR (1500) ENCODE ZSTD      NOT NULL,
           |  "ref_parent"        VARCHAR (255) ENCODE ZSTD      NOT NULL,
           |  "ptr"              VARCHAR(65535) ENCODE ZSTD      NOT NULL,
           |  "ptrlooong"               BOOLEAN ENCODE RUNLENGTH NOT NULL,
           |  "ptrlooon"         VARCHAR(65535) ENCODE ZSTD      NOT NULL,
           |  "ptrs"                  TIMESTAMP ENCODE ZSTD      NOT NULL,
           |  "ptrlooong1111"  DOUBLE PRECISION ENCODE RAW""".stripMargin)
    }
    "align column width to extra columns when other columns are smaller" in {
      val cols = List(
        ShredModelEntry("/ptr".jsonPointer, json"""{"type": "array"}""".schema)
      )
      cols.show must beEqualTo(
        """|  "schema_vendor"   VARCHAR (128) ENCODE ZSTD NOT NULL,
           |  "schema_name"     VARCHAR (128) ENCODE ZSTD NOT NULL,
           |  "schema_format"   VARCHAR (128) ENCODE ZSTD NOT NULL,
           |  "schema_version"  VARCHAR (128) ENCODE ZSTD NOT NULL,
           |  "root_id"             CHAR (36) ENCODE RAW  NOT NULL,
           |  "root_tstamp"         TIMESTAMP ENCODE ZSTD NOT NULL,
           |  "ref_root"        VARCHAR (255) ENCODE ZSTD NOT NULL,
           |  "ref_tree"       VARCHAR (1500) ENCODE ZSTD NOT NULL,
           |  "ref_parent"      VARCHAR (255) ENCODE ZSTD NOT NULL,
           |  "ptr"            VARCHAR(65535) ENCODE ZSTD NOT NULL""".stripMargin)
    }
  }

  "String factory" should {
    "extract jsonNull" in {
      ShredModelEntry(
        "/nullPtr".jsonPointer,
        json"""{"type": ["number","null"]}""".schema
      ).stringFactory(refJson) must beEqualTo("\\N")
    }
    "extract jsonBoolean" in {
      ShredModelEntry(
        "/boolPtr".jsonPointer,
        json"""{"type": "boolean"}""".schema
      ).stringFactory(refJson) must beEqualTo("1")
    }
    "extract jsonNumber" in {
      ShredModelEntry(
        "/numPtr".jsonPointer,
        json"""{"type": "number"}""".schema
      ).stringFactory(refJson) must beEqualTo("9999")
    }
    "extract jsonString" in {
      ShredModelEntry(
        "/strPtr".jsonPointer,
        json"""{"type": "string"}""".schema
      ).stringFactory(refJson) must beEqualTo("a")
    }
    "extract jsonArray" in {
      ShredModelEntry(
        "/arrayPtr".jsonPointer,
        json"""{"type": "array"}""".schema
      ).stringFactory(refJson) must beEqualTo("[\"a\",\"b\"]")
    }
    "extract jsonObject" in {
      ShredModelEntry(
        "/objPtr".jsonPointer,
        json"""{"type": "object"}""".schema
      ).stringFactory(refJson) must beEqualTo("{\"a\":\"b\"}")
    }
    "extract nested field" in {
      ShredModelEntry(
        "/objPtr/a".jsonPointer,
        json"""{"type": "string"}""".schema
      ).stringFactory(refJson) must beEqualTo("b")
    }
    "extract null from missing nested field" in {
      ShredModelEntry(
        "/objPtr/b/s/c".jsonPointer,
        json"""{"type": "string"}""".schema
      ).stringFactory(refJson) must beEqualTo("\\N")
    }
  }
}

object ShredModelEntrySpec {
  val refJson =
    json"""{
       "arrayPtr": ["a", "b"],
       "objPtr": {"a": "b"},
       "strPtr": "a",
       "numPtr": 9999,
       "nullPtr": null,
       "boolPtr": true
        }"""
  val dummyPtr = "/ptr".jsonPointer
  val dummySchema = json"""{"type": "string"}""".schema
}