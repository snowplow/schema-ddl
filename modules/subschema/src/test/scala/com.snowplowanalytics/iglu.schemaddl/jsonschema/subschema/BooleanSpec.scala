package com.snowplowanalytics.iglu.schemaddl.jsonschema.subschema

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.CommonProperties._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.CommonProperties.Type._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.subschema.subschema._
import io.circe.Json
import org.specs2.Specification


class BooleanSpec extends Specification with org.specs2.specification.Tables { def is = s2"""
  Number schema ranges
  ${
    "s1enum"                | "s2enum"                | "result"     |>
    Some(List(true))        ! Some(List(true, false)) ! Compatible   |
    Some(List(true, false)) ! Some(List(true))        ! Incompatible |
    None                    ! Some(List(false))       ! Incompatible |
    Some(List(true))        ! None                    ! Compatible   |
    None                    ! None                    ! Compatible   |
    { (a, b, c) =>
      val s1 = Schema.empty.copy(
        `type` = Some(Boolean),
        `enum` = a.map(xe => Enum(xe.map(Json.fromBoolean(_))))
      )
      val s2 = Schema.empty.copy(
        `type` = Some(Boolean),
        `enum` = b.map(xe => Enum(xe.map(Json.fromBoolean(_))))
      )

      isSubSchema(s1, s2) mustEqual c
    }
  }"""
}