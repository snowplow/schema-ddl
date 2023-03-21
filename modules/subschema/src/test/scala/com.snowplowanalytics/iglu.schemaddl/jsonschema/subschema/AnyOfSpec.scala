package com.snowplowanalytics.iglu.schemaddl.jsonschema.subschema

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.CommonProperties.{AnyOf, Enum}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.CommonProperties.Type._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.subschema.subschema._
import io.circe.Json
import org.specs2.Specification


class AnyOfSpec extends Specification with org.specs2.specification.Tables {

  val s1 = Schema.empty.copy(
    anyOf = Some(AnyOf(List(
      Schema.empty.copy(`type` = Some(String))
    )))
  )

  // Optionals using anyOf
  val s2 = Schema.empty.copy(
    anyOf = Some(AnyOf(List(
      Schema.empty.copy(`type` = Some(String)),
      Schema.empty.copy(`type` = Some(Null)),
    )))
  )

  // Optionals with union of types
  val s3 = Schema.empty.copy(
    `type` = Some(Union(Set(String, Null)))
  )

  // Heterogenous enums
  val s4 = Schema.empty.copy(
    `enum` = Some(Enum(List(Json.fromString("some_string"), Json.Null)))
  )

  def is =
    s2"""
      AnyOf
      ${
        "s1" | "s2" | "result"     |>
        s1   ! s1   ! Compatible   |
        s1   ! s2   ! Compatible   |
        s1   ! s3   ! Compatible   |
        s2   ! s1   ! Incompatible |
        s3   ! s1   ! Incompatible |
        s3   ! s2   ! Compatible   |
        s2   ! s3   ! Compatible   |
        s4   ! s1   ! Incompatible |
        s4   ! s2   ! Compatible   |
        s4   ! s3   ! Compatible   |
        { (s1, s2, result) => isSubSchema(s1, s2) mustEqual result }
      }
    """
}