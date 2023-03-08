package com.snowplowanalytics.iglu.schemaddl.jsonschema.subschema

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.CommonProperties.Type._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.ObjectProperty.AdditionalProperties.AdditionalPropertiesAllowed
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.ObjectProperty._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.subschema.subschema._
import org.specs2.Specification


class ObjectSpec extends Specification with org.specs2.specification.Tables {

  val s1 = Schema.empty.copy(`type` = Some(Object))

  val s2 = Schema.empty.copy(
    `type` = Some(Object),
    properties = Some(Properties(Map(
      "a" -> any
    )))
  )

  val s3 = Schema.empty.copy(
    `type` = Some(Object),
    properties = Some(Properties(Map(
      "a" -> Schema.empty.copy(`type` = Some(Boolean))
    )))
  )

  val s4 = Schema.empty.copy(
    `type` = Some(Object),
    properties = Some(Properties(Map(
      "a" -> Schema.empty.copy(`type` = Some(Boolean))
    ))),
    required = Some(Required(List("a"))),
  )

  val s5 = Schema.empty.copy(
    `type` = Some(Object),
    properties = Some(Properties(Map(
      "a" -> Schema.empty.copy(`type` = Some(Boolean)),
      "b" -> Schema.empty.copy(`type` = Some(String)),
    ))),
    required = Some(Required(List("a"))),
  )

  val s6 = Schema.empty.copy(
    `type` = Some(Object),
    properties = Some(Properties(Map(
      "a" -> Schema.empty.copy(`type` = Some(Boolean))
    ))),
    required = Some(Required(List("a"))),
    additionalProperties = Some(AdditionalPropertiesAllowed(false))
  )

  def is =
    s2"""
      Objects
      ${
        "s1" | "s2" | "result"     |>
        s1   ! s1   ! Compatible   |
        s2   ! s2   ! Compatible   |
        s3   ! s2   ! Compatible   |
        s2   ! s3   ! Incompatible |
        s3   ! s4   ! Incompatible |
        s4   ! s3   ! Compatible   |
        s5   ! s4   ! Compatible   |
        s4   ! s5   ! Incompatible | // interesting...
        s6   ! s5   ! Compatible   |
        { (s1, s2, result) => isSubSchema(s1, s2) mustEqual result }
      }
    """
}