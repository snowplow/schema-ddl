package com.snowplowanalytics.iglu.schemaddl.jsonschema.subschema

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.ArrayProperty.AdditionalItems._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.ArrayProperty.Items._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.ArrayProperty._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.CommonProperties.Type._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.subschema.subschema._
import org.specs2.Specification


class ArraySpec extends Specification with org.specs2.specification.Tables {

  val s1 = Schema.empty.copy(`type` = Some(Array))

  val s2 = Schema.empty.copy(
    `type` = Some(Array),
    items = Some(ListItems(Schema.empty.copy(`type` = Some(Integer))))
  )

  val s3 = Schema.empty.copy(
    `type` = Some(Array),
    items = Some(ListItems(Schema.empty.copy(`type` = Some(Number))))
  )

  val s4 = Schema.empty.copy(
    `type` = Some(Array),
    items = Some(ListItems(Schema.empty.copy(`type` = Some(Number)))),
    minItems = Some(MinItems(3)),
    maxItems = Some(MaxItems(10))
  )

  val s5 = Schema.empty.copy(
    `type` = Some(Array),
    items = Some(TupleItems(List(
      Schema.empty.copy(`type` = Some(Number))
    ))),
    additionalItems = Some(AdditionalItemsAllowed(true))
  )

  val s6 = Schema.empty.copy(
    `type` = Some(Array),
    items = Some(TupleItems(List(
      Schema.empty.copy(`type` = Some(Number)),
      Schema.empty.copy(`type` = Some(String)),
    ))),
    additionalItems = Some(AdditionalItemsAllowed(true))
  )

  val s7 = Schema.empty.copy(
    `type` = Some(Array),
    items = Some(TupleItems(List(
      Schema.empty.copy(`type` = Some(Number))
    ))),
    additionalItems = Some(AdditionalItemsSchema(Schema.empty.copy(`type` = Some(String))))
  )

  def is =
    s2"""
      Arrays
      ${
        "s1" | "s2" | "result"     |>
        s1   ! s2   ! Incompatible |
        s2   ! s1   ! Compatible   |
        s2   ! s2   ! Compatible   |
        s2   ! s3   ! Compatible   |
        s3   ! s2   ! Incompatible |
        s3   ! s4   ! Incompatible |
        s4   ! s3   ! Compatible   |
        s5   ! s6   ! Incompatible |
        s6   ! s5   ! Compatible   |
        s6   ! s7   ! Incompatible |
        s7   ! s6   ! Compatible   |
        { (s1, s2, result) => isSubSchema(s1, s2) mustEqual result }
      }
    """
}