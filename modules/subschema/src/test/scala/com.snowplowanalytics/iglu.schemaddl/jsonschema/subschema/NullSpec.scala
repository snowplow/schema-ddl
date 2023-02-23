package com.snowplowanalytics.iglu.schemaddl.jsonschema.subschema

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.CommonProperties.Type._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.subschema.subschema._
import org.specs2.Specification


class NullSpec extends Specification {
  def is =
    s2"""
      two schemas that allow only the null value are subtypes of each other $e1
    """

  def e1 = {
    val s1 = Schema.empty.copy(`type` = Some(Null))
    val s2 = Schema.empty.copy(`type` = Some(Null))

    isSubSchema(s1, s2) mustEqual Compatible
  }
}