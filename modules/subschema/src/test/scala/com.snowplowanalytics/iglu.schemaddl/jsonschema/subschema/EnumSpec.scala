package com.snowplowanalytics.iglu.schemaddl.jsonschema.subschema

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.CommonProperties._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.CommonProperties.Type._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.subschema.subschema._

import io.circe.Json
import org.specs2.Specification


class EnumSpec extends Specification {
  def is =
    s2"""
      heterogeneous enum gets canonicalized into a disjunction of multiple homogeneous enums $e1
    """

  def e1 = {
    val input = Schema.empty.copy(
      `enum` = Some(Enum(List(
        Json.Null,
        Json.True,
        Json.False,
        Json.fromInt(1),
        Json.fromDoubleOrNull(1.23),
        Json.fromString("a"),
        Json.obj(),
        Json.arr()
      ))),
      not = Some(Not(Schema.empty))
    )

    val result = canonicalize(input)

    result.not must beSome(Not(Schema.empty))
    result.anyOf.map(_.value.toSet) mustEqual Some(Set[Schema](
      Schema.empty.copy(`type` = Some(Null)),
      Schema.empty.copy(`type` = Some(Boolean), `enum` = Some(Enum(List(Json.True, Json.False)))),
      Schema.empty.copy(`type` = Some(Number), `enum` = Some(Enum(List(Json.fromInt(1), Json.fromDoubleOrNull(1.23))))),
      Schema.empty.copy(`type` = Some(String), `enum` = Some(Enum(List(Json.fromString("a"))))),
      Schema.empty.copy(`type` = Some(Object), `enum` = Some(Enum(List(Json.obj())))),
      Schema.empty.copy(`type` = Some(Array), `enum` = Some(Enum(List(Json.arr()))))
    ))
  }
}