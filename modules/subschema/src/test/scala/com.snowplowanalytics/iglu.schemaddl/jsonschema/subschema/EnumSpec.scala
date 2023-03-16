package com.snowplowanalytics.iglu.schemaddl.jsonschema.subschema

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.ArrayProperty.Items._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.ArrayProperty._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.CommonProperties._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.CommonProperties.Type._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.NumberProperty.Maximum._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.NumberProperty.Minimum._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.ObjectProperty.AdditionalProperties._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.ObjectProperty._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.StringProperty._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.subschema.subschema._
import io.circe.Json
import org.specs2.Specification


class EnumSpec extends Specification {
  def is =
    s2"""
      heterogeneous enum gets canonicalized into a disjunction of multiple homogeneous enums $e1
      non-boolean multi-valued enum gets simplified to anyOf of single-valued enums $e2
      null single-valued enum gets simplified to schema with type but no enum $e3
      string single-valued enum gets simplified to schema with type and pattern $e4
      integer single-valued enum gets simplified to schema with type, minimum and maximum $e5
      number single-valued enum gets simplified to schema with type, minimum and maximum $e6
      array single-valued enum gets simplified to schema with type, minItems, maxItems and items $e7
      object single-valued enum gets simplified to schema with type, required, additionalProperties and properties $e8
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

  def e2 = {
    val input = Schema.empty.copy(
      `type` = Some(String),
      `enum` = Some(Enum(List(Json.fromString("a"), Json.fromString("b"))))
    )

    val result = simplify(input)

    result.anyOf.map(_.value.length) mustEqual input.`enum`.map(_.value.length)
  }

  def e3 = {
    val input = Schema.empty.copy(
      `type` = Some(Null),
      `enum` = Some(Enum(List(Json.Null)))
    )

    simplify(input) mustEqual Schema.empty.copy(`type` = Some(Null))
  }

  def e4 = {
    val input = Schema.empty.copy(
      `type` = Some(String),
      `enum` = Some(Enum(List(Json.fromString("a"))))
    )

    simplify(input) mustEqual Schema.empty.copy(`type` = Some(String), pattern = Some(Pattern("^a$")))
  }

  def e5 = {
    val input = Schema.empty.copy(
      `type` = Some(Integer),
      `enum` = Some(Enum(List(Json.fromInt(1))))
    )

    simplify(input) mustEqual Schema.empty.copy(
      `type` = Some(Integer),
      minimum = Some(IntegerMinimum(1)),
      maximum = Some(IntegerMaximum(1))
    )
  }

  def e6 = {
    val input = Schema.empty.copy(
      `type` = Some(Number),
      `enum` = Some(Enum(List(Json.fromBigDecimal(1.0))))
    )

    simplify(input) mustEqual Schema.empty.copy(
      `type` = Some(Number),
      minimum = Some(NumberMinimum(1.0)),
      maximum = Some(NumberMaximum(1.0))
    )
  }

  def e7 = {
    val input = Schema.empty.copy(
      `type` = Some(Array),
      `enum` = Some(Enum(List(Json.arr(Json.fromInt(1)))))
    )

    simplify(input) mustEqual Schema.empty.copy(
      `type` = Some(Array),
      minItems = Some(MinItems(1)),
      maxItems = Some(MaxItems(1)),
      items = Some(TupleItems(List(
        Schema.empty.copy(
          `type` = Some(Number),
          minimum = Some(NumberMinimum(1)),
          maximum = Some(NumberMaximum(1))
        )
      )))
    )
  }

  def e8 = {
    val input = Schema.empty.copy(
      `type` = Some(Object),
      `enum` = Some(Enum(List(Json.obj("a" -> Json.fromInt(1)))))
    )

    simplify(input) mustEqual Schema.empty.copy(
      `type` = Some(Object),
      required = Some(Required(List("a"))),
      additionalProperties = Some(AdditionalPropertiesSchema(Schema.empty.copy(not = Some(Not(Schema.empty))))),
      properties = Some(Properties(Map(
        "a" -> Schema.empty.copy(
          `type` = Some(Number),
          minimum = Some(NumberMinimum(1)),
          maximum = Some(NumberMaximum(1))
        )
      )))
    )
  }
}