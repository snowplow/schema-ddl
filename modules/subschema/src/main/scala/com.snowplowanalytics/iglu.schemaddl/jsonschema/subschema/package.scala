package com.snowplowanalytics.iglu.schemaddl.jsonschema.subschema

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.ArrayProperty.AdditionalItems._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.ArrayProperty.Items._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.CommonProperties._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.CommonProperties.Type._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.ObjectProperty.AdditionalProperties._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.ObjectProperty._
import dregex.Regex
import io.circe.Json

import scala.annotation.tailrec

package object subschema {

  val any: Schema  = Schema.empty
  val none: Schema = Schema.empty.copy(not = Some(Not(any)))

  def isSubSchema(s1: Schema, s2: Schema): Compatibility =
    isSubType(simplify(canonicalize(s1)), simplify(canonicalize(s2)))

  @tailrec
  def canonicalize(s: Schema): Schema =
    s match {
      case s if s.`type`.contains(Null)                => s.copy(`enum` = None)
      case s if s.`type`.contains(Boolean)             => canonicalizeBoolean(s)
      case s if s.`type`.contains(Integer)             => s
      case s if s.`type`.contains(Number)              => s
      case s if s.`type`.contains(String)              => s
      case s if s.`type`.contains(Object)              => canonicalizeObject(s)
      case s if s.`type`.contains(Array)               => canonicalizeArray(s)
      case s if s.`type`.isEmpty && s.`enum`.isDefined => canonicalize(canonicalizeEnum(s))
      case _ => s
    }

  def canonicalizeBoolean(s: Schema): Schema =
    if (s.`enum`.isDefined) s else s.copy(`enum` = Some(Enum(List(Json.True, Json.False))))

  def canonicalizeEnum(s: Schema): Schema = {
    val typeValue: Json => (Type, Option[Json]) =
      _.fold(
        jsonNull    = Null -> None,
        jsonBoolean = v => Boolean -> Some(Json.fromBoolean(v)),
        jsonNumber  = v => Number -> Some(Json.fromJsonNumber(v)),
        jsonString  = v => String -> Some(Json.fromString(v)),
        jsonArray   = v => Array -> Some(Json.fromValues(v)),
        jsonObject  = v => Object -> Some(Json.fromJsonObject(v))
      )

    val splitByType: List[Json] => List[Schema] =
      _.map(typeValue(_))
        .groupBy(_._1)
        .mapValues(_.flatMap(_._2))
        .mapValues(values => if (values.isEmpty) None else Some(Enum(values)))
        .toList
        .map({ case (t, e) => Schema.empty.copy(`type` = Some(t), `enum` = e)})

    s.`enum`
      .map(_.value)
      .map(splitByType(_))
      .map(anyOf => s.copy(`enum` = None, anyOf = Some(AnyOf(anyOf))))
      .getOrElse(s)
  }

  @tailrec
  def canonicalizeObject(s: Schema): Schema =
    (s.properties, s.additionalProperties, s.patternProperties) match {
      case (_, Some(AdditionalPropertiesAllowed(allowed)), _) =>
        canonicalizeObject(s.copy(additionalProperties = Some(AdditionalPropertiesSchema(if (allowed) any else none))))
      case (Some(Properties(props)), Some(AdditionalPropertiesSchema(_)), pProps) =>
        val newPatternProps = props.map({case (k, v) => (s"^$k$$", v)}) ++ pProps.map(_.value).getOrElse(Map.empty)
        // TODO: rewrite additionalProperties into patternProperties
        s.copy(properties = None, patternProperties = Some(PatternProperties(newPatternProps)))
      // TODO: handle overlapping patternProperties
      case _ => s
    }

  def canonicalizeArray(s: Schema): Schema =
    (s.items, s.additionalItems) match {
      case (Some(ListItems(schema)), _) =>
        s.copy(items = Some(TupleItems(List.empty)), additionalItems = Some(AdditionalItemsSchema(schema)))
      case (_, Some(AdditionalItemsAllowed(false))) =>
        s.copy(additionalItems = Some(AdditionalItemsSchema(none)))
      case _ => s
    }

  def simplify(s: Schema): Schema = s

  def isSubType(s1: Schema, s2: Schema): Compatibility = (s1, s2) match {
    case (s1, s2) if s1.`type`.contains(Null) && s1.`type` == s2.`type`    => Compatible
    case (s1, s2) if s1.`type`.contains(Boolean) && s1.`type` == s2.`type` => isBooleanSubType(s1, s2)
    case (s1, s2) if isNumber(s1) && isNumber(s2)                          => isNumberSubType(s1, s2)
    case (s1, s2) if s1.`type`.contains(String) && s1.`type` == s2.`type`  => isStringSubType(s1, s2)
    case _ => Undecidable
  }

  def isBooleanSubType(s1: Schema, s2: Schema): Compatibility =
    (s1.`enum`.map(_.value.toSet), s2.`enum`.map(_.value.toSet)) match {
      case (maybeXe1, Some(xe2)) if !maybeXe1.getOrElse(Set(Json.True, Json.False)).subsetOf(xe2) => Incompatible
      case _ => Compatible
    }

  def isStringSubType(s1: Schema, s2: Schema): Compatibility = {
    val lengthRangeToPattern: Schema => String =
      s => (s.minLength, s.maxLength) match {
        case (Some(m1), Some(m2)) => s".{${m1.value},${m2.value}}"
        case (None, Some(m2))     => s".{0,${m2.value}}"
        case (Some(m1), None)     => s".{${m1.value},}"
        case (None, None)         => s".*"
      }

    val List(p1, pl1, p2, pl2) = Regex.compile(
      List(
        s1.pattern.map(_.value).map(stripAnchors).getOrElse(".*"),
        lengthRangeToPattern(s1),
        s2.pattern.map(_.value).map(stripAnchors).getOrElse(".*"),
        lengthRangeToPattern(s2)
      )
    )

    if (p1.isSubsetOf(p2) && pl1.isSubsetOf(pl2))
      Compatible
    else
      Incompatible
  }

  def isNumberSubType(s1: Schema, s2: Schema): Compatibility = {
    val s1min = s1.minimum.map(_.getAsDecimal)
    val s1max = s1.maximum.map(_.getAsDecimal)
    val s2min = s2.minimum.map(_.getAsDecimal)
    val s2max = s2.maximum.map(_.getAsDecimal)

    (s1.`type`, s2.`type`, isSubRange((s1min, s1max), (s2min, s2max))) match {
      case (Some(t1), Some(t2), true) if t1 == t2 => Compatible
      case (Some(Integer), Some(Number), true)    => Compatible
      case _ => Incompatible
    }
  }

  def isNumber(s: Schema): Boolean =
    s.`type`.contains(Integer) || s.`type`.contains(Number)

  def stripAnchors(r: String): String = r.stripPrefix("^").stripSuffix("$")

  def isSubRange(r1: (Option[BigDecimal], Option[BigDecimal]), r2: (Option[BigDecimal], Option[BigDecimal])): Boolean = {
    val minCheck = (r1._1, r2._1) match {
      case (None, Some(_))    => false
      case (Some(_), None)    => true
      case (Some(l), Some(r)) => l >= r
      case (None, None)       => true
    }

    val maxCheck = (r1._2, r2._2) match {
      case (None, Some(_))    => false
      case (Some(_), None)    => true
      case (Some(l), Some(r)) => l <= r
      case (None, None)       => true
    }

    minCheck && maxCheck
  }

}
