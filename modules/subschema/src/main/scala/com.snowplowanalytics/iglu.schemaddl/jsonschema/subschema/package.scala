package com.snowplowanalytics.iglu.schemaddl.jsonschema.subschema

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.CommonProperties.Type._
import dregex.Regex


package object subschema {

  def isSubSchema(s1: Schema, s2: Schema): Compatibility =
    isSubType(simplify(canonicalize(s1)), simplify(canonicalize(s2)))

  def canonicalize(s: Schema): Schema =
    s match {
      case s if s.`type` == Some(Integer) => s.copy(`type` = Some(Number))
      case s if s.`type` == Some(Number)  => s
      case s if s.`type` == Some(String)  => s
      case _ => s
    }


  def simplify(s: Schema): Schema = s

  def isSubType(s1: Schema, s2: Schema): Compatibility = (s1, s2) match {
    case (s1, s2) if s1.`type` == Some(Number) && s1.`type` == s2.`type` => isNumberSubType(s1, s2)
    case (s1, s2) if s1.`type` == Some(String) && s1.`type` == s2.`type` => isStringSubType(s1, s2)
    case _ => Undecidable
  }

  def stripAnchors(r: String): String = r.stripPrefix("^").stripSuffix("$")

  def isStringSubType(s1: Schema, s2: Schema): Compatibility = {
    val sp1 = (s1.minLength, s1.maxLength) match {
      case (Some(m1), Some(m2)) => s".{${m1.value},${m2.value}}"
      case (None, Some(m2))     => s".{,${m2.value}}"
      case (Some(m1), None)     => s".{${m1.value},}"
      case (None, None)         => s".*"
    }

    val sp2 = (s2.minLength, s2.maxLength) match {
      case (Some(m1), Some(m2)) => s".{${m1.value},${m2.value}}"
      case (None, Some(m2))     => s".{,${m2.value}}"
      case (Some(m1), None)     => s".{${m1.value},}"
      case (None, None)         => s".*"
    }

    val List(p1, pl1, p2, pl2) = Regex.compile(
      List(
        s1.pattern.map(_.value).map(stripAnchors).getOrElse(".*"),
        sp1,
        s2.pattern.map(_.value).map(stripAnchors).getOrElse(".*"),
        sp2
      )
    )

    (p1.isSubsetOf(p2), pl1.isSubsetOf(pl2)) match {
      case (true, true) => Compatible
      case _ => Incompatible
    }
  }

  def isNumberSubType(s1: Schema, s2: Schema): Compatibility = {
    val s1min = s1.minimum.map(_.getAsDecimal)
    val s2min = s2.minimum.map(_.getAsDecimal)
    val s1max = s1.maximum.map(_.getAsDecimal)
    val s2max = s2.maximum.map(_.getAsDecimal)

    val minRange = (s1min, s2min) match {
      case (None, Some(_)) => Incompatible
      case (Some(_), None) => Compatible
      case (Some(l), Some(r)) => if (l >= r) Compatible else Incompatible
      case (None, None) => Compatible
    }

    val maxRange = (s1max, s2max) match {
      case (None, Some(_)) => Incompatible
      case (Some(_), None) => Compatible
      case (Some(l), Some(r)) => if (l <= r) Compatible else Incompatible
      case (None, None) => Compatible
    }

    val typeEquality = if (s1.`type` == s2.`type`) {
      Compatible
    } else {
      Incompatible
    }

    (typeEquality, minRange, maxRange) match {
      case (Compatible, Compatible, Compatible) => Compatible
      case _ => Incompatible
    }
  }
}
