package com.snowplowanalytics.iglu.schemaddl.jsonschema.subschema

import org.specs2.Specification
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.CommonProperties.Type._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.NumberProperty._

import com.snowplowanalytics.iglu.schemaddl.jsonschema.subschema.subschema._


class NumbersSpec extends Specification with org.specs2.specification.Tables { def is = s2"""
  Number schema ranges
  ${
     "s1min" | "s1max" | "s2max" | "s2max" | "result" |>
     None    ! None    ! None    ! None    ! Compatible     |
     Some(1) ! Some(1) ! Some(0) ! Some(2) ! Compatible     |
     Some(0) ! Some(1) ! Some(1) ! Some(1) ! Incompatible   |
     Some(0) ! None    ! Some(1) ! Some(1) ! Incompatible   |
     None    ! Some(1) ! Some(1) ! Some(1) ! Incompatible   |
     Some(1) ! Some(1) ! None    ! None    ! Compatible     |
     None    ! None    ! Some(1) ! Some(1) ! Incompatible   |
     { (a, b, c, d, e) => {
       val s1 = Schema.empty.copy(
         `type`=Some(Number),
         minimum=a.map(Minimum.NumberMinimum(_)),
         maximum=b.map(Maximum.NumberMaximum(_))
       )
       val s2 = Schema.empty.copy(
         `type`=Some(Number),
         minimum=c.map(Minimum.NumberMinimum(_)),
         maximum=d.map(Maximum.NumberMaximum(_))
       )

       isSubSchema(s1, s2) mustEqual e
     }}
   }"""
}
