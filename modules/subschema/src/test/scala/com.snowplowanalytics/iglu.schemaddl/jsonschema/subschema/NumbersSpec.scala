package com.snowplowanalytics.iglu.schemaddl.jsonschema.subschema

import org.specs2.Specification
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.CommonProperties.Type._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.NumberProperty._

import com.snowplowanalytics.iglu.schemaddl.jsonschema.subschema.subschema._


class NumbersSpec extends Specification with org.specs2.specification.Tables { def is = s2"""
  Number schema ranges ${
     "s1Type" | "s2Type"  | "s1min"          | "s1max"          | "s2max"   | "s2max"   | "result"       |>
     Number   ! Number    ! None             ! None             ! None      ! None      ! Compatible     |
     Number   ! Number    ! Some(1.0)        ! Some(1.0)        ! Some(0.0) ! Some(2.0) ! Compatible     |
     Number   ! Number    ! Some(0.0)        ! Some(1.0)        ! Some(1.0) ! Some(1.0) ! Incompatible   |
     Number   ! Number    ! Some(0.0)        ! None             ! Some(1.0) ! Some(1.0) ! Incompatible   |
     Number   ! Number    ! None             ! Some(1.0)        ! Some(1.0) ! Some(1.0) ! Incompatible   |
     Number   ! Number    ! Some(1.0)        ! Some(1.0)        ! None      ! None      ! Compatible     |
     Number   ! Number    ! None             ! None             ! Some(1.0) ! Some(1.0) ! Incompatible   |
     Number   ! Integer   ! None             ! None             ! None      ! None      ! Incompatible   |
     Integer  ! Number    ! None             ! None             ! None      ! None      ! Compatible     |
     Integer  ! Number    ! Some(1.toDouble) ! Some(2.toDouble) ! Some(0.9) ! Some(2.1) ! Compatible     |
     Integer  ! Integer   ! None             ! None             ! None      ! None      ! Compatible     |
     { (a, b, c, d, e, f, g) => {
       val s1 = Schema.empty.copy(
         `type`=Some(a),
         minimum=c.map(Minimum.NumberMinimum(_)),
         maximum=d.map(Maximum.NumberMaximum(_))
       )
       val s2 = Schema.empty.copy(
         `type`=Some(b),
         minimum=e.map(Minimum.NumberMinimum(_)),
         maximum=f.map(Maximum.NumberMaximum(_))
       )

       isSubSchema(s1, s2) mustEqual g
     }}
   }"""
}
