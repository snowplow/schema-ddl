package com.snowplowanalytics.iglu.schemaddl.jsonschema.subschema

import org.specs2.Specification

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.subschema.subschema._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.CommonProperties.Type._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.StringProperty._


class StringsSpec extends Specification with org.specs2.specification.Tables { def is = s2"""
  Strings
  ${
     "s1min" | "s1max" | "s2max" | "s2max" | "p1" | "p2" | "result" |>
     None    ! None    ! None    ! None    ! None ! None ! true     |
     Some(1) ! Some(1) ! Some(0) ! Some(2) ! None ! None ! true     |
     Some(0) ! Some(1) ! Some(1) ! Some(1) ! None ! None ! false    |
     Some(0) ! None    ! Some(1) ! Some(1) ! None ! None ! false    |
     None    ! Some(1) ! Some(1) ! Some(1) ! None ! None ! false    |
     Some(1) ! Some(1) ! None    ! None    ! None ! None ! true     |
     None    ! None    ! None    ! None    ! Some("^.*$") ! Some(".*") ! true |
     None    ! None    ! None    ! None    ! Some("[def]*") ! Some("[^abc]*") ! true |
     None    ! None    ! None    ! None    ! None ! Some("[abc]*") ! false |
     None    ! None    ! None    ! None    ! Some("a") ! Some("a|b|c") ! true |
     { (a, b, c, d, e, f, g) => {
       val s1 = Schema.empty.copy(
         `type`=Some(String),
         pattern=e.map(Pattern(_)),
         minLength=a.map(MinLength(_)),
         maxLength=b.map(MaxLength(_))
       )
       val s2 = Schema.empty.copy(
         `type`=Some(String),
         pattern=f.map(Pattern(_)),
         minLength=c.map(MinLength(_)),
         maxLength=d.map(MaxLength(_))
       )

       isSubSchema(s1, s2) mustEqual g
     }}
   }"""
}
