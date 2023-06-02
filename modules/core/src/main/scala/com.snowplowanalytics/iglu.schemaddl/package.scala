package com.snowplowanalytics.iglu

import com.snowplowanalytics.iglu.core.SelfDescribingSchema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema

package object schemaddl {
  /**
   * Self-describing Schema container for JValue
   */
  type IgluSchema = SelfDescribingSchema[Schema]
}
