/*
 * Copyright (c) 2014-2023 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.iglu.schemaddl.jsonschema
package properties

/**
  * Marker trait for properties specific *ONLY* for objects
  */
private[iglu] sealed trait ObjectProperty

object ObjectProperty {
  /**
   * Type representing keyword `properties`
   *
   * @see http://json-schema.org/latest/json-schema-validation.html#anchor64
   */
  final case class Properties(value: Map[String, Schema]) extends JsonSchemaProperty with ObjectProperty {
    val keyword: Keyword = Keyword.Properties
  }

  /**
   * ADT representing value for `additionalProperties` keyword
   *
   * @see http://json-schema.org/latest/json-schema-validation.html#anchor64
   */
  sealed trait AdditionalProperties extends JsonSchemaProperty with ObjectProperty {
    val keyword: Keyword = Keyword.AdditionalProperties
  }
  object AdditionalProperties {
    /**
      * Allowance of properties not listed in `properties` and `patternProperties`
      */
    final case class AdditionalPropertiesAllowed(value: Boolean) extends AdditionalProperties

    /**
      * Value **must** be always valid Schema, but it's always equals to just
      * `additionalProperties: true`
      */
    final case class AdditionalPropertiesSchema(value: Schema) extends AdditionalProperties
  }

  /**
   * ADT representing holder for `required` keyword
   *
   * @see http://json-schema.org/latest/json-schema-validation.html#anchor61
   */
  final case class Required(value: List[String]) extends JsonSchemaProperty with ObjectProperty  {
    val keyword: Keyword = Keyword.Required
  }

  /**
   * ADT representing value for `patternProperties` keyword
   *
   * @see http://json-schema.org/latest/json-schema-validation.html#anchor64
   */
  final case class PatternProperties(value: Map[String, Schema]) extends JsonSchemaProperty with ObjectProperty {
    val keyword: Keyword = Keyword.PatternProperties
  }
}
