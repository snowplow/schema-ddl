/*
 * Copyright (c) 2014-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.iglu.schemaddl.jsonschema.mutate

import cats.Semigroup
import cats.implicits._

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties._


// This is private to schema-ddl until the implementation is stable.
// In future this object could find use beyond just schema-ddl.
private[mutate] object Narrowed {

  /**
   * Narrows the properties of a schema.
   *
   * The expression `val s3 = Narrowed(s1, s2)` has the property that if data validates against both
   * schemas s1 AND s2 then it also validates against schema s3.  Conversely, data that validates
   * against s3 does not necessarily validate against both s1 and s2.
   *
   * Narrowed is a [[cats.Semigroup]].
   *
   * Note the order: `Narrowed(s1, s2)` and `Narrowed(s2, s1)` can be subtly different, although the
   * difference is unlikely to be important. For example, if s1 has format=uuid and s2 has
   * format=time, then `Narrowed(s1, s2)` has format=uuid.
   */
  def apply(s1: Schema, s2: Schema): Schema = s1 |+| s2

  implicit val schemaSemigroup: Semigroup[Schema] = new Semigroup[Schema] {
    def combine(s1: Schema, s2: Schema): Schema =
      Schema(
        multipleOf = s1.multipleOf |+| s2.multipleOf,
        minimum = s1.minimum |+| s2.minimum,
        maximum = s1.maximum |+| s2.maximum,

        maxLength = s1.maxLength |+| s2.maxLength,
        minLength = s1.minLength |+| s2.minLength,
        pattern = s1.pattern |+| s2.pattern,
        format = s1.format |+| s2.format,
        `$schema` = s1.`$schema` |+| s2.`$schema`,

        items = s1.items |+| s2.items,
        additionalItems = s1.additionalItems |+| s2.additionalItems,
        minItems = s1.minItems |+| s2.minItems,
        maxItems = s1.maxItems |+| s2.maxItems,

        properties = s1.properties |+| s2.properties,
        additionalProperties = s1.additionalProperties |+| s2.additionalProperties,
        required = s1.required |+| s2.required,
        patternProperties = s1.patternProperties |+| s2.patternProperties,

        `type` = s1.`type` |+| s2.`type`,
        enum = s1.enum |+| s2.enum,
        oneOf = s1.oneOf |+| s2.oneOf,
        anyOf = s1.anyOf |+| s2.anyOf,
        description = s1.description |+| s2.description
      )
  }

  private implicit val minimumsSemigroup: Semigroup[NumberProperty.Minimum] = new Semigroup[NumberProperty.Minimum] {
    def combine(m1: NumberProperty.Minimum, m2: NumberProperty.Minimum): NumberProperty.Minimum =
      if (m1.getAsDecimal > m2.getAsDecimal) m1 else m2
  }

  private implicit val maximumsSemigroup: Semigroup[NumberProperty.Maximum] = new Semigroup[NumberProperty.Maximum] {
    def combine(m1: NumberProperty.Maximum, m2: NumberProperty.Maximum): NumberProperty.Maximum =
      if (m1.getAsDecimal < m2.getAsDecimal) m1 else m2
  }

  private implicit val multipleOfSemigroup: Semigroup[NumberProperty.MultipleOf] = new Semigroup[NumberProperty.MultipleOf] {
    def combine(m1: NumberProperty.MultipleOf, m2: NumberProperty.MultipleOf): NumberProperty.MultipleOf =
      (m1, m2) match {
        case (NumberProperty.MultipleOf.NumberMultipleOf(v1), _) => NumberProperty.MultipleOf.NumberMultipleOf(v1.min(m2.getAsDecimal))
        case (_, NumberProperty.MultipleOf.NumberMultipleOf(v2)) => NumberProperty.MultipleOf.NumberMultipleOf(v2.min(m1.getAsDecimal))
        case (NumberProperty.MultipleOf.IntegerMultipleOf(v1), NumberProperty.MultipleOf.IntegerMultipleOf(v2)) => NumberProperty.MultipleOf.IntegerMultipleOf(v1.min(v2))
      }
  }

  private implicit val maxLengthSemigroup: Semigroup[StringProperty.MaxLength] = new Semigroup[StringProperty.MaxLength] {
    def combine(m1: StringProperty.MaxLength, m2: StringProperty.MaxLength): StringProperty.MaxLength =
      StringProperty.MaxLength(m1.value.min(m2.value))
  }

  private implicit val minLengthSemigroup: Semigroup[StringProperty.MinLength] = new Semigroup[StringProperty.MinLength] {
    def combine(m1: StringProperty.MinLength, m2: StringProperty.MinLength): StringProperty.MinLength =
      StringProperty.MinLength(m1.value.max(m2.value))
  }

  private def takeFirstSemigroup[A]: Semigroup[Option[A]] = new Semigroup[Option[A]] {
    def combine(o1: Option[A], o2: Option[A]): Option[A] =
      o1.orElse(o2)
  }

  private implicit val patternSemigroup: Semigroup[Option[StringProperty.Pattern]] =
    takeFirstSemigroup[StringProperty.Pattern]

  private implicit val formatSemigroup: Semigroup[Option[StringProperty.Format]] =
    takeFirstSemigroup[StringProperty.Format]

  private implicit val schemaUriSemigroup: Semigroup[Option[StringProperty.SchemaUri]] =
    takeFirstSemigroup[StringProperty.SchemaUri]

  private implicit val propertiesSemigroup: Semigroup[ObjectProperty.Properties] = new Semigroup[ObjectProperty.Properties] {
    def combine(t1: ObjectProperty.Properties, t2: ObjectProperty.Properties): ObjectProperty.Properties =
      ObjectProperty.Properties(t1.value |+| t2.value)
  }

  private implicit val typesSemigroup: Semigroup[CommonProperties.Type] = new Semigroup[CommonProperties.Type] {
    def combine(t1: CommonProperties.Type, t2: CommonProperties.Type): CommonProperties.Type =
      t1.asUnion.value.intersect(t2.asUnion.value).toList match {
        case single :: Nil => single
        case more => CommonProperties.Type.Union(more.toSet)
      }
  }

  private implicit val enumsSemigroup: Semigroup[CommonProperties.Enum] = new Semigroup[CommonProperties.Enum] {
    def combine(t1: CommonProperties.Enum, t2: CommonProperties.Enum): CommonProperties.Enum =
      CommonProperties.Enum(t1.value.intersect(t2.value))
  }

  private implicit val oneOfSemigroup: Semigroup[CommonProperties.OneOf] = new Semigroup[CommonProperties.OneOf] {
    def combine(t1: CommonProperties.OneOf, t2: CommonProperties.OneOf): CommonProperties.OneOf =
      CommonProperties.OneOf(t1.value.flatMap(schema1 => t2.value.map(_ |+| schema1).toSet.toList))
  }

  private implicit val anyOfSemigroup: Semigroup[CommonProperties.AnyOf] = new Semigroup[CommonProperties.AnyOf] {
    def combine(t1: CommonProperties.AnyOf, t2: CommonProperties.AnyOf): CommonProperties.AnyOf =
      CommonProperties.AnyOf(t1.value.flatMap(schema1 => t2.value.map(_ |+| schema1).toSet.toList))
  }

  private implicit val minItemsSemigroup: Semigroup[ArrayProperty.MinItems] = new Semigroup[ArrayProperty.MinItems] {
    def combine(m1: ArrayProperty.MinItems, m2: ArrayProperty.MinItems): ArrayProperty.MinItems =
      ArrayProperty.MinItems(m1.value.max(m2.value))
  }

  private implicit val maxItemsSemigroup: Semigroup[ArrayProperty.MaxItems] = new Semigroup[ArrayProperty.MaxItems] {
    def combine(m1: ArrayProperty.MaxItems, m2: ArrayProperty.MaxItems): ArrayProperty.MaxItems =
      ArrayProperty.MaxItems(m1.value.min(m2.value))
  }

  private implicit def additionalPropertiesSemigroup: Semigroup[ObjectProperty.AdditionalProperties] = new Semigroup[ObjectProperty.AdditionalProperties]{
    def combine(p1: ObjectProperty.AdditionalProperties, p2: ObjectProperty.AdditionalProperties): ObjectProperty.AdditionalProperties =
      (p1, p2) match {
        case (ObjectProperty.AdditionalProperties.AdditionalPropertiesAllowed(false), _) | (_, ObjectProperty.AdditionalProperties.AdditionalPropertiesAllowed(false)) =>
          ObjectProperty.AdditionalProperties.AdditionalPropertiesAllowed(false)
        case (ObjectProperty.AdditionalProperties.AdditionalPropertiesAllowed(true), _) => p2
        case (_, ObjectProperty.AdditionalProperties.AdditionalPropertiesAllowed(true)) => p1
        case (ObjectProperty.AdditionalProperties.AdditionalPropertiesSchema(s1), ObjectProperty.AdditionalProperties.AdditionalPropertiesSchema(s2)) =>
          ObjectProperty.AdditionalProperties.AdditionalPropertiesSchema(s1 |+| s2)
      }
  }

  private implicit def requiredSemigroup: Semigroup[ObjectProperty.Required] = new Semigroup[ObjectProperty.Required]{
    def combine(p1: ObjectProperty.Required, p2: ObjectProperty.Required): ObjectProperty.Required =
      ObjectProperty.Required((p1.value ++ p2.value).toSet.toList)
  }

  private implicit def patternPropertiesSemigroup: Semigroup[ObjectProperty.PatternProperties] = new Semigroup[ObjectProperty.PatternProperties]{
    def combine(p1: ObjectProperty.PatternProperties, p2: ObjectProperty.PatternProperties): ObjectProperty.PatternProperties =
      ObjectProperty.PatternProperties(p1.value |+| p2.value)
  }

  private implicit val descriptionSemigroup: Semigroup[Option[CommonProperties.Description]] =
    takeFirstSemigroup[CommonProperties.Description]

  private implicit val itemsSemigroup: Semigroup[ArrayProperty.Items] = new Semigroup[ArrayProperty.Items] {
    def combine(i1: ArrayProperty.Items, i2: ArrayProperty.Items): ArrayProperty.Items = {
      (i1, i2) match {
        case (ArrayProperty.Items.ListItems(li1), ArrayProperty.Items.ListItems(li2)) =>
          // This is the only possible branch *if* the method [[Mutate.noTupleItems]] has been applied to the schema.
          ArrayProperty.Items.ListItems(li1 |+| li2)
        case _ =>
          // This is extremely permissive, but we assume that [[Mutate.noTupleItems]] has been applied so it does not matter.
          // To handle this case properly we would need to consider the combination of tuple items with additionalItems.
          ArrayProperty.Items.ListItems(Schema())
      }
    }
  }

  private implicit val additionalItemsSemigroup: Semigroup[ArrayProperty.AdditionalItems] = new Semigroup[ArrayProperty.AdditionalItems] {
    def combine(i1: ArrayProperty.AdditionalItems, i2: ArrayProperty.AdditionalItems): ArrayProperty.AdditionalItems = {
      (i1, i2) match {
        case (ArrayProperty.AdditionalItems.AdditionalItemsAllowed(false), _) =>
          ArrayProperty.AdditionalItems.AdditionalItemsAllowed(false)
        case (_, ArrayProperty.AdditionalItems.AdditionalItemsAllowed(false)) =>
          ArrayProperty.AdditionalItems.AdditionalItemsAllowed(false)
        case (ArrayProperty.AdditionalItems.AdditionalItemsAllowed(true), _) =>
          i2
        case (_, ArrayProperty.AdditionalItems.AdditionalItemsAllowed(true)) =>
          i1
        case (ArrayProperty.AdditionalItems.AdditionalItemsSchema(s1), ArrayProperty.AdditionalItems.AdditionalItemsSchema(s2)) =>
          ArrayProperty.AdditionalItems.AdditionalItemsSchema(s1 |+| s2)
      }
    }
  }

}
