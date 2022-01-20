/*
 * Copyright (c) 2012-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.iglu

import cats.data.NonEmptyList

import core.{SchemaMap, SelfDescribingSchema}

import schemaddl.jsonschema.{Pointer, Schema}
import com.snowplowanalytics.iglu.schemaddl.migrations.Migration


package object schemaddl {
  /**
    * Set of Schemas properties attached to corresponding JSON Pointers
    * Unlike their original Schemas, these have `null` among types if they're not required
    */
  type SubSchemas = Set[(Pointer.SchemaPointer, Schema)]

  /**
    * List of Schemas properties attached to corresponding JSON Pointers
    * Unlike SubSchemas, they are ordered according to nullness of field,
    * name of field and version which field is added
    */
  type Properties = List[(Pointer.SchemaPointer, Schema)]

  /**
   * Map of Schemas to all its possible target schemas
   * Examples:
   * com.acme/event/1-0-0    -> [1-0-0/1-0-1, 1-0-0/1-0-2, 1-0-0/1-0-3]
   * com.acme/event/1-0-1    -> [1-0-1/1-0-2, 1-0-1/1-0-3]
   * com.acme/event/1-0-2    -> [1-0-2/1-0-3]
   * com.acme/config/1-1-0   -> [1-1-0/1-0-1]
   */
  type MigrationMap = Map[SchemaMap, NonEmptyList[Migration]]

  /**
   * Schema criterion restricted to revision: vendor/name/m-r-*
   * Tuple using as root key to bunch of Schemas differing only by addition
   * (vendor, name, model, revision)
   * Hypothetical "lower" AdditionGroup could contain only one Schema
   */
  type RevisionGroup = (String, String, Int, Int)

  /**
   * Schema criterion restricted to model: vendor/name/m-*-*
   * Tuple using as root key to bunch of Schemas differing only by addition
   * (vendor, name, model)
   */
  type ModelGroup = (String, String, Int)

  /**
   * Intermediate nested structure used to group schemas by revision
   * Examples:
   * com.acme/event/1-0-*    -> [[MigrationMap]]
   * com.acme/event/1-1-*    -> [[MigrationMap]]
   * com.acme/config/1-1-*   -> [[MigrationMap]]
   * com.google/schema/1-0-* -> [[MigrationMap]]
   */
  type RevisionMigrationMap = Map[RevisionGroup, MigrationMap]

  /**
   * Self-describing Schema container for JValue
   */
  type IgluSchema = SelfDescribingSchema[Schema]

}
