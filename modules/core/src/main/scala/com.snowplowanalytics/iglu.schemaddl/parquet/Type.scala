/*
 * Copyright (c) 2021-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.iglu.schemaddl.parquet

/** BigQuery field type; "array" and "null" are expressed via `Mode` */
sealed trait Type extends Product with Serializable

object Type {
  case object String extends Type
  case object Boolean extends Type
  case object Integer extends Type
  case object Long extends Type
  case object Double extends Type
  case class Decimal(precision: Int, scale: Int) extends Type
  case object Date extends Type
  case object Timestamp extends Type
  case class Struct(fields: List[Field]) extends Type
  case class Array(element: Type, containsNull: Boolean) extends Type
}
