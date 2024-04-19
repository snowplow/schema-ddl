/*
 * Copyright (c) 2021-2023 Snowplow Analytics Ltd. All rights reserved.
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

import cats.Eq
import cats.data.NonEmptyList

sealed trait Type extends Product with Serializable

object Type {

  case object String extends Type 
  case object Boolean extends Type 
  case object Integer extends Type 
  case object Long extends Type 
  case object Double extends Type 
  case class Decimal(precision: DecimalPrecision, scale: Int) extends Type 
  case object Date extends Type 
  case object Timestamp extends Type 
  case class Struct(fields: NonEmptyList[Field]) extends Type 
  case class Array(element: Type, nullability: Nullability) extends Type

  /* Fallback type for when json schema does not map to a parquet primitive type (e.g. unions)
   *
   * Where possible, this should be written as Parquet's Json logical type.
   * ....but Spark cannot write the Json logical type, so then write it as a String logical type.
   * Spark and Databricks can safely read the Json and String logical types interchangeably.
   */
  case object Json extends Type

  implicit val typeEq: Eq[Type] = Eq.fromUniversalEquals[Type]

  sealed trait Nullability {
    def nullable: Boolean
    def required: Boolean = !nullable
  }

  object Nullability {
    case object Nullable extends Nullability {
      override def nullable: Boolean = true
    }
    case object Required extends Nullability {
      override def nullable: Boolean = false
    }
  }

  sealed trait DecimalPrecision
  object DecimalPrecision {
    case object Digits9 extends DecimalPrecision // Int32 physical type
    case object Digits18 extends DecimalPrecision // Int64 physical type
    case object Digits38 extends DecimalPrecision // Fixed length byte array physical type.

    def of(precision: Int): Option[DecimalPrecision] =
      if (precision <= 9) Some(Digits9)
      else if (precision <= 18) Some(Digits18)
      else if (precision <= 38) Some(Digits38)
      else None

    def toInt(precision: DecimalPrecision): Int = precision match {
      case Digits9 => 9
      case Digits18 => 18
      case Digits38 => 38
    }
  }
}
