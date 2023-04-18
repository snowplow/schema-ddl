package com.snowplowanalytics.iglu.schemaddl.jsonschema.suggestion

import baseTypes._
import io.circe.Json
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.NumberProperty
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema

object decimals {

  def integerType(schema: Schema): BaseType =
    (schema.minimum, schema.maximum) match {
      case (Some(min), Some(max)) =>
        val minDecimal = min.getAsDecimal
        val maxDecimal = max.getAsDecimal
        if (maxDecimal <= Int.MaxValue && minDecimal >= Int.MinValue) BaseType.Int32
        else if (maxDecimal <= Long.MaxValue && minDecimal >= Long.MinValue) BaseType.Int64
        else BaseType.Decimal(
          (maxDecimal.precision - maxDecimal.scale).max(minDecimal.precision - minDecimal.scale), 0
        )
      case _ => BaseType.Int64
    }

  def numericWithMultiple(mult: NumberProperty.MultipleOf.NumberMultipleOf,
                          maximum: Option[NumberProperty.Maximum],
                          minimum: Option[NumberProperty.Minimum]): BaseType =
    (maximum, minimum) match {
      case (Some(max), Some(min)) =>
        val topPrecision = max match {
          case NumberProperty.Maximum.IntegerMaximum(max) =>
            BigDecimal(max).precision + mult.value.scale
          case NumberProperty.Maximum.NumberMaximum(max) =>
            max.precision - max.scale + mult.value.scale
        }
        val bottomPrecision = min match {
          case NumberProperty.Minimum.IntegerMinimum(min) =>
            BigDecimal(min).precision + mult.value.scale
          case NumberProperty.Minimum.NumberMinimum(min) =>
            min.precision - min.scale + mult.value.scale
        }

        BaseType.Decimal(topPrecision.max(bottomPrecision), mult.value.scale)

      case _ =>
        BaseType.Double
    }


  def numericEnum(enums: List[Json]): Option[NullableWrapper] = {
    def go(scale: Int, max: BigDecimal, nullable: Boolean, enums: List[Json]): Option[NullableWrapper] =
      enums match {
        case Nil =>
          val t = if ((scale == 0) && (max <= Int.MaxValue)) BaseType.Int32
          else if ((scale == 0) && (max <= Long.MaxValue)) BaseType.Int64
          else BaseType.Decimal(max.precision - max.scale + scale, scale)

          Some(NullableWrapper.fromBool(t, nullable))
        case Json.Null :: tail => go(scale, max, true, tail)
        case h :: tail =>
          h.asNumber.flatMap(_.toBigDecimal) match {
            case Some(bigDecimal) =>
              val nextScale = scale.max(bigDecimal.scale)
              val nextMax = (if (bigDecimal > 0) bigDecimal else -bigDecimal).max(max)
              go(nextScale, nextMax, nullable, tail)
            case None => None
          }
      }

    go(0, 0, false, enums)
  }

}
