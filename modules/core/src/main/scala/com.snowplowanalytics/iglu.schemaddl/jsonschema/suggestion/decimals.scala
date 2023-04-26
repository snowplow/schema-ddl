package com.snowplowanalytics.iglu.schemaddl.jsonschema.suggestion

import numericType._
import io.circe.Json
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.NumberProperty
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema

private[schemaddl] object decimals {

  def integerType(schema: Schema): NumericType =
    (schema.minimum, schema.maximum) match {
      case (Some(min), Some(max)) =>
        val minDecimal = min.getAsDecimal
        val maxDecimal = max.getAsDecimal
        if (maxDecimal <= Int.MaxValue && minDecimal >= Int.MinValue) NumericType.Int32
        else if (maxDecimal <= Long.MaxValue && minDecimal >= Long.MinValue) NumericType.Int64
        else NumericType.Decimal(
          (maxDecimal.precision - maxDecimal.scale).max(minDecimal.precision - minDecimal.scale), 0
        )
      case _ => NumericType.Int64
    }

  def numericWithMultiple(mult: NumberProperty.MultipleOf.NumberMultipleOf,
                          maximum: Option[NumberProperty.Maximum],
                          minimum: Option[NumberProperty.Minimum]): NumericType =
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

        NumericType.Decimal(topPrecision.max(bottomPrecision), mult.value.scale)

      case _ =>
        NumericType.Double
    }


  def numericEnum(enums: List[Json]): Option[NullableWrapper] = {
    def go(scale: Int, max: BigDecimal, nullable: Boolean, enums: List[Json]): Option[NullableWrapper] =
      enums match {
        case Nil =>
          val t = if ((scale == 0) && (max <= Int.MaxValue)) NumericType.Int32
          else if ((scale == 0) && (max <= Long.MaxValue)) NumericType.Int64
          else NumericType.Decimal(max.precision - max.scale + scale, scale)

          Some(if (nullable) NullableWrapper.NullableValue(t)
          else NullableWrapper.NotNullValue(t))

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
