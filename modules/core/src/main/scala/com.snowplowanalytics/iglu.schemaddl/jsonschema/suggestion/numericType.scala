package com.snowplowanalytics.iglu.schemaddl.jsonschema.suggestion

private[schemaddl] object numericType {
  sealed trait NumericType extends Product with Serializable

  object NumericType {
    case object Double extends NumericType

    case object Int32 extends NumericType

    case object Int64 extends NumericType

    case class Decimal(precision: Int, scale: Int) extends NumericType
  }

  sealed trait NullableWrapper

  object NullableWrapper {

    case class NullableValue(value: NumericType) extends NullableWrapper

    case class NotNullValue(value: NumericType) extends NullableWrapper

  }
}
