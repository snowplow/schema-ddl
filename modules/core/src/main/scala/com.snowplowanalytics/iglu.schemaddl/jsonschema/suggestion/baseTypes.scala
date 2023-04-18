package com.snowplowanalytics.iglu.schemaddl.jsonschema.suggestion

object baseTypes {
  sealed trait BaseType extends Product with Serializable

  object BaseType {
    case object Double extends BaseType

    case object Int32 extends BaseType

    case object Int64 extends BaseType

    case class Decimal(precision: Int, scale: Int) extends BaseType
  }

  sealed trait NullableWrapper

  object NullableWrapper {

    case class NullableValue(`type`: BaseType) extends NullableWrapper

    case class NotNullValue(`type`: BaseType) extends NullableWrapper

    def fromBool(`type`: BaseType, nullable: Boolean): NullableWrapper =
      if (nullable) NullableWrapper.NullableValue(`type`)
      else NullableWrapper.NotNullValue(`type`)
  }
}
