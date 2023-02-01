package com.snowplowanalytics.iglu.schemaddl.jsonschema.subschema

sealed trait Compatibility

case object Compatible extends Compatibility

case object Incompatible extends Compatibility

case object Undecidable extends Compatibility

