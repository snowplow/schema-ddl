package com.snowplowanalytics.iglu.schemaddl.jsonschema

sealed trait Keyword {
  def name: Symbol

  /** Can contain another schema */
  def recursive: Boolean
}


object Keyword {
  // increased/decresed

  // added/removed (type, enum)

  // copy of a Schema
  // with prop: (was, became)

  case object MultipleOf extends Keyword {
    val name = Symbol("multipleOf")
    val recursive = false
  }
  case object Minimum extends Keyword {
    val name = Symbol("minimum")
    val recursive = false
  }
  case object Maximum extends Keyword {
    val name = Symbol("maximum")
    val recursive = false
  }

  case object MaxLength extends Keyword {
    val name = Symbol("maxLength")
    val recursive = false
  }
  case object MinLength extends Keyword {
    val name = Symbol("minLength")
    val recursive = false
  }
  case object Pattern extends Keyword {
    val name = Symbol("pattern")
    val recursive = false
  }
  case object Format extends Keyword {
    val name = Symbol("format")
    val recursive = false
  }
  case object SchemaUri extends Keyword {
    val name = Symbol("$schema")
    val recursive = false
  }

  case object Items extends Keyword {
    val name = Symbol("items")
    val recursive = true
  }
  case object AdditionalItems extends Keyword {
    val name = Symbol("additionalItems")
    val recursive = true
  }
  case object MinItems extends Keyword {
    val name = Symbol("minItems")
    val recursive = false
  }
  case object MaxItems extends Keyword {
    val name = Symbol("maxItems")
    val recursive = false
  }

  case object Properties extends Keyword {
    val name = Symbol("properties")
    val recursive = true
  }
  case object AdditionalProperties extends Keyword {
    val name = Symbol("additionalProperties")
    val recursive = true
  }
  case object Required extends Keyword {
    val name = Symbol("required")
    val recursive = false
  }
  case object PatternProperties extends Keyword {
    val name = Symbol("patternProperties")
    val recursive = true
  }

  case object Type extends Keyword {
    val name = Symbol("type")
    val recursive = false
  }
  case object Enum extends Keyword {
    val name = Symbol("enum")
    val recursive = false
  }
  case object OneOf extends Keyword {
    val name = Symbol("oneOf")
    val recursive = true
  }
  case object Description extends Keyword {
    val name = Symbol("description")
    val recursive = false
  }
}
