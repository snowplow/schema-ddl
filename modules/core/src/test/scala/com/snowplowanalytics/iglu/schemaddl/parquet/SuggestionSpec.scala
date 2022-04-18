/*
 * Copyright (c) 2018-2022 Snowplow Analytics Ltd. All rights reserved.
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

import com.snowplowanalytics.iglu.schemaddl.SpecHelpers
import com.snowplowanalytics.iglu.schemaddl.parquet.Field.JsonNullability
import com.snowplowanalytics.iglu.schemaddl.parquet.Suggestion._
import org.specs2.mutable.Specification

class SuggestionSpec extends Specification {

  "Timestamp suggestion" should {
    "provide date without null" in {
      assert(
        suggestion = timestampSuggestion,
        schema = """{"type": "string", "format": "date"}""",
        expectedType = Type.Date,
        expectedNullability = JsonNullability.NoExplicitNull
      )
    }
    "provide date with explicit null" in {
      assert(
        suggestion = timestampSuggestion,
        schema = """{"type": ["string", "null"], "format": "date"}""",
        expectedType = Type.Date,
        expectedNullability = JsonNullability.ExplicitlyNullable
      )
    }
    "provide timestamp without null" in {
      assert(
        suggestion = timestampSuggestion,
        schema = """{"type": "string", "format": "date-time"}""",
        expectedType = Type.Timestamp,
        expectedNullability = JsonNullability.NoExplicitNull
      )
    }
    "provide timestamp with explicit null" in {
      assert(
        suggestion = timestampSuggestion,
        schema = """{"type": ["string", "null"], "format": "date-time"}""",
        expectedType = Type.Timestamp,
        expectedNullability = JsonNullability.ExplicitlyNullable
      )
    }
    "not provide any type in case of regular string type" in {
      assertNone(
        suggestion = timestampSuggestion,
        schema = """{"type": "string"}"""
      )
    }
    "not provide any type in case of non-timestamp format" in {
      assertNone(
        suggestion = timestampSuggestion,
        schema = """{"type": "string", "format": "uuid"}"""
      )
    }
  }

  "Boolean suggestion" should {
    "provide boolean without null" in {
      assert(
        suggestion = booleanSuggestion,
        schema = """{"type": "boolean"}""",
        expectedType = Type.Boolean,
        expectedNullability = JsonNullability.NoExplicitNull
      )
    }
    "provide boolean with explicit null" in {
      assert(
        suggestion = booleanSuggestion,
        schema = """{"type": ["boolean", "null"]}""",
        expectedType = Type.Boolean,
        expectedNullability = JsonNullability.ExplicitlyNullable
      )
    }
    "not provide any type in case of non boolean type" in {
      assertNone(
        suggestion = booleanSuggestion,
        schema = """{"type": "integer"}"""
      )
    }
    "not provide any type in case of union with string" in {
      assertNone(
        suggestion = booleanSuggestion,
        schema = """{"type": ["boolean", "string"]}"""
      )
    }
  }

  "String suggestion" should {
    "provide string without null" in {
      assert(
        suggestion = stringSuggestion,
        schema = """{"type": "string"}""",
        expectedType = Type.String,
        expectedNullability = JsonNullability.NoExplicitNull
      )
    }
    "provide string with explicit null" in {
      assert(
        suggestion = stringSuggestion,
        schema = """{"type": ["null", "string"]}""",
        expectedType = Type.String,
        expectedNullability = JsonNullability.ExplicitlyNullable
      )
    }

    "not provide any type in case of non string type" in {
      assertNone(
        suggestion = stringSuggestion,
        schema = """{"type": "integer"}"""
      )
    }
    "not provide any type in case of union with string" in {
      assertNone(
        suggestion = stringSuggestion,
        schema = """{"type": ["integer", "string"]}"""
      )
    }
  }

  "Numeric suggestion" should {
    "provide integer when" >> {
      "type - integer, min/max within integer range" in {
        assert(
          suggestion = numericSuggestion,
          schema = """{"type": "integer", "minimum": 0, "maximum": 10}""",
          expectedType = Type.Integer,
          expectedNullability = JsonNullability.NoExplicitNull
        )
      }
      "type - integer, negative min/max within integer range" in {
        assert(
          suggestion = numericSuggestion,
          schema = """{"type": "integer", "minimum": -10, "maximum": 0}""",
          expectedType = Type.Integer,
          expectedNullability = JsonNullability.NoExplicitNull
        )
      }
      "type - integer, min/max are decimals within integer range" in {
        assert(
          suggestion = numericSuggestion,
          schema = """{"type": "integer", "minimum": 0.1, "maximum": 10.1}""",
          expectedType = Type.Integer,
          expectedNullability = JsonNullability.NoExplicitNull
        )
      }
      "'type' - number, min/max within integer range, 'multipleOf' - integer" in {
        assert(
          suggestion = numericSuggestion,
          schema = """{"type": "number", "minimum": 0, "maximum": 10, "multipleOf": 10}""",
          expectedType = Type.Integer,
          expectedNullability = JsonNullability.NoExplicitNull
        )
      }
    }

    "provide nullable integer when" >> {
      "type - integer, min/max within integer range" in {
        assert(
          suggestion = numericSuggestion,
          schema = """{"type": ["integer", "null"], "minimum": 0, "maximum": 10}""",
          expectedType = Type.Integer,
          expectedNullability = JsonNullability.ExplicitlyNullable
        )
      }
      "'type' - number, min/max within integer range, 'multipleOf' - integer" in {
        assert(
          suggestion = numericSuggestion,
          schema = """{"type": ["number", "null"], "minimum": 0, "maximum": 10, "multipleOf": 10}""",
          expectedType = Type.Integer,
          expectedNullability = JsonNullability.ExplicitlyNullable
        )
      }
    }

    "provide long when" >> {
      "type - integer, no min/max specified" in {
        assert(
          suggestion = numericSuggestion,
          schema = """{"type": "integer"}""",
          expectedType = Type.Long,
          expectedNullability = JsonNullability.NoExplicitNull
        )
      }
      "type - integer, only min specified" in {
        assert(
          suggestion = numericSuggestion,
          schema = """{"type": "integer", "min": 5}""",
          expectedType = Type.Long,
          expectedNullability = JsonNullability.NoExplicitNull
        )
      }
      "type - integer, only max specified" in {
        assert(
          suggestion = numericSuggestion,
          schema = """{"type": "integer", "max": 5}""",
          expectedType = Type.Long,
          expectedNullability = JsonNullability.NoExplicitNull
        )
      }
      "type - integer, min/max exceeds integer range" in {
        assert(
          suggestion = numericSuggestion,
          schema = """{"type": "integer", "minimum": 0, "maximum": 2147483648}""",
          expectedType = Type.Long,
          expectedNullability = JsonNullability.NoExplicitNull
        )
      }
      "type - integer, negative min/max exceeds integer range" in {
        assert(
          suggestion = numericSuggestion,
          schema = """{"type": "integer", "minimum": -2147483649, "maximum": 0}""",
          expectedType = Type.Long,
          expectedNullability = JsonNullability.NoExplicitNull
        )
      }
      "type - integer, decimal min/max exceeds integer range" in {
        assert(
          suggestion = numericSuggestion,
          schema = """{"type": "integer", "minimum": 0.5, "maximum": 2147483648.9}""",
          expectedType = Type.Long,
          expectedNullability = JsonNullability.NoExplicitNull
        )
      }
      "type - integer, min/max exceeds integer range, 'multipleOf' - integer" in {
        assert(
          suggestion = numericSuggestion,
          schema = """{"type": "integer", "minimum": 0, "maximum": 2147483648, "multipleOf": 10}""",
          expectedType = Type.Long,
          expectedNullability = JsonNullability.NoExplicitNull
        )
      }
      "type - number, min/max exceeds integer range, 'multipleOf' - integer" in {
        assert(
          suggestion = numericSuggestion,
          schema = """{"type": "number", "minimum": 0, "maximum": 2147483648, "multipleOf": 10}""",
          expectedType = Type.Long,
          expectedNullability = JsonNullability.NoExplicitNull
        )
      }
      "type - number or integer, min/max exceeds integer range, 'multipleOf' - integer" in {
        assert(
          suggestion = numericSuggestion,
          schema = """{"type": ["number", "integer"], "minimum": 0, "maximum": 2147483648, "multipleOf": 10}""",
          expectedType = Type.Long,
          expectedNullability = JsonNullability.NoExplicitNull
        )
      }
    }

    "provide nullable long when" >> {
      "type - integer, no min/max specified" in {
        assert(
          suggestion = numericSuggestion,
          schema = """{"type": ["integer", "null"]}""",
          expectedType = Type.Long,
          expectedNullability = JsonNullability.ExplicitlyNullable
        )
      }
    }

    "provide decimal when" >> {
      "type - integer, min/max exceeds long range, no 'multipleOf'" in {
        assert(
          suggestion = numericSuggestion,
          schema = """{"type": "integer", "minimum": 0, "maximum": 9223372036854775808}""",
          expectedType = Type.Decimal(Type.DecimalPrecision.Digits38, 0),
          expectedNullability = JsonNullability.NoExplicitNull
        )
      }
      "type - integer, negative min/max exceeds long range, no 'multipleOf'" in {
        assert(
          suggestion = numericSuggestion,
          schema = """{"type": "integer", "minimum": -9223372036854775809, "maximum": 0}""",
          expectedType = Type.Decimal(Type.DecimalPrecision.Digits38, 0),
          expectedNullability = JsonNullability.NoExplicitNull
        )
      }
      "type - number, min/max in integer range, 'multipleOf' - number" in {
        assert(
          suggestion = numericSuggestion,
          schema = """{"type": "number", "minimum": 0, "maximum": 500, "multipleOf": 0.01}""",
          expectedType = Type.Decimal(Type.DecimalPrecision.Digits9, 2),
          expectedNullability = JsonNullability.NoExplicitNull
        )
      }
      "type - number, negative min/max in integer range, 'multipleOf' - number" in {
        assert(
          suggestion = numericSuggestion,
          schema = """{"type": "number", "minimum": -500, "maximum": 0, "multipleOf": 0.01}""",
          expectedType = Type.Decimal(Type.DecimalPrecision.Digits9, 2),
          expectedNullability = JsonNullability.NoExplicitNull
        )
      }
      "type - number, min/max exceeds integer range, 'multipleOf' - number" in {
        assert(
          suggestion = numericSuggestion,
          schema = """{"type": "number", "minimum": 0, "maximum": 2147483648, "multipleOf": 0.01}""",
          expectedType = Type.Decimal(Type.DecimalPrecision.Digits18, 2),
          expectedNullability = JsonNullability.NoExplicitNull
        )
      }
      "type - integer, min/max exceeds long range, 'multipleOf' - integer" in {
        assert(
          suggestion = numericSuggestion,
          schema = """{"type": "number", "minimum": 0, "maximum": 9223372036854775808, "multipleOf": 10}""",
          expectedType = Type.Decimal(Type.DecimalPrecision.Digits38, 0),
          expectedNullability = JsonNullability.NoExplicitNull
        )
      }
      "type - number, min/max exceeds long range, 'multipleOf' - integer" in {
        assert(
          suggestion = numericSuggestion,
          schema = """{"type": "number", "minimum": 0, "maximum": 9223372036854775808, "multipleOf": 10}""",
          expectedType = Type.Decimal(Type.DecimalPrecision.Digits38, 0),
          expectedNullability = JsonNullability.NoExplicitNull
        )
      }
    }

    "provide double when" >> {
      "type - number, no min/max" in {
        assert(
          suggestion = numericSuggestion,
          schema = """{"type": "number"}""",
          expectedType = Type.Double,
          expectedNullability = JsonNullability.NoExplicitNull
        )
      }
      "type - number or integer, no min/max" in {
        assert(
          suggestion = numericSuggestion,
          schema = """{"type": ["number", "integer"]}""",
          expectedType = Type.Double,
          expectedNullability = JsonNullability.NoExplicitNull
        )
      }
    }

    "provide nullable double when" >> {
      "type - number, no min/max" in {
        assert(
          suggestion = numericSuggestion,
          schema = """{"type": ["number", "null"]}""",
          expectedType = Type.Double,
          expectedNullability = JsonNullability.ExplicitlyNullable
        )
      }
    }

    "not provide any type in case of non-numeric" in {
      "type - string" in {
        assertNone(
          suggestion = numericSuggestion,
          schema = """{"type": "string"}"""
        )
      }
      "type - number or string" in {
        assertNone(
          suggestion = numericSuggestion,
          schema = """{"type": ["number", "string"]}"""
        )
      }
    }
  }

  "Enum suggestion" should {
    "provide string enum without null" in {
      assert(
        suggestion = enumSuggestion,
        schema = """{"enum": ["x", "y", "z"]}""",
        expectedType = Type.String,
        expectedNullability = JsonNullability.NoExplicitNull
      )
    }
    "provide string enum with null" in {
      assert(
        suggestion = enumSuggestion,
        schema = """{"enum": ["x", "y", "z", null]}""",
        expectedType = Type.String,
        expectedNullability = JsonNullability.ExplicitlyNullable
      )
    }
    "provide integer enum without null" in {
      assert(
        suggestion = enumSuggestion,
        schema = """{"enum": [1, 2, 3]}""",
        expectedType = Type.Decimal(Type.DecimalPrecision.Digits9, 0),
        expectedNullability = JsonNullability.NoExplicitNull
      )
    }
    "provide integer enum with null" in {
      assert(
        suggestion = enumSuggestion,
        schema = """{"enum": [1, 2, null, 3]}""",
        expectedType = Type.Decimal(Type.DecimalPrecision.Digits9, 0),
        expectedNullability = JsonNullability.ExplicitlyNullable
      )
    }
    "provide decimal enum without null" in {
      assert(
        suggestion = enumSuggestion,
        schema = """{"enum": [0.1, 222, 3.3]}""",
        expectedType = Type.Decimal(Type.DecimalPrecision.Digits9, 1),
        expectedNullability = JsonNullability.NoExplicitNull
      )
    }
    "provide integer enum with null" in {
      assert(
        suggestion = enumSuggestion,
        schema = """{"enum": [0.1, null, 222, 3.3]}""",
        expectedType = Type.Decimal(Type.DecimalPrecision.Digits9, 1),
        expectedNullability = JsonNullability.ExplicitlyNullable
      )
    }
    "provide json enum without null" in {
      assert(
        suggestion = enumSuggestion,
        schema = """{"enum": [0.1, "xyz"]}""",
        expectedType = Type.Json,
        expectedNullability = JsonNullability.NoExplicitNull
      )
    }
    "provide json enum with null" in {
      assert(
        suggestion = enumSuggestion,
        schema = """{"enum": [0.1, null, "xyz"]}""",
        expectedType = Type.Json,
        expectedNullability = JsonNullability.ExplicitlyNullable
      )
    }
  }

  private def assert(suggestion: Suggestion,
                     schema: String,
                     expectedType: Type,
                     expectedNullability: JsonNullability) = {
    val input = SpecHelpers.parseSchema(schema)

    suggestion.apply(input) must beSome(Field.NullableType(expectedType, expectedNullability))
  }

  private def assertNone(suggestion: Suggestion,
                         schema: String) = {
    val input = SpecHelpers.parseSchema(schema)

    suggestion.apply(input) must beNone
  }
}
