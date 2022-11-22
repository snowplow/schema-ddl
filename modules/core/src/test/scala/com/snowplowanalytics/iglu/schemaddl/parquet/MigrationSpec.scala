package com.snowplowanalytics.iglu.schemaddl.parquet

import com.snowplowanalytics.iglu.schemaddl.SpecHelpers
import cats.syntax.either._
import com.snowplowanalytics.iglu.schemaddl.parquet.Migrations.{NullableRequired, ParquetMigration, ParquetSchemaMigrations, TopLevelKeyAddition, isSchemaMigrationBreakingFromMigrations}

class MigrationSpec extends org.specs2.Specification {

  import MigrationSpec._

  def is =
    s2"""
              Produce no migration for the same schemas $e1
              Produce migration for new nested fields $e2
              Produce migration for new top-level fields $e3
              Produce migration for removal of top-level fields $e4
              Produce migration for removal of nested fields $e5
              Produce migration for simple type widening in nested arrays $e6
              Produce migration for nullables AND invalid type change in arrays $e7
              Produce migration structs in nested arrays $e8
              Produce migration for removal of nested in array $e9
              Produce migration for nullables arrays $e10
              Produce migration for simple type widening in nested fields $e11
              Produce migration for nullables AND invalid type change in fields $e12
              Produce migration for type widening in top-level fields $e13
              Produce every possible migration for every possible type widening $e14
              Suggest version change correctly $e15
  """

  def e1 = {
    Migrations.mergeSchemas(leanBase, leanBase) should beRight(leanBase)
    Migrations.assessSchemaMigration(leanBase, leanBase) shouldEqual Set.empty[ParquetMigration]
  }

  // Produce migration for new nested fields
  def e2 = {
    val input2 = SpecHelpers.parseSchema(
      """
        |{"type": "object",
        |"properties": {
        |  "stringKey": {
        |    "type": "string",
        |    "maxLength": 500
        |  },
        |  "objectKey": {
        |    "type": "object",
        |    "properties": {
        |      "nestedKey1": { "type": "string" },
        |      "nestedKey2": { "type": ["integer", "null"] },
        |      "nestedKey3": { "type": "boolean" },
        |      "string2Key": {
        |           "type": "string",
        |           "maxLength": 500
        |         }
        |    },
        |    "required": ["nestedKey3"]
        |  }
        |}
        |}
      """.stripMargin)
    val schema2 = Field.build("top", input2, enforceValuePresence = false)
    Migrations.mergeSchemas(leanBase, schema2) should beRight(schema2)
    Migrations.assessSchemaMigration(leanBase, schema2).map(_.toString) shouldEqual Set("Nested object key addition at /objectKey/string2Key")
  }

  //  Produce migration for new top-level fields $e3
  def e3 = {
    val input2 = SpecHelpers.parseSchema(
      """
        |{"type": "object",
        |"properties": {
        |  "stringKey": {
        |    "type": "string",
        |    "maxLength": 500
        |  },
        |  "string2Key": {
        |    "type": "string",
        |    "maxLength": 500
        |  },
        |  "objectKey": {
        |    "type": "object",
        |    "properties": {
        |      "nestedKey1": { "type": "string" },
        |      "nestedKey2": { "type": ["integer", "null"] },
        |      "nestedKey3": { "type": "boolean" }
        |    },
        |    "required": ["nestedKey3"]
        |  }
        |}
        |}
      """.stripMargin)
    val schema2 = Field.build("top", input2, enforceValuePresence = false)
    Migrations.mergeSchemas(leanBase, schema2) should beRight(schema2)
    Migrations.assessSchemaMigration(leanBase, schema2).map(_.toString) shouldEqual Set("Top-level schema key addition at /string2Key")
  }

  //  Produce migration for removal of top-level fields $e4
  def e4 = {
    val input2 = SpecHelpers.parseSchema(
      """
        |{"type": "object",
        |"properties": {
        |  "objectKey": {
        |    "type": "object",
        |    "properties": {
        |      "nestedKey1": { "type": "string" },
        |      "nestedKey2": { "type": ["integer", "null"] },
        |      "nestedKey3": { "type": "boolean" }
        |    },
        |    "required": ["nestedKey3"]
        |  }
        |}
        |}
      """.stripMargin)
    val schema2 = Field.build("top", input2, enforceValuePresence = false)
    Migrations.mergeSchemas(leanBase, schema2) should beRight(leanBase)
    Migrations.assessSchemaMigration(leanBase, schema2).map(_.toString) shouldEqual Set("Key removal at /stringKey")
  }

  //  Produce migration for removal of nested fields $e5
  def e5 = {
    val input2 = SpecHelpers.parseSchema(
      """
        |{"type": "object",
        |"properties": {
        |  "stringKey": {
        |    "type": "string",
        |    "maxLength": 500
        |  },
        |  "objectKey": {
        |    "type": "object",
        |    "properties": {
        |      "nestedKey2": { "type": ["integer", "null"] },
        |      "nestedKey3": { "type": "boolean" }
        |    },
        |    "required": ["nestedKey3"]
        |  }
        |}
        |}
      """.stripMargin)
    val schema2 = Field.build("top", input2, enforceValuePresence = false)

    Migrations.mergeSchemas(leanBase, schema2) should beRight(leanBase)
    Migrations.assessSchemaMigration(leanBase, schema2).map(_.toString) shouldEqual Set("Key removal at /objectKey/nestedKey1")
  }

  //  Produce migration for simple type widening in nested arrays $e6
  def e6 = {
    val input1 = SpecHelpers.parseSchema(
      """
        |{"type": "object",
        |"properties": {
        |  "arrayKey": {
        |    "type": "array",
        |    "items": {
        |      "type": "integer"
        |     }
        |  }
        |}
        |}
      """.stripMargin)
    val schema1 = Field.build("top", input1, enforceValuePresence = false)

    val input2 = SpecHelpers.parseSchema(
      """
        |{"type": "object",
        |"properties": {
        |  "arrayKey": {
        |    "type": "array",
        |    "items": {
        |      "type": "number"
        |    }
        |  }
        |}
        |}
      """.stripMargin)
    val schema2 = Field.build("top", input2, enforceValuePresence = false)
    Migrations.mergeSchemas(schema1, schema2) should beRight(schema2)
    Migrations.assessSchemaMigration(schema1, schema2).map(_.toString) shouldEqual Set("Type widening from Long to Double at /arrayKey/[arrayDown]")
  }

  //    Produce migration for nullables AND invalid type change in arrays $e7
  def e7 = {
    val input1 = SpecHelpers.parseSchema(
      """
        |{"type": "object",
        |"properties": {
        |  "arrayKey": {
        |    "type": "array",
        |    "items": {
        |      "type": ["null", "string"]
        |    }
        |  }
        |}
        |}
      """.stripMargin)
    val schema1 = Field.build("top", input1, enforceValuePresence = false)

    val input2 = SpecHelpers.parseSchema(
      """
        |{"type": "object",
        |"properties": {
        |  "arrayKey": {
        |    "type": "array",
        |    "items": {
        |      "type": "number"
        |    }
        |  }
        |}
        |}
      """.stripMargin)
    val schema2 = Field.build("top", input2, enforceValuePresence = false)

    Migrations.mergeSchemas(schema1, schema2).leftMap(_.toString) should beLeft("Incompatible type change String to Double at /arrayKey/[arrayDown]")
    Migrations.assessSchemaMigration(schema1, schema2).map(_.toString).toSet shouldEqual Set(
      "Incompatible type change String to Double at /arrayKey/[arrayDown]",
      "Changing nullable property to required at /arrayKey/[arrayDown]"
    )
  }

  //  Produce migration structs in nested arrays $e8
  def e8 = {
    val input1 = SpecHelpers.parseSchema(
      """
        |{"type": "object",
        |"properties": {
        |  "arrayKey": {
        |    "type": "array",
        |    "items": {
        |         "type": "object",
        |         "properties": {
        |           "nestedKey1": { "type": "string" },
        |           "nestedKey2": { "type": ["integer", "null"] },
        |           "nestedKey3": { "type": "boolean" }
        |         },
        |         "required": ["nestedKey3"]
        |       }
        |    }
        |}
        |}
      """.stripMargin)
    val schema1 = Field.build("top", input1, enforceValuePresence = false)

    val input2 = SpecHelpers.parseSchema(
      """
        |{"type": "object",
        |"properties": {
        |  "arrayKey": {
        |    "type": "array",
        |    "items": {
        |         "type": "object",
        |         "properties": {
        |           "nestedKey1": { "type": "string" },
        |           "nestedKey2": { "type": ["integer", "null"] },
        |           "nestedKey3": { "type": "boolean" },
        |           "nestedKey4": { "type": "boolean" }
        |         },
        |         "required": ["nestedKey3"]
        |       }
        |    }
        |}
        |}
      """.stripMargin)
    val schema2 = Field.build("top", input2, enforceValuePresence = false)
    Migrations.mergeSchemas(schema1, schema2).leftMap(_.toString) should beRight(schema2)
    Migrations.assessSchemaMigration(schema1, schema2).map(_.toString) shouldEqual
      Set("Nested object key addition at /arrayKey/[arrayDown]/nestedKey4")
  }

  //  Produce migration for removal of nested key in object in array $e9
  def e9 = {
    val input1 = SpecHelpers.parseSchema(
      """
        |{"type": "object",
        |"properties": {
        |  "arrayKey": {
        |    "type": "array",
        |    "items": {
        |         "type": "object",
        |         "properties": {
        |           "nestedKey1": { "type": "string" },
        |           "nestedKey2": { "type": ["integer", "null"] },
        |           "nestedKey3": { "type": "boolean" }
        |         },
        |         "required": ["nestedKey3"]
        |       }
        |    }
        |}
        |}
      """.stripMargin)
    val schema1 = Field.build("top", input1, enforceValuePresence = false)

    val input2 = SpecHelpers.parseSchema(
      """
        |{"type": "object",
        |"properties": {
        |  "arrayKey": {
        |    "type": "array",
        |    "items": {
        |         "type": "object",
        |         "properties": {
        |           "nestedKey1": { "type": "string" },
        |           "nestedKey2": { "type": ["integer", "null"] }
        |         },
        |         "required": ["nestedKey3"]
        |       }
        |    }
        |}
        |}
      """.stripMargin)
    val schema2 = Field.build("top", input2, enforceValuePresence = false)

    Migrations.mergeSchemas(schema1, schema2).leftMap(_.toString) should beRight(schema1)
    Migrations.assessSchemaMigration(schema1, schema2).map(_.toString) shouldEqual
      Set("Key removal at /arrayKey/[arrayDown]/nestedKey3")
  }

  //    Produce migration for nullables arrays $e10
  def e10 = {
    val input1 = SpecHelpers.parseSchema(
      """
        |{"type": "object",
        |"properties": {
        |  "arrayKey": {
        |    "type": "array",
        |    "items": {
        |      "type": ["integer", "null"]
        |    }
        |  }
        |}
        |}
      """.stripMargin)
    val schema1 = Field.build("top", input1, enforceValuePresence = false)

    val input2 = SpecHelpers.parseSchema(
      """
        |{"type": "object",
        |"properties": {
        |  "arrayKey": {
        |    "type": "array",
        |    "items": {
        |      "type": ["integer"]
        |    }
        |  }
        |}
        |}
      """.stripMargin)
    val schema2 = Field.build("top", input2, enforceValuePresence = false)

    Migrations.mergeSchemas(schema1, schema2).leftMap(_.toString) should beRight(schema2)
    Migrations.assessSchemaMigration(schema1, schema2).map(_.toString) shouldEqual Set(
      "Changing nullable property to required at /arrayKey/[arrayDown]"
    )
  }

  //  Produce migration for simple type widening in nested fields
  def e11 = {
    val input1 = SpecHelpers.parseSchema(
      """
        |{"type": "object",
        |"properties": {
        |  "objectKey": {
        |      "type": "object",
        |      "properties": {
        |        "nestedKey1": {
        |            "type": "number",
        |            "multipleOf": 16
        |         },
        |        "nestedKey2": { "type": "string" }
        |      }
        |    }
        |  }
        |}
      """.stripMargin)
    val schema1 = Field.build("top", input1, enforceValuePresence = false)

    val input2 = SpecHelpers.parseSchema(
      """
        |{"type": "object",
        |"properties": {
        |  "objectKey": {
        |      "type": "object",
        |      "properties": {
        |        "nestedKey1": {
        |         "type": "number",
        |         "multipleOf": 0.1
        |        },
        |        "nestedKey2": { "type": "string" }
        |      }
        |    }
        |  }
        |}
      """.stripMargin)
    val schema2 = Field.build("top", input2, enforceValuePresence = false)
    Migrations.mergeSchemas(schema1, schema2).leftMap(_.toString) should beRight(schema2)
    Migrations.assessSchemaMigration(schema1, schema2).map(_.toString) shouldEqual Set("Type widening from Long to Double at /objectKey/nestedKey1")
  }

  //    Produce migration for nullables AND invalid type change in fields
  def e12 = {
    val input2 = SpecHelpers.parseSchema(
      """
        |{"type": "object",
        |"properties": {
        |  "stringKey": {
        |    "type": "string"
        |  },
        |  "objectKey": {
        |    "type": "object",
        |    "properties": {
        |      "nestedKey1": { "type": ["integer"] },
        |      "nestedKey2": { "type": ["integer", "null"] },
        |      "nestedKey3": { "type": "boolean" }
        |    }
        |  }
        |}
        |}
      """.stripMargin)
    val schema2 = Field.build("top", input2, enforceValuePresence = false)

    Migrations.mergeSchemas(leanBase, schema2).leftMap(_.toString) should beLeft("Incompatible type change String to Long at /objectKey/nestedKey1")
    Migrations.assessSchemaMigration(leanBase, schema2).map(_.toString) shouldEqual Set(
      "Incompatible type change String to Long at /objectKey/nestedKey1",
      "Changing required property to nullable at /objectKey/nestedKey3")
  }

  //   Produce migration for type widening in top-level fields
  def e13 = {
    val input1 = SpecHelpers.parseSchema(
      """
        |{"type": "object",
        |"properties": {
        |  "numKey": {
        |    "type": "integer",
        |     "minimum": 150,
        |     "maximum": 200,
        |     "multipleOf": 10
        |  }
        |}
        |}
      """.stripMargin)
    val schema1 = Field.build("top", input1, enforceValuePresence = false)

    val input2 = SpecHelpers.parseSchema(
      """
        |{"type": "object",
        |"properties": {
        |  "numKey": {
        |    "type": "integer"
        |  }
        |}
        |}
      """.stripMargin)
    val schema2 = Field.build("top", input2, enforceValuePresence = false)

    Migrations.mergeSchemas(schema1, schema2).leftMap(_.toString) should beRight(schema2)
    Migrations.mergeSchemas(schema2, schema1).leftMap(_.toString) should beRight(schema2)
    Migrations.assessSchemaMigration(schema1, schema2).map(_.toString) shouldEqual Set("Type widening from Integer to Long at /numKey")
  }

  //  Produce every possible migration for every possible type widening
  def e14 = {
    val input1 = SpecHelpers.parseSchema(
      """
        |{"type": "object",
        |"properties": {
        |  "intKey": {
        |    "type": "integer",
        |     "minimum": 150,
        |     "maximum": 200,
        |     "multipleOf": 10
        |  },
        |  "longKey": {
        |    "type": "integer",
        |    "maximum": 9223372036854775808
        |  }
        |}
        |}
      """.stripMargin)
    val schema1 = Field.build("top", input1, enforceValuePresence = false)

    val input2 = SpecHelpers.parseSchema(
      """
        |{"type": "object",
        |"properties": {
        |  "intKey": {
        |    "type": "integer",
        |    "maximum": 9223372036854775808
        |  },
        |  "longKey": {
        |      "type": "number"
        |  }
        |}
        |}
      """.stripMargin)
    val schema2 = Field.build("top", input2, enforceValuePresence = false)

    Migrations.mergeSchemas(schema1, schema2).leftMap(_.toString) should beRight(schema2)
    Migrations.mergeSchemas(schema2, schema1).leftMap(_.toString) should beRight(schema2)
    Migrations.assessSchemaMigration(schema1, schema2).map(_.toString) shouldEqual Set(
      "Type widening from Integer to Long at /intKey", "Type widening from Long to Double at /longKey")
  }

  //Suggest version change correctly $e15
  def e15 = {
    val patch: ParquetSchemaMigrations = Set(NullableRequired(Nil))
    val major: ParquetSchemaMigrations = Set(TopLevelKeyAddition(Nil, Type.Boolean))

    isSchemaMigrationBreakingFromMigrations(major) shouldEqual true
    isSchemaMigrationBreakingFromMigrations(patch) shouldEqual false
  }
}

object MigrationSpec {
  val leanBase: Field = Field.build("top", SpecHelpers.parseSchema(
    """
      |{"type": "object",
      |"properties": {
      |  "stringKey": {
      |    "type": "string",
      |    "maxLength": 500
      |  },
      |  "objectKey": {
      |    "type": "object",
      |    "properties": {
      |      "nestedKey1": { "type": "string" },
      |      "nestedKey2": { "type": ["integer", "null"] },
      |      "nestedKey3": { "type": "boolean" }
      |    },
      |    "required": ["nestedKey3"]
      |  }
      |}
      |}
      """.stripMargin), enforceValuePresence = false)
}