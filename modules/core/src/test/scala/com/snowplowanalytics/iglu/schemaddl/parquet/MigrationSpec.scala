package com.snowplowanalytics.iglu.schemaddl.parquet

import cats.syntax.either._
import com.snowplowanalytics.iglu.schemaddl.SpecHelpers
import com.snowplowanalytics.iglu.schemaddl.parquet.Migrations._
import com.snowplowanalytics.iglu.schemaddl.parquet.Type.Struct

class MigrationSpec extends org.specs2.Specification {

  import MigrationSpec._

  def is =
    s2"""
        Produce no migration for the same schemas $e1
        Produce migration for new nested fields $e2
        Error for type casting in nested fields $e3
        Produce migration for removal of nested fields $e4
        Produce migration for new top-level fields $e5
        Error for type casting in top-level fields $e6
        Produce migration for removal of top-level fields $e7
        Error with invalid type change in arrays AND nullable change $e8              
        Produce migration for adding fields in structs in arrays $e9
        Produce migration for removal fields in structs in arrays $e10
        Error for type casting in nested arrays $e11
        Produce migration for nullables arrays $e12
        Preserve ordering in nested arrays and structs $e13
        Suggest version change correctly $e14
        Collapse field name collisions $e15
        Drop not null constraint when field is removed in next generation $e16
        Drop not null constraint when field is added in next generation $e17
  """

  def e1 = {
    Migrations.mergeSchemas(leanBase, leanBase) should beRight(leanBase) and (
      Migrations.assessSchemaMigration(leanBase, leanBase) shouldEqual Set.empty[ParquetMigration])
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
        |    }
        |  }
        |}
        |}
      """.stripMargin)
    val schema2 = Field.build("top", input2, enforceValuePresence = false).get
    Migrations.mergeSchemas(leanBase, schema2) should beRight(schema2) and (
      Migrations.assessSchemaMigration(leanBase, schema2).map(_.toString) shouldEqual Set("Schema key addition at /objectKey/string2Key")
      )
  }

  // Error for type casting in nested fields $e3
  def e3 = {
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
        |      "nestedKey1": { "type": "integer" },
        |      "nestedKey2": { "type": ["integer", "null"] },
        |      "nestedKey3": { "type": "boolean" }
        |    }
        |  }
        |}
        |}
      """.stripMargin)
    val schema2 = Field.build("top", input2, enforceValuePresence = false).get
    Migrations.mergeSchemas(leanBase, schema2).leftMap(_.map(_.toString)) should beLeft(List("Incompatible type change String to Long at /objectKey/nestedKey1")) and (
      Migrations.assessSchemaMigration(leanBase, schema2).map(_.toString) shouldEqual Set("Incompatible type change String to Long at /objectKey/nestedKey1")
      )
  }

  //  Produce migration for removal of nested fields $e4
  def e4 = {
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
        |      "nestedKey2": { "type": ["integer", "null"] }
        |    }
        |  }
        |}
        |}
      """.stripMargin)
    val schema2 = Field.build("top", input2, enforceValuePresence = false).get

    Migrations.mergeSchemas(leanBase, schema2) should beRight(leanBase) and (
      Migrations.assessSchemaMigration(leanBase, schema2).map(_.toString) shouldEqual Set("Key removal at /objectKey/nestedKey3"))
  }

  //  Produce migration for new top-level fields $e5
  def e5 = {
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
        |    }
        |  }
        |}
        |}
      """.stripMargin)
    val schema2 = Field.build("top", input2, enforceValuePresence = false).get
    Migrations.mergeSchemas(leanBase, schema2) should beRight(schema2) and (
      Migrations.assessSchemaMigration(leanBase, schema2).map(_.toString) shouldEqual Set("Schema key addition at /string2Key")
      )
  }

  //  Error for type casting in top-level fields $e6
  def e6 = {
    val input2 = SpecHelpers.parseSchema(
      """
        |{"type": "object",
        |"properties": {
        |  "stringKey": {
        |    "type": "integer"
        |  },
        |  "objectKey": {
        |    "type": "object",
        |    "properties": {
        |      "nestedKey1": { "type": "string" },
        |      "nestedKey2": { "type": ["integer", "null"] },
        |      "nestedKey3": { "type": "boolean" }
        |    }
        |  }
        |}
        |}
      """.stripMargin)
    val schema2 = Field.build("top", input2, enforceValuePresence = false).get
    Migrations.mergeSchemas(leanBase, schema2).leftMap(_.toString) should beLeft(Set("Incompatible type change String to Long at /stringKey"))
    Migrations.assessSchemaMigration(leanBase, schema2).map(_.toString) shouldEqual Set("Incompatible type change String to Long at /stringKey")
  }

  //  Produce migration for removal of top-level fields $e7
  def e7 = {
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
        |    }
        |  }
        |}
        |}
      """.stripMargin)
    val schema2 = Field.build("top", input2, enforceValuePresence = false).get
    Migrations.mergeSchemas(leanBase, schema2) should beRight(leanBase) and (
      Migrations.assessSchemaMigration(leanBase, schema2).map(_.toString) shouldEqual Set("Key removal at /stringKey"))
  }

  //  Error with invalid type change in arrays AND nullable change $e8
  def e8 = {
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
    val schema1 = Field.build("top", input1, enforceValuePresence = false).get

    val input2 = SpecHelpers.parseSchema(
      """
        |{"type": "object",
        |"properties": {
        |  "arrayKey": {
        |    "type": "array",
        |    "items": {
        |      "type": ["string"]
        |    }
        |  }
        |}
        |}
      """.stripMargin)
    val schema2 = Field.build("top", input2, enforceValuePresence = false).get

    Migrations.mergeSchemas(schema1, schema2).leftMap(_.map(_.toString)) should beLeft(List(
      "Incompatible type change Long to String at /arrayKey/[arrayDown]"
    )) and (
      Migrations.assessSchemaMigration(schema1, schema2).map(_.toString) shouldEqual Set(
        "Changing nullable property to required at /arrayKey/[arrayDown]",
        "Incompatible type change Long to String at /arrayKey/[arrayDown]"
      ))
  }

  //  Produce migration for adding fields in structs in arrays $e9
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
        |           "nestedKey2": { "type": ["integer", "null"] }
        |         }
        |       }
        |    }
        |}
        |}
      """.stripMargin)
    val schema1 = Field.build("top", input1, enforceValuePresence = false).get

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
        |           "nestedKey3": { "type": "boolean" }
        |         }
        |       }
        |    }
        |}
        |}
      """.stripMargin)
    val schema2 = Field.build("top", input2, enforceValuePresence = false).get

    Migrations.mergeSchemas(schema1, schema2).leftMap(_.toString) should beRight(schema2) and (
      Migrations.assessSchemaMigration(schema1, schema2).map(_.toString) shouldEqual
        Set("Schema key addition at /arrayKey/[arrayDown]/nestedKey3")
      )
  }

  //  Produce migration for removal fields in structs in arrays $e10
  def e10 = {
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
        |           "nestedKey2": { "type": ["integer", "null"] }
        |         }
        |       }
        |    }
        |}
        |}
      """.stripMargin)
    val schema1 = Field.build("top", input1, enforceValuePresence = false).get

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
        |           "nestedKey3": { "type": "boolean" }
        |         }
        |       }
        |    }
        |}
        |}
      """.stripMargin)
    val schema2 = Field.build("top", input2, enforceValuePresence = false).get

    Migrations.mergeSchemas(schema2, schema1).leftMap(_.toString) should beRight(schema2) and (
      Migrations.assessSchemaMigration(schema2, schema1).map(_.toString) shouldEqual
        Set("Key removal at /arrayKey/[arrayDown]/nestedKey3")
      )
  }

  //  Error for type casting in nested arrays $e11
  def e11 = {
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
        |           "nestedKey2": { "type": ["integer", "null"] }
        |         }
        |       }
        |    }
        |}
        |}
      """.stripMargin)
    val schema1 = Field.build("top", input1, enforceValuePresence = false).get

    val input2 = SpecHelpers.parseSchema(
      """
        |{"type": "object",
        |"properties": {
        |  "arrayKey": {
        |    "type": "array",
        |    "items": {
        |         "type": "object",
        |         "properties": {
        |           "nestedKey1": { "type": "integer" },
        |           "nestedKey2": { "type": ["integer", "null"] }
        |         }
        |       }
        |    }
        |}
        |}
      """.stripMargin)
    val schema2 = Field.build("top", input2, enforceValuePresence = false).get

    Migrations.mergeSchemas(schema1, schema2).leftMap(_.map(_.toString)) should beLeft(List(
      "Incompatible type change String to Long at /arrayKey/[arrayDown]/nestedKey1"
    )) and (
      Migrations.assessSchemaMigration(schema1, schema2).map(_.toString) shouldEqual Set(
        "Incompatible type change String to Long at /arrayKey/[arrayDown]/nestedKey1"
      ))
  }

  //  Produce migration for nullables arrays $e12
  def e12 = {
    val input1 = SpecHelpers.parseSchema(
      """
        |{"type": "object",
        |"properties": {
        |  "arrayKey": {
        |    "type": "array",
        |    "items": {
        |         "type": "object",
        |         "properties": {
        |           "nestedKey1": { "type": "integer" },
        |           "nestedKey2": { "type": "integer" }
        |         }
        |       }
        |    }
        |}
        |}
      """.stripMargin)
    val schema1 = Field.build("top", input1, enforceValuePresence = false).get

    val input2 = SpecHelpers.parseSchema(
      """
        |{"type": "object",
        |"properties": {
        |  "arrayKey": {
        |    "type": "array",
        |    "items": {
        |         "type": "object",
        |         "properties": {
        |           "nestedKey1": { "type": "integer" },
        |           "nestedKey2": { "type": "integer" }
        |         },
        |         "required": ["nestedKey2"]
        |       }
        |    }
        |}
        |}
      """.stripMargin)
    val schema2 = Field.build("top", input2, enforceValuePresence = false).get

    Migrations.mergeSchemas(schema1, schema2).leftMap(_.toString) should beRight(schema1) and (
      Migrations.assessSchemaMigration(schema1, schema2).map(_.toString) shouldEqual Set(
        "Changing nullable property to required at /arrayKey/[arrayDown]/nestedKey2"
      ))
  }
  
  // Preserve ordering in nested arrays and structs $e13
  def e13 = {
    val input1 = SpecHelpers.parseSchema(
      """
        |{"type": "object",
        |"properties": {
        |  "arrayKey": {
        |    "type": "array",
        |    "items": {
        |         "type": "object",
        |         "properties": {
        |           "nestedKey1": { "type": "integer" },
        |           "nestedKey2": { "type": "integer" }
        |         },
        |         "required": ["nestedKey2"]
        |       }
        |    }
        |   },
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
        |  }
        |}
        |}
      """.stripMargin)
    val schema1 = Field.build("top", input1, enforceValuePresence = false).get

    val input2 = SpecHelpers.parseSchema(
      """
        |{"type": "object",
        |"properties": {
        |  "arrayKey": {
        |    "type": "array",
        |    "items": {
        |         "type": "object",
        |         "properties": {
        |           "nestedKey1": { "type": "integer" },
        |           "nestedKey0": { "type": "integer" },
        |           "nestedKey2": { "type": "integer" }
        |         },
        |         "required": ["nestedKey2"]
        |       }}
        |    },
        |  "objectKey": {
        |    "type": "object",
        |    "properties": {
        |      "nestedKey1": { "type": "string" },
        |      "nestedKey0": { "type": "integer" },
        |      "nestedKey2": { "type": ["integer", "null"] },
        |      "nestedKey3": { "type": "boolean" },
        |      "string2Key": {
        |           "type": "string",
        |           "maxLength": 500
        |         }
        |    }
        |}
        |}
      """.stripMargin)
    val schema2 = Field.build("top", input2, enforceValuePresence = false).get

    Migrations.mergeSchemas(schema1, schema2).leftMap(_.toString) should beRight(schema2) and (
      Migrations.assessSchemaMigration(schema1, schema2).map(_.toString) shouldEqual Set(
        "Schema key addition at /arrayKey/[arrayDown]/nestedKey0"
      ))
  }

  //  Suggest version change correctly $e14
  def e14 = {
    val major: ParquetSchemaMigrations = Set(IncompatibleType(List("/"), Type.Boolean, Type.Double))
    val patch: ParquetSchemaMigrations = Set(KeyAddition(Nil, Type.Boolean))

    isSchemaMigrationBreakingFromMigrations(major) shouldEqual true
    isSchemaMigrationBreakingFromMigrations(patch) shouldEqual false
  }
  
  def e15 = {
    val input1 = SpecHelpers.parseSchema(
      """
        |{"type": "object",
        |"properties": {
        |  "collidingKey": { "type": "string" }
        |}
        |}
      """.stripMargin)
    val schema1 = Field.normalize(Field.build("top", input1, enforceValuePresence = false).get)

    val input2 = SpecHelpers.parseSchema(
      """
        |{"type": "object",
        |"properties": {
        |  "colliding_key": { "type": "string" }
        |}
        |}
          """.stripMargin)
    val schema2 =  Field.normalize(Field.build("top", input2, enforceValuePresence = false).get)

    Migrations.mergeSchemas(schema1, schema2).map(
      f => f.fieldType.asInstanceOf[Struct].fields.head.accessors.mkString(",")
    ) should beRight("colliding_key,collidingKey")
  }
  
  def e16 = {
    val input1 = SpecHelpers.parseSchema(
      """
        |{"type": "object",
        |"properties": {
        |  "k1": { "type": "string" } 
        |},
        |"required" : ["k1"]
        |}
    """.stripMargin)
    val schema1 = Field.normalize(Field.build("top", input1, enforceValuePresence = false).get)

    val input2 = SpecHelpers.parseSchema(
      """
        |{"type": "object",
        |"properties": {
        |  "k2": { "type": "string" }
        |}
        |}
        """.stripMargin)
    val schema2 = Field.normalize(Field.build("top", input2, enforceValuePresence = false).get)

    Migrations.mergeSchemas(schema1, schema2).map(
      f => f.fieldType.asInstanceOf[Struct].fields.last.nullability.nullable
    ) should beRight(true)
    
  }

  def e17 = {
    val input1 = SpecHelpers.parseSchema(
      """
        |{"type": "object",
        |"properties": {
        |  "k1": { "type": "string" } 
        |}
        |}
    """.stripMargin)
    val schema1 = Field.normalize(Field.build("top", input1, enforceValuePresence = false).get)

    val input2 = SpecHelpers.parseSchema(
      """
        |{"type": "object",
        |"properties": {
        |  "k2": { "type": "string" }
        |},
        |  "required" : ["k2"]
        |}
        """.stripMargin)
    val schema2 = Field.normalize(Field.build("top", input2, enforceValuePresence = false).get)

    Migrations.mergeSchemas(schema1, schema2).map(
      f => f.fieldType.asInstanceOf[Struct].fields.forall(_.nullability.nullable)
    ) should beRight(true)

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
      |    }
      |  }
      |}
      |}
      """.stripMargin), enforceValuePresence = false).get
}
