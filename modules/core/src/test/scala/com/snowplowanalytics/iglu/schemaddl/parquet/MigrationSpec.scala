package com.snowplowanalytics.iglu.schemaddl.parquet

import cats.data.NonEmptyVector
import cats.syntax.either._

import com.snowplowanalytics.iglu.schemaddl.SpecHelpers
import com.snowplowanalytics.iglu.schemaddl.parquet.Migrations._
import com.snowplowanalytics.iglu.schemaddl.parquet.Type.Struct
import com.snowplowanalytics.iglu.schemaddl.parquet.Type.Nullability.{Required, Nullable}

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
        Suggest version change correctly $e13
        Collapse field name collisions $e14
        Drop not null constraint when field is removed in next generation $e15
        Drop not null constraint when field is added in next generation $e16
        Preserve ordering of fields in a struct $e17
        Preserve ordering in nested arrays and structs $e18
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
    val schema2 = Field.normalize(Field.build("top", input2, enforceValuePresence = false).get)
    Migrations.mergeSchemas(leanBase, schema2) should beRight(schema2) and (
      Migrations.assessSchemaMigration(leanBase, schema2).map(_.toString) shouldEqual Set("Schema key addition at /object_key/string2_key")
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
    val schema2 = Field.normalize(Field.build("top", input2, enforceValuePresence = false).get)
    Migrations.mergeSchemas(leanBase, schema2).leftMap(_.map(_.toString)) should beLeft(List("Incompatible type change String to Long at /object_key/nested_key1")) and (
      Migrations.assessSchemaMigration(leanBase, schema2).map(_.toString) shouldEqual Set("Incompatible type change String to Long at /object_key/nested_key1")
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
    val schema2 = Field.normalize(Field.build("top", input2, enforceValuePresence = false).get)

    Migrations.mergeSchemas(leanBase, schema2) should beRight(leanBase) and (
      Migrations.assessSchemaMigration(leanBase, schema2).map(_.toString) shouldEqual Set("Key removal at /object_key/nested_key3"))
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
        |  "stringKey2": {
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
    val schema2 = Field.normalize(Field.build("top", input2, enforceValuePresence = false).get)
    Migrations.mergeSchemas(leanBase, schema2) should beRight(schema2) and (
      Migrations.assessSchemaMigration(leanBase, schema2).map(_.toString) shouldEqual Set("Schema key addition at /string_key2")
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
    val schema2 = Field.normalize(Field.build("top", input2, enforceValuePresence = false).get)
    Migrations.mergeSchemas(leanBase, schema2).leftMap(_.toString) should beLeft(Set("Incompatible type change String to Long at /string_key"))
    Migrations.assessSchemaMigration(leanBase, schema2).map(_.toString) shouldEqual Set("Incompatible type change String to Long at /string_key")
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
    val schema2 = Field.normalize(Field.build("top", input2, enforceValuePresence = false).get)
    Migrations.mergeSchemas(leanBase, schema2) should beRight(leanBase) and (
      Migrations.assessSchemaMigration(leanBase, schema2).map(_.toString) shouldEqual Set("Key removal at /string_key"))
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
    val schema1 = Field.normalize(Field.build("top", input1, enforceValuePresence = false).get)

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
    val schema2 = Field.normalize(Field.build("top", input2, enforceValuePresence = false).get)

    Migrations.mergeSchemas(schema1, schema2) should beRight(schema2) and (
      Migrations.assessSchemaMigration(schema1, schema2).map(_.toString) shouldEqual
        Set("Schema key addition at /array_key/[arrayDown]/nested_key3")
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
    val schema1 = Field.normalize(Field.build("top", input1, enforceValuePresence = false).get)

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
    val schema2 = Field.normalize(Field.build("top", input2, enforceValuePresence = false).get)

    Migrations.mergeSchemas(schema2, schema1) should beRight(schema2) and (
      Migrations.assessSchemaMigration(schema2, schema1).map(_.toString) shouldEqual
        Set("Key removal at /array_key/[arrayDown]/nested_key3")
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
    val schema1 = Field.normalize(Field.build("top", input1, enforceValuePresence = false).get)

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
    val schema2 = Field.normalize(Field.build("top", input2, enforceValuePresence = false).get)

    Migrations.mergeSchemas(schema1, schema2) should beRight(schema1) and (
      Migrations.assessSchemaMigration(schema1, schema2).map(_.toString) shouldEqual Set(
        "Changing nullable property to required at /array_key/[arrayDown]/nested_key2"
      ))
  }

  //  Suggest version change correctly $e13
  def e13 = {
    val major: ParquetSchemaMigrations = Set(IncompatibleType(List("/"), Type.Boolean, Type.Double))
    val patch: ParquetSchemaMigrations = Set(KeyAddition(Nil, Type.Boolean))

    isSchemaMigrationBreakingFromMigrations(major) shouldEqual true
    isSchemaMigrationBreakingFromMigrations(patch) shouldEqual false
  }
  
  def e14 = {
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
      f => f.fieldType.asInstanceOf[Struct].fields.head.accessors
    ) should beRight(beEqualTo(Set("colliding_key", "collidingKey")))
  }
  
  def e15 = {
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

  def e16 = {
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

  // Preserve ordering of fields in a struct $e17
  def e17 = {
    val struct1 = Type.Struct(
      NonEmptyVector.of(
        Field("vvv", Type.String, Nullable),
        Field("xxx", Type.String, Nullable),
        Field("zzz", Type.String, Nullable),
      )
    )

    val struct2 = Type.Struct(
      NonEmptyVector.of(
        Field("vvv", Type.String, Required),
        Field("www", Type.String, Required),
        Field("xxx", Type.String, Required),
        Field("yyy", Type.String, Required),
        Field("zzz", Type.String, Required),
      )
    )

    val expectedStruct = Type.Struct(
      NonEmptyVector.of(
        // original fields
        Field("vvv", Type.String, Nullable),
        Field("xxx", Type.String, Nullable),
        Field("zzz", Type.String, Nullable),
        // added fields
        Field("www", Type.String, Nullable),
        Field("yyy", Type.String, Nullable),
      )
    )

    val field1 = Field("top", struct1, Required)
    val field2 = Field("top", struct2, Required)
    val expected = Field("top", expectedStruct, Required)

    Migrations.mergeSchemas(field1, field2) must beRight(expected)
  }
  
  // Preserve ordering in nested arrays and structs $e18
  def e18 = {
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
        |    },
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
    val schema1 = Field.normalize((Field.build("top", input1, enforceValuePresence = true).get))

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
        |       }},
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
        |  }
        |}
        |}
      """.stripMargin)
    val schema2 = Field.normalize((Field.build("top", input2, enforceValuePresence = true).get))

    val expected = {

      val arrayStruct = Struct(
        NonEmptyVector.of(
          Field("nested_key1", Type.Long, Nullable, Set("nestedKey1")),
          Field("nested_key2", Type.Long, Required, Set("nestedKey2")),
          Field("nested_key0", Type.Long, Nullable, Set("nestedKey0"))
        )
      )

      val objectStruct = Struct(
        NonEmptyVector.of(
          Field("nested_key1", Type.String, Nullable, Set("nestedKey1")),
          Field("nested_key2", Type.Long, Nullable, Set("nestedKey2")),
          Field("nested_key3", Type.Boolean, Nullable, Set("nestedKey3")),
          Field("string2_key", Type.String, Nullable, Set("string2Key")),
          Field("nested_key0", Type.Long, Nullable, Set("nestedKey0"))
        )
      )

      val topStruct = Struct(
        NonEmptyVector.of(
          Field("array_key", Type.Array(arrayStruct, Required), Nullable, Set("arrayKey")),
          Field("object_key", objectStruct, Nullable, Set("objectKey"))
        )
      )
      Field("top", topStruct, Required)
    }

    Migrations.mergeSchemas(schema1, schema2) should beRight(expected) and (
      Migrations.assessSchemaMigration(schema1, schema2).map(_.toString) shouldEqual Set(
        "Schema key addition at /array_key/[arrayDown]/nested_key0",
        "Schema key addition at /object_key/nested_key0"
      ))
  }

}

object MigrationSpec {
  val leanBase: Field = Field.normalize(Field.build("top", SpecHelpers.parseSchema(
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
      """.stripMargin), enforceValuePresence = false).get)
}
