package com.snowplowanalytics.iglu.schemaddl.redshift

import cats.data.NonEmptyList
import cats.syntax.either._
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaMap, SchemaVer, SelfDescribingSchema}
import org.specs2.mutable.Specification
import com.snowplowanalytics.iglu.schemaddl.SpecHelpers._
import com.snowplowanalytics.iglu.schemaddl.redshift.ShredModel.{GoodModel, RecoveryModel}
import com.snowplowanalytics.iglu.schemaddl.redshift.ShredModelSpec.{ModelMergeOps, dummyKey, dummyKey1, dummyKey2, dummyModel}
import com.snowplowanalytics.iglu.schemaddl.redshift.internal.Migrations.NullableRequired
import io.circe.literal._

class ShredModelSpec extends Specification {
  "model sql representation" should {
    "render shred table, ordering column by nullability" in {
      dummyModel.toTableSql("custom") must beEqualTo(
        """CREATE TABLE IF NOT EXISTS custom.com_acme_example_1 (
          |  "schema_vendor"           VARCHAR(128)  ENCODE ZSTD NOT NULL,
          |  "schema_name"             VARCHAR(128)  ENCODE ZSTD NOT NULL,
          |  "schema_format"           VARCHAR(128)  ENCODE ZSTD NOT NULL,
          |  "schema_version"          VARCHAR(128)  ENCODE ZSTD NOT NULL,
          |  "root_id"                 CHAR(36)      ENCODE RAW  NOT NULL,
          |  "root_tstamp"             TIMESTAMP     ENCODE ZSTD NOT NULL,
          |  "ref_root"                VARCHAR(255)  ENCODE ZSTD NOT NULL,
          |  "ref_tree"                VARCHAR(1500) ENCODE ZSTD NOT NULL,
          |  "ref_parent"              VARCHAR(255)  ENCODE ZSTD NOT NULL,
          |  "a_field.d_field"         VARCHAR(4096) ENCODE ZSTD NOT NULL,
          |  "e_field.g_field"         VARCHAR(4096) ENCODE ZSTD NOT NULL,
          |  "a_field.b_field"         VARCHAR(4096) ENCODE ZSTD,
          |  "a_field.c_field.d_field" VARCHAR(4096) ENCODE ZSTD,
          |  "a_field.c_field.e_field" VARCHAR(4096) ENCODE ZSTD,
          |  "b_field"                 BIGINT        ENCODE ZSTD,
          |  "bar"                     SMALLINT      ENCODE ZSTD,
          |  "c_field"                 BIGINT        ENCODE ZSTD,
          |  "d_field.e_field"         VARCHAR(4096) ENCODE ZSTD,
          |  "d_field.f_field"         VARCHAR(4096) ENCODE ZSTD,
          |  "e_field.f_field"         VARCHAR(4096) ENCODE ZSTD,
          |  "f_field"                 VARCHAR(4096) ENCODE ZSTD,
          |  "foo"                     VARCHAR(20)   ENCODE ZSTD,
          |  "g_field"                 VARCHAR(4096) ENCODE ZSTD,
          |  FOREIGN KEY (root_id) REFERENCES custom.events(event_id)
          |)
          |DISTSTYLE KEY
          |DISTKEY (root_id)
          |SORTKEY (root_tstamp);
          |
          |COMMENT ON TABLE custom.com_acme_example_1 IS 'iglu:com.acme/example/jsonschema/1-0-0';
          |""".stripMargin)
    }
    "render recovery table" in {
      dummyModel.makeRecovery(NonEmptyList.one(NullableRequired(
        ShredModelEntry("/".jsonPointer, json"""{"type": "string"}""".schema)
      ))).toTableSql("custom") must beEqualTo(
        """CREATE TABLE IF NOT EXISTS custom.com_acme_example_1_0_0_recovered_235658654 (
          |  "schema_vendor"           VARCHAR(128)  ENCODE ZSTD NOT NULL,
          |  "schema_name"             VARCHAR(128)  ENCODE ZSTD NOT NULL,
          |  "schema_format"           VARCHAR(128)  ENCODE ZSTD NOT NULL,
          |  "schema_version"          VARCHAR(128)  ENCODE ZSTD NOT NULL,
          |  "root_id"                 CHAR(36)      ENCODE RAW  NOT NULL,
          |  "root_tstamp"             TIMESTAMP     ENCODE ZSTD NOT NULL,
          |  "ref_root"                VARCHAR(255)  ENCODE ZSTD NOT NULL,
          |  "ref_tree"                VARCHAR(1500) ENCODE ZSTD NOT NULL,
          |  "ref_parent"              VARCHAR(255)  ENCODE ZSTD NOT NULL,
          |  "a_field.d_field"         VARCHAR(4096) ENCODE ZSTD NOT NULL,
          |  "e_field.g_field"         VARCHAR(4096) ENCODE ZSTD NOT NULL,
          |  "a_field.b_field"         VARCHAR(4096) ENCODE ZSTD,
          |  "a_field.c_field.d_field" VARCHAR(4096) ENCODE ZSTD,
          |  "a_field.c_field.e_field" VARCHAR(4096) ENCODE ZSTD,
          |  "b_field"                 BIGINT        ENCODE ZSTD,
          |  "bar"                     SMALLINT      ENCODE ZSTD,
          |  "c_field"                 BIGINT        ENCODE ZSTD,
          |  "d_field.e_field"         VARCHAR(4096) ENCODE ZSTD,
          |  "d_field.f_field"         VARCHAR(4096) ENCODE ZSTD,
          |  "e_field.f_field"         VARCHAR(4096) ENCODE ZSTD,
          |  "f_field"                 VARCHAR(4096) ENCODE ZSTD,
          |  "foo"                     VARCHAR(20)   ENCODE ZSTD,
          |  "g_field"                 VARCHAR(4096) ENCODE ZSTD,
          |  FOREIGN KEY (root_id) REFERENCES custom.events(event_id)
          |)
          |DISTSTYLE KEY
          |DISTKEY (root_id)
          |SORTKEY (root_tstamp);
          |
          |COMMENT ON TABLE custom.com_acme_example_1_0_0_recovered_235658654 IS 'iglu:com.acme/example/jsonschema/1-0-0';
          |""".stripMargin)
    }
  }

  "factory method" should {
    "transform events" in {

      dummyModel.jsonToStrings(
        json"""{
            "a_field": {"d_field":  "zzzz", "g_field":  "gggg"},
            "e_field": {"g_field":  "xxxx"},
            "bar": "ssss"
            }""") must beEqualTo(List(
        "zzzz", "xxxx", "\\N", "\\N", "\\N", "\\N", "ssss", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N"))
    }
    "transform events with special characters" in {
      dummyModel.jsonToStrings(
        json"""{
      "a_field": {"d_field":  "z\tzzz", "g_field":  "gggg"},
      "e_field": {"g_field":  "xxxx"},
      "bar": "ssss"
      }""") must beEqualTo(List(
        "z zzz", "xxxx", "\\N", "\\N", "\\N", "\\N", "ssss", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N"))
    }
  }

  "model migrations" should {
    "should merge with varchar widening" in {
      val s1 = ShredModel.good(dummyKey,
        json"""{
       "type": "object",
       "properties": {
         "foo": {
           "type": "string",
           "maxLength": 20
         }}
      }""".schema)
      val s2 = ShredModel.good(dummyKey1,
        json"""{
       "type": "object",
       "properties": {
         "foo": {
           "type": "string",
           "maxLength": 30
         }}
      }""".schema)
      s1.merge(s2).toTestString must beRight((
        """CREATE TABLE IF NOT EXISTS s.com_acme_example_1 (
          |  "schema_vendor"  VARCHAR(128)  ENCODE ZSTD NOT NULL,
          |  "schema_name"    VARCHAR(128)  ENCODE ZSTD NOT NULL,
          |  "schema_format"  VARCHAR(128)  ENCODE ZSTD NOT NULL,
          |  "schema_version" VARCHAR(128)  ENCODE ZSTD NOT NULL,
          |  "root_id"        CHAR(36)      ENCODE RAW  NOT NULL,
          |  "root_tstamp"    TIMESTAMP     ENCODE ZSTD NOT NULL,
          |  "ref_root"       VARCHAR(255)  ENCODE ZSTD NOT NULL,
          |  "ref_tree"       VARCHAR(1500) ENCODE ZSTD NOT NULL,
          |  "ref_parent"     VARCHAR(255)  ENCODE ZSTD NOT NULL,
          |  "foo"            VARCHAR(30)   ENCODE ZSTD,
          |  FOREIGN KEY (root_id) REFERENCES s.events(event_id)
          |)
          |DISTSTYLE KEY
          |DISTKEY (root_id)
          |SORTKEY (root_tstamp);
          |
          |COMMENT ON TABLE s.com_acme_example_1 IS 'iglu:com.acme/example/jsonschema/1-0-1';
          |
          |-- WARNING: only apply this file to your database if the following SQL returns the expected:
          |--
          |-- SELECT pg_catalog.obj_description(c.oid) FROM pg_catalog.pg_class c WHERE c.relname = 'com_acme_example_1';
          |--  obj_description
          |-- -----------------
          |--  iglu:com.acme/example/jsonschema/1-0-0
          |--  (1 row)
          |
          |  ALTER TABLE s.com_acme_example_1
          |    ALTER COLUMN "foo" TYPE VARCHAR(30);
          |
          |-- NO ADDED COLUMNS CAN BE EXPRESSED IN SQL MIGRATION
          |
          |COMMENT ON TABLE s.com_acme_example_1 IS 'iglu:com.acme/example/jsonschema/1-0-1';
          |""".stripMargin
        ))
    }

    "should detect int to str conversion errors" in {
      val s1 = SelfDescribingSchema(
        SchemaMap(SchemaKey("dummy-vendor", "dummy-name", "jsonschema", SchemaVer.Full(1, 0, 0))),
        json"""{
              "type": "object",
              "properties": {
                "stringKey": {
                  "type": "string"
                }
              },
              "self": {
                "vendor": "com.snowplowanalytics",
                "name": "json1",
                "format": "jsonschema",
                "version": "1-0-0"
              },
              "$$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#"
            }""".schema)


      val s2 = SelfDescribingSchema(
        SchemaMap(SchemaKey("test.lukasz", "snowman", "jsonschema", SchemaVer.Full(1, 0, 0))),
        json"""{
              "type": "object",
              "properties": {
                "stringKey": {
                  "type": "integer"
                }
              },
              "self": {
                "vendor": "dummy-vendor",
                "name": "dummy-name",
                "format": "jsonschema",
                "version": "1-0-0"
              },
              "$$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#"
            }""".schema)

      isRedshiftMigrationBreaking(List(s2), s1) must beTrue
    }
    
    "should make a recovery model when incompatible encodings are merged" in {

      val s1 = ShredModel.good(dummyKey,
        json"""{
               "type": "object",
               "properties": {
                 "foo": {
                   "type": "string",
                   "maxLength": 20
                 }}
              }""".schema)
      val s2 = ShredModel.good(dummyKey1,
        json"""{
               "type": "object",
               "properties": {
                 "foo": {
                   "type": "number"
                 }}
              }""".schema)

      s1.merge(s2).toTestString must beLeft(
        """CREATE TABLE IF NOT EXISTS s.com_acme_example_1_0_1_recovered_194359593 (
          |  "schema_vendor"  VARCHAR(128)     ENCODE ZSTD NOT NULL,
          |  "schema_name"    VARCHAR(128)     ENCODE ZSTD NOT NULL,
          |  "schema_format"  VARCHAR(128)     ENCODE ZSTD NOT NULL,
          |  "schema_version" VARCHAR(128)     ENCODE ZSTD NOT NULL,
          |  "root_id"        CHAR(36)         ENCODE RAW  NOT NULL,
          |  "root_tstamp"    TIMESTAMP        ENCODE ZSTD NOT NULL,
          |  "ref_root"       VARCHAR(255)     ENCODE ZSTD NOT NULL,
          |  "ref_tree"       VARCHAR(1500)    ENCODE ZSTD NOT NULL,
          |  "ref_parent"     VARCHAR(255)     ENCODE ZSTD NOT NULL,
          |  "foo"            DOUBLE PRECISION ENCODE RAW,
          |  FOREIGN KEY (root_id) REFERENCES s.events(event_id)
          |)
          |DISTSTYLE KEY
          |DISTKEY (root_id)
          |SORTKEY (root_tstamp);
          |
          |COMMENT ON TABLE s.com_acme_example_1_0_1_recovered_194359593 IS 'iglu:com.acme/example/jsonschema/1-0-1';
          |
          |Incompatible types in column foo old RedshiftVarchar(20) new RedshiftDouble""".stripMargin
      )
    }

    "should make a recovery model when not null field is removed" in {
      val s1 = ShredModel.good(dummyKey,
        json"""{
               "type": "object",
               "properties": {
                 "foo": {
                   "type": "string",
                   "maxLength": 20
                 }},
              "required": ["foo"]
              }""".schema)
      val s2 = ShredModel.good(dummyKey1,
        json"""{
               "type": "object",
               "properties": {
                 "foo1": {
                   "type": "number"
                 }}
              }""".schema)

      s1.merge(s2).toTestString must beLeft(
        """CREATE TABLE IF NOT EXISTS s.com_acme_example_1_0_1_recovered_1202144068 (
          |  "schema_vendor"  VARCHAR(128)     ENCODE ZSTD NOT NULL,
          |  "schema_name"    VARCHAR(128)     ENCODE ZSTD NOT NULL,
          |  "schema_format"  VARCHAR(128)     ENCODE ZSTD NOT NULL,
          |  "schema_version" VARCHAR(128)     ENCODE ZSTD NOT NULL,
          |  "root_id"        CHAR(36)         ENCODE RAW  NOT NULL,
          |  "root_tstamp"    TIMESTAMP        ENCODE ZSTD NOT NULL,
          |  "ref_root"       VARCHAR(255)     ENCODE ZSTD NOT NULL,
          |  "ref_tree"       VARCHAR(1500)    ENCODE ZSTD NOT NULL,
          |  "ref_parent"     VARCHAR(255)     ENCODE ZSTD NOT NULL,
          |  "foo1"           DOUBLE PRECISION ENCODE RAW,
          |  FOREIGN KEY (root_id) REFERENCES s.events(event_id)
          |)
          |DISTSTYLE KEY
          |DISTKEY (root_id)
          |SORTKEY (root_tstamp);
          |
          |COMMENT ON TABLE s.com_acme_example_1_0_1_recovered_1202144068 IS 'iglu:com.acme/example/jsonschema/1-0-1';
          |
          |Making required column nullable foo""".stripMargin
      )
    }

    
    "should make a ignore varchar narrowing" in {
      val s1 = ShredModel.good(dummyKey,
        json"""{
               "type": "object",
               "properties": {
                 "foo": {
                   "type": "string",
                   "maxLength": 20
                 }}
              }""".schema)
      val s2 = ShredModel.good(dummyKey1,
        json"""{
               "type": "object",
               "properties": {
                 "foo": {
                   "type": "string",
                   "maxLength": 10
                 }}
              }""".schema)

      s1.merge(s2).toTestString must beRight(
        """CREATE TABLE IF NOT EXISTS s.com_acme_example_1 (
          |  "schema_vendor"  VARCHAR(128)  ENCODE ZSTD NOT NULL,
          |  "schema_name"    VARCHAR(128)  ENCODE ZSTD NOT NULL,
          |  "schema_format"  VARCHAR(128)  ENCODE ZSTD NOT NULL,
          |  "schema_version" VARCHAR(128)  ENCODE ZSTD NOT NULL,
          |  "root_id"        CHAR(36)      ENCODE RAW  NOT NULL,
          |  "root_tstamp"    TIMESTAMP     ENCODE ZSTD NOT NULL,
          |  "ref_root"       VARCHAR(255)  ENCODE ZSTD NOT NULL,
          |  "ref_tree"       VARCHAR(1500) ENCODE ZSTD NOT NULL,
          |  "ref_parent"     VARCHAR(255)  ENCODE ZSTD NOT NULL,
          |  "foo"            VARCHAR(20)   ENCODE ZSTD,
          |  FOREIGN KEY (root_id) REFERENCES s.events(event_id)
          |)
          |DISTSTYLE KEY
          |DISTKEY (root_id)
          |SORTKEY (root_tstamp);
          |
          |COMMENT ON TABLE s.com_acme_example_1 IS 'iglu:com.acme/example/jsonschema/1-0-1';
          |
          |-- WARNING: only apply this file to your database if the following SQL returns the expected:
          |--
          |-- SELECT pg_catalog.obj_description(c.oid) FROM pg_catalog.pg_class c WHERE c.relname = 'com_acme_example_1';
          |--  obj_description
          |-- -----------------
          |--  iglu:com.acme/example/jsonschema/1-0-0
          |--  (1 row)
          |
          |
          |-- NO ADDED COLUMNS CAN BE EXPRESSED IN SQL MIGRATION
          |
          |COMMENT ON TABLE s.com_acme_example_1 IS 'iglu:com.acme/example/jsonschema/1-0-1';
          |""".stripMargin
      )
    }

    "should merge multiple schemas" in {
      val s1 = ShredModel.good(dummyKey,
        json"""{
               "type": "object",
               "properties": {
                 "foo": {
                   "type": "string",
                   "maxLength": 20
                 }}
              }""".schema)
      val s2 = ShredModel.good(dummyKey1,
        json"""{
               "type": "object",
               "properties": {
                 "bar": {
                   "type": "string",
                   "maxLength": 10
                 }}
              }""".schema)
      val s3 = ShredModel.good(dummyKey2,
        json"""{
         "type": "object",
         "properties": {
           "foo": {
             "type": "string",
             "maxLength": 30
           }}
        }""".schema)
      s1.merge(s2).flatMap(_.merge(s3)).toTestString must beRight(
        """CREATE TABLE IF NOT EXISTS s.com_acme_example_1 (
          |  "schema_vendor"  VARCHAR(128)  ENCODE ZSTD NOT NULL,
          |  "schema_name"    VARCHAR(128)  ENCODE ZSTD NOT NULL,
          |  "schema_format"  VARCHAR(128)  ENCODE ZSTD NOT NULL,
          |  "schema_version" VARCHAR(128)  ENCODE ZSTD NOT NULL,
          |  "root_id"        CHAR(36)      ENCODE RAW  NOT NULL,
          |  "root_tstamp"    TIMESTAMP     ENCODE ZSTD NOT NULL,
          |  "ref_root"       VARCHAR(255)  ENCODE ZSTD NOT NULL,
          |  "ref_tree"       VARCHAR(1500) ENCODE ZSTD NOT NULL,
          |  "ref_parent"     VARCHAR(255)  ENCODE ZSTD NOT NULL,
          |  "foo"            VARCHAR(30)   ENCODE ZSTD,
          |  "bar"            VARCHAR(10)   ENCODE ZSTD,
          |  FOREIGN KEY (root_id) REFERENCES s.events(event_id)
          |)
          |DISTSTYLE KEY
          |DISTKEY (root_id)
          |SORTKEY (root_tstamp);
          |
          |COMMENT ON TABLE s.com_acme_example_1 IS 'iglu:com.acme/example/jsonschema/1-0-2';
          |
          |-- WARNING: only apply this file to your database if the following SQL returns the expected:
          |--
          |-- SELECT pg_catalog.obj_description(c.oid) FROM pg_catalog.pg_class c WHERE c.relname = 'com_acme_example_1';
          |--  obj_description
          |-- -----------------
          |--  iglu:com.acme/example/jsonschema/1-0-0
          |--  (1 row)
          |
          |  ALTER TABLE s.com_acme_example_1
          |    ALTER COLUMN "foo" TYPE VARCHAR(30);
          |
          |BEGIN TRANSACTION;
          |
          |  ALTER TABLE s.com_acme_example_1
          |    ADD COLUMN "bar" VARCHAR(10) ENCODE ZSTD;
          |
          |  COMMENT ON TABLE s.com_acme_example_1 IS 'iglu:com.acme/example/jsonschema/1-0-2';
          |
          |END TRANSACTION;""".stripMargin
      )
    }
    
    "should merge multiple schemas when only adding columns" in {
      val s1 = SelfDescribingSchema(SchemaMap(dummyKey),
        json"""{
               "type": "object",
               "properties": {
                 "foo": {
                   "type": "string",
                   "maxLength": 20
                 }}
              }""".schema)
      val s2 = SelfDescribingSchema(SchemaMap(dummyKey1),
        json"""{
               "type": "object",
               "properties": {
                 "zoo": {
                   "type": "number"
                 }}
              }""".schema)
      val s3 = SelfDescribingSchema(SchemaMap(dummyKey2),
        json"""{
         "type": "object",
         "properties": {
           "foo1": {
             "type": "string",
             "maxLength": 30
           }}
        }""".schema)

      foldMapMergeRedshiftSchemas(NonEmptyList.of(s1, s2, s3)).goodModel
        .asRight[RecoveryModel].toTestString must beRight(
        """CREATE TABLE IF NOT EXISTS s.com_acme_example_1 (
          |  "schema_vendor"  VARCHAR(128)     ENCODE ZSTD NOT NULL,
          |  "schema_name"    VARCHAR(128)     ENCODE ZSTD NOT NULL,
          |  "schema_format"  VARCHAR(128)     ENCODE ZSTD NOT NULL,
          |  "schema_version" VARCHAR(128)     ENCODE ZSTD NOT NULL,
          |  "root_id"        CHAR(36)         ENCODE RAW  NOT NULL,
          |  "root_tstamp"    TIMESTAMP        ENCODE ZSTD NOT NULL,
          |  "ref_root"       VARCHAR(255)     ENCODE ZSTD NOT NULL,
          |  "ref_tree"       VARCHAR(1500)    ENCODE ZSTD NOT NULL,
          |  "ref_parent"     VARCHAR(255)     ENCODE ZSTD NOT NULL,
          |  "foo"            VARCHAR(20)      ENCODE ZSTD,
          |  "zoo"            DOUBLE PRECISION ENCODE RAW,
          |  "foo1"           VARCHAR(30)      ENCODE ZSTD,
          |  FOREIGN KEY (root_id) REFERENCES s.events(event_id)
          |)
          |DISTSTYLE KEY
          |DISTKEY (root_id)
          |SORTKEY (root_tstamp);
          |
          |COMMENT ON TABLE s.com_acme_example_1 IS 'iglu:com.acme/example/jsonschema/1-0-2';
          |
          |-- WARNING: only apply this file to your database if the following SQL returns the expected:
          |--
          |-- SELECT pg_catalog.obj_description(c.oid) FROM pg_catalog.pg_class c WHERE c.relname = 'com_acme_example_1';
          |--  obj_description
          |-- -----------------
          |--  iglu:com.acme/example/jsonschema/1-0-0
          |--  (1 row)
          |
          |
          |BEGIN TRANSACTION;
          |
          |  ALTER TABLE s.com_acme_example_1
          |    ADD COLUMN "zoo" DOUBLE PRECISION ENCODE RAW;
          |  ALTER TABLE s.com_acme_example_1
          |    ADD COLUMN "foo1" VARCHAR(30) ENCODE ZSTD;
          |
          |  COMMENT ON TABLE s.com_acme_example_1 IS 'iglu:com.acme/example/jsonschema/1-0-2';
          |
          |END TRANSACTION;""".stripMargin
      )
    }

    "should merge multiple schemas skipping broken one in the middle" in {
      val s1 = SelfDescribingSchema(SchemaMap(dummyKey),
        json"""{
               "type": "object",
               "properties": {
                 "foo": {
                   "type": "string",
                   "maxLength": 20
                 }}
              }""".schema)
      val s2 = SelfDescribingSchema(SchemaMap(dummyKey1),
        json"""{
               "type": "object",
               "properties": {
                 "foo": {
                   "type": "number"
                 }}
              }""".schema)
      val s3 = SelfDescribingSchema(SchemaMap(dummyKey2),
        json"""{
         "type": "object",
         "properties": {
           "foo": {
             "type": "string",
             "maxLength": 30
           }}
        }""".schema)

      foldMapMergeRedshiftSchemas(NonEmptyList.of(s1, s2, s3)).goodModel
        .asRight[RecoveryModel].toTestString must beRight(
        """CREATE TABLE IF NOT EXISTS s.com_acme_example_1 (
          |  "schema_vendor"  VARCHAR(128)  ENCODE ZSTD NOT NULL,
          |  "schema_name"    VARCHAR(128)  ENCODE ZSTD NOT NULL,
          |  "schema_format"  VARCHAR(128)  ENCODE ZSTD NOT NULL,
          |  "schema_version" VARCHAR(128)  ENCODE ZSTD NOT NULL,
          |  "root_id"        CHAR(36)      ENCODE RAW  NOT NULL,
          |  "root_tstamp"    TIMESTAMP     ENCODE ZSTD NOT NULL,
          |  "ref_root"       VARCHAR(255)  ENCODE ZSTD NOT NULL,
          |  "ref_tree"       VARCHAR(1500) ENCODE ZSTD NOT NULL,
          |  "ref_parent"     VARCHAR(255)  ENCODE ZSTD NOT NULL,
          |  "foo"            VARCHAR(30)   ENCODE ZSTD,
          |  FOREIGN KEY (root_id) REFERENCES s.events(event_id)
          |)
          |DISTSTYLE KEY
          |DISTKEY (root_id)
          |SORTKEY (root_tstamp);
          |
          |COMMENT ON TABLE s.com_acme_example_1 IS 'iglu:com.acme/example/jsonschema/1-0-2';
          |
          |-- WARNING: only apply this file to your database if the following SQL returns the expected:
          |--
          |-- SELECT pg_catalog.obj_description(c.oid) FROM pg_catalog.pg_class c WHERE c.relname = 'com_acme_example_1';
          |--  obj_description
          |-- -----------------
          |--  iglu:com.acme/example/jsonschema/1-0-0
          |--  (1 row)
          |
          |  ALTER TABLE s.com_acme_example_1
          |    ALTER COLUMN "foo" TYPE VARCHAR(30);
          |
          |-- NO ADDED COLUMNS CAN BE EXPRESSED IN SQL MIGRATION
          |
          |COMMENT ON TABLE s.com_acme_example_1 IS 'iglu:com.acme/example/jsonschema/1-0-2';
          |""".stripMargin
      )
    }

    "should merge multiple schemas merged with broken one in the middle and it should get a recovery model" in {
      val s1 = SelfDescribingSchema(SchemaMap(dummyKey),
        json"""{
               "type": "object",
               "properties": {
                 "foo": {
                   "type": "string",
                   "maxLength": 20
                 }}
              }""".schema)
      val s2 = SelfDescribingSchema(SchemaMap(dummyKey1),
        json"""{
               "type": "object",
               "properties": {
                 "foo": {
                   "type": "number"
                 }}
              }""".schema)
      val s3 = SelfDescribingSchema(SchemaMap(dummyKey2),
        json"""{
         "type": "object",
         "properties": {
           "foo": {
             "type": "string",
             "maxLength": 30
           }}
        }""".schema)

      foldMapRedshiftSchemas(NonEmptyList.of(s1, s2, s3))(dummyKey1).asInstanceOf[RecoveryModel]
        .asLeft[GoodModel].toTestString must beLeft(
        """CREATE TABLE IF NOT EXISTS s.com_acme_example_1_0_1_recovered_194359593 (
          |  "schema_vendor"  VARCHAR(128)     ENCODE ZSTD NOT NULL,
          |  "schema_name"    VARCHAR(128)     ENCODE ZSTD NOT NULL,
          |  "schema_format"  VARCHAR(128)     ENCODE ZSTD NOT NULL,
          |  "schema_version" VARCHAR(128)     ENCODE ZSTD NOT NULL,
          |  "root_id"        CHAR(36)         ENCODE RAW  NOT NULL,
          |  "root_tstamp"    TIMESTAMP        ENCODE ZSTD NOT NULL,
          |  "ref_root"       VARCHAR(255)     ENCODE ZSTD NOT NULL,
          |  "ref_tree"       VARCHAR(1500)    ENCODE ZSTD NOT NULL,
          |  "ref_parent"     VARCHAR(255)     ENCODE ZSTD NOT NULL,
          |  "foo"            DOUBLE PRECISION ENCODE RAW,
          |  FOREIGN KEY (root_id) REFERENCES s.events(event_id)
          |)
          |DISTSTYLE KEY
          |DISTKEY (root_id)
          |SORTKEY (root_tstamp);
          |
          |COMMENT ON TABLE s.com_acme_example_1_0_1_recovered_194359593 IS 'iglu:com.acme/example/jsonschema/1-0-1';
          |
          |Incompatible types in column foo old RedshiftVarchar(20) new RedshiftDouble""".stripMargin
      )
    }

  }
}

object ShredModelSpec {
  val dummyKey = SchemaKey("com.acme", "example", "jsonschema", SchemaVer.Full(1, 0, 0))
  val dummyKey1 = SchemaKey("com.acme", "example", "jsonschema", SchemaVer.Full(1, 0, 1))
  val dummyKey2 = SchemaKey("com.acme", "example", "jsonschema", SchemaVer.Full(1, 0, 2))
  val dummyModel = ShredModel.good(dummyKey,
    json"""{
           "type": "object",
           "properties": {
             "foo": {
               "type": "string",
               "maxLength": 20
             },
             "bar": {
               "type": "integer",
               "maximum": 4000
             },
             "a_field": {
               "type": "object",
               "properties": {
                 "b_field": {
                   "type": "string"
                 },
                 "c_field": {
                   "type": "object",
                   "properties": {
                     "d_field": {
                       "type": "string"
                     },
                     "e_field": {
                       "type": "string"
                     }
                   }
                 },
                 "d_field": {
                   "type": "string"
                 }
               },
               "required": ["d_field"]
             },
             "b_field": {
               "type": "integer"
             },
             "c_field": {
               "type": "integer"
             },
             "d_field": {
               "type": "object",
               "properties": {
                 "e_field": {
                   "type": "string"
                 },
                 "f_field": {
                   "type": "string"
                 }
               }
             },
             "e_field": {
               "type": "object",
               "properties": {
                 "f_field": {
                   "type": "string"
                 },
                 "g_field": {
                   "type": "string"
                 }
               },
               "required": ["g_field"]
             },
             "f_field": {
               "type": "string"
             },
             "g_field": {
               "type": "string"
             }
           },
           "required": ["a_field", "e_field"] 
}""".schema)

  implicit class ModelMergeOps(result: Either[RecoveryModel, GoodModel]) {
    def toTestString: Either[String, String] = result.leftMap(badModel =>
      (badModel.toTableSql("s") + "\n" + badModel.errorAsStrings.toList.mkString("\n")))
      .map(goodModel => goodModel.toTableSql("s") + "\n" + goodModel.migrationSql("s", None))
  }

}
