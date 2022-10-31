package com.snowplowanalytics.iglu.schemaddl.parquet

import com.snowplowanalytics.iglu.schemaddl.parquet.Type.{Array, Struct}

/*
  Parquet schemas are migrated though the `merge.schema = true` loading option in Spark/Databricks.
  Migrations in this package do not define spark 3.0 style `ATLER TABLE` transformations, similar to Redshift. Instead
  they are used to categorize schema changes into 2 categories:
   - Non breaking - Miscellaneous changes that would be handled by the `merge.schema`:
      * `NullableRequired` - nullable to required change
      * `NestedKeyAddition` - addition of the new Struct key anywhere except for top level Schema.
      * `TypeWidening` - Compatible type casting. Such as Int -> Long (cast as BitInt) -> Double.
      * `TopLevelKeyAddition` - key addition in top level schema
   - Breaking - Breaking change that would either lead to oen of these outcomes:
      * `KeyRemoval` historical data loss, such as removing field in Struct. Loading will succeed with data loss.
      * `IncompatibleType` Incompatible type casting, such as String -> Int, etc. Loading will fail.
      * `RequiredNullable` Nullable to required field or array migration. Loading will fail.
 */
object Migrations {

  // Path to field or type within the Parquet schema structure. String `"[arrayDown]"` used to indicate array traversal
  type ParquetSchemaPath = List[String]


  // Base trait for schema differences, which would result in migration.
  sealed trait ParquetMigration {
    // As Migration logic traverses deeper into the structure keys appended to the head of list.
    // So path stored is reverse.
    def path: ParquetSchemaPath

    def reversedPath: ParquetSchemaPath = path.reverse
  }

  type ParquetSchemaMigrations = Set[ParquetMigration]

  sealed trait NonBreaking extends ParquetMigration

  sealed trait Breaking extends ParquetMigration

  case class KeyRemoval(override val path: ParquetSchemaPath, removedKey: Type) extends Breaking {
    override def toString: String = s"Key removal at /${reversedPath.mkString("/")}"
  }

  case class RequiredNullable(override val path: ParquetSchemaPath) extends Breaking {
    override def toString: String = s"Changing nullable property to required at /${reversedPath.mkString("/")}"
  }

  case class NullableRequired(override val path: ParquetSchemaPath) extends NonBreaking {
    override def toString: String = s"Changing required property to nullable at /${reversedPath.mkString("/")}"
  }

  case class TopLevelKeyAddition(override val path: ParquetSchemaPath, key: Type) extends NonBreaking {
    override def toString: String = s"Top-level schema key addition at /${reversedPath.mkString("/")}"
  }

  case class NestedKeyAddition(override val path: ParquetSchemaPath, key: Type) extends NonBreaking {
    override def toString: String = s"Nested object key addition at /${reversedPath.mkString("/")}"
  }

  case class TypeWidening(override val path: ParquetSchemaPath, oldType: Type, newType: Type) extends NonBreaking {
    override def toString: String = s"Type widening from $oldType to $newType at /${reversedPath.mkString("/")}"
  }

  case class IncompatibleType(override val path: ParquetSchemaPath, oldType: Type, newType: Type) extends Breaking {
    override def toString: String = s"Incompatible type change $oldType to $newType at /${reversedPath.mkString("/")}"
  }

  private implicit class FocusStruct(val value: Struct) {
    def focus(subkey: String): Option[Field] = value.fields.collectFirst { case x if x.name == subkey => x }
  }

  private case class MigrationTypePair(path: ParquetSchemaPath, sourceType: Type, targetType: Type) {
    def migrations: ParquetSchemaMigrations = {
      var migrations: Set[ParquetMigration] = Set.empty[ParquetMigration]

      def addIncompatibleType(): Unit =
        migrations += IncompatibleType(path: ParquetSchemaPath, sourceType: Type, targetType: Type)

      def addTypeWidening(): Unit =
        migrations += TypeWidening(path: ParquetSchemaPath, sourceType: Type, targetType: Type)

      sourceType match {
        case Type.String => targetType match {
          case Type.String => ()
          case _ => addIncompatibleType()
        }
        case Type.Boolean => targetType match {
          case Type.Boolean => ()
          case _ => addIncompatibleType()
        }
        case Type.Integer => targetType match {
          case Type.Integer => ()
          case Type.Long => addTypeWidening()
          case Type.Double => addTypeWidening()
          case _ => addIncompatibleType()
        }
        case Type.Long => targetType match {
          case Type.Long => ()
          case Type.Double => addTypeWidening()
          case _ => addIncompatibleType()
        }
        case Type.Double => targetType match {
          case Type.Double => ()
          case _ => addIncompatibleType()
        }
        case Type.Decimal(precision, scale) => targetType match {
          case Type.Double => addTypeWidening()
          case Type.Decimal(targetPrecision, targetScale) =>
            if (targetPrecision == precision & targetScale == scale)
              ()
            else
              addIncompatibleType()
          case _ => addIncompatibleType()
        }
        case Type.Date => targetType match {
          case Type.Date => ()
          case Type.Timestamp => addTypeWidening()
          case _ => addIncompatibleType()
        }
        case Type.Timestamp => targetType match {
          case Type.Timestamp => ()
          case _ => addIncompatibleType()
        }
        case sourceStruct@Struct(sourceFields) => targetType match {
          case targetStruct@Type.Struct(targetFields) =>
            migrations ++= sourceFields.flatMap(f =>
              MigrationFieldPair(f.name::path, f, targetStruct.focus(f.name)).migrations) ++
              // Comparing struct target fields to the source. This will detect additions.
              targetFields
                .flatMap(f => MigrationFieldPair(f.name::path, f, sourceStruct.focus(f.name)).migrations)
                .flatMap {
                  case KeyRemoval(path, value) => if (path.length == 1) {
                    List(TopLevelKeyAddition(path, value))
                  } else {
                    List(NestedKeyAddition(path, value))
                  }
                  case _ => Nil // discard the modification as they will be detected earlier
                }
          case _ => addIncompatibleType()
        }
        case Array(sourceElement, sourceNullability) => targetType match {
          case Type.Array(targetElement, targetNullability) =>
            if (sourceNullability.nullable & targetNullability.required)
              migrations += RequiredNullable("[arrayDown]" :: path)
            else if (sourceNullability.required & targetNullability.nullable) {
              migrations += NullableRequired("[arrayDown]" :: path)
            }
            migrations ++= MigrationTypePair("[arrayDown]" :: path, sourceElement, targetElement).migrations
          case _ => addIncompatibleType()
        }
        case Type.Json => targetType match {
          // Json is cast down to srting by the transformer
          case Type.Json => ()
          case Type.String => addTypeWidening()
          case _ => addIncompatibleType()
        }
      }
      migrations
    }
  }

  private case class MigrationFieldPair(path: ParquetSchemaPath, sourceField: Field, maybeTargetField: Option[Field]) {
    def migrations: ParquetSchemaMigrations = maybeTargetField match {
      case None => Set(KeyRemoval(path, sourceField.fieldType)) // target schema does not have this field
      case Some(targetField) =>
        var migrations: ParquetSchemaMigrations = Set.empty[ParquetMigration]
        if (sourceField == targetField) {
          return  Set.empty[ParquetMigration] // Schemas are equal
        }
        if (sourceField.nullability.nullable & targetField.nullability.required)
          migrations += RequiredNullable(path)
        else if (sourceField.nullability.required & targetField.nullability.nullable) {
          migrations += NullableRequired(path)
        }

        migrations ++= MigrationTypePair(path, sourceField.fieldType, targetField.fieldType).migrations

        migrations
    }
  }

  /*
    Generates list of all migration for the Schema pair. Top level of schema is always Struct.
   */
  def assessSchemaMigration(source: Field, target: Field): ParquetSchemaMigrations =
    MigrationFieldPair(Nil, source, Some(target)).migrations

  /*
    Generates tuple of (Major, Minor, Patch) boolean flags. Indicating which part of schema version should be bumped
    for target migration.
   */

  // [parquet] to access this in tests
  private[parquet] def suggestSchemaVersionMaskFromMigrations(migrations: ParquetSchemaMigrations): (Boolean, Boolean) = {
    val finalFlags = migrations.foldLeft((false, false))((flags, migration) =>
      migration match {
        case _: NonBreaking => (flags._1, true)
        case _: Breaking => (true, flags._2)
      })
    (
      finalFlags._1,
      finalFlags._2 & !finalFlags._1
    )
  }

  def getSchemaMigrationFlags(source: Field, target: Field): (Boolean, Boolean) =
    suggestSchemaVersionMaskFromMigrations(MigrationFieldPair(Nil, source, Some(target)).migrations)
}
