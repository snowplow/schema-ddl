package com.snowplowanalytics.iglu.schemaddl.parquet

import cats.Show
import cats.syntax.all._
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
    def reversedPath: ParquetSchemaPath

    def path: ParquetSchemaPath = reversedPath.reverse
  }

  type ParquetSchemaMigrations = Set[ParquetMigration]

  sealed trait NonBreaking extends ParquetMigration

  sealed trait Breaking extends ParquetMigration

  implicit val showPerson: Show[ParquetMigration] = Show.show(_.toString)

  case class KeyRemoval(override val reversedPath: ParquetSchemaPath, removedKey: Type) extends Breaking {
    override def toString: String = s"Key removal at /${path.mkString("/")}"
  }

  case class RequiredNullable(override val reversedPath: ParquetSchemaPath) extends Breaking {
    override def toString: String = s"Changing nullable property to required at /${path.mkString("/")}"
  }

  case class NullableRequired(override val reversedPath: ParquetSchemaPath) extends NonBreaking {
    override def toString: String = s"Changing required property to nullable at /${path.mkString("/")}"
  }

  case class TopLevelKeyAddition(override val reversedPath: ParquetSchemaPath, key: Type) extends NonBreaking {
    override def toString: String = s"Top-level schema key addition at /${path.mkString("/")}"
  }

  case class NestedKeyAddition(override val reversedPath: ParquetSchemaPath, key: Type) extends NonBreaking {
    override def toString: String = s"Nested object key addition at /${path.mkString("/")}"
  }

  case class TypeWidening(override val reversedPath: ParquetSchemaPath, oldType: Type, newType: Type) extends NonBreaking {
    override def toString: String = s"Type widening from $oldType to $newType at /${path.mkString("/")}"
  }

  case class IncompatibleType(override val reversedPath: ParquetSchemaPath, oldType: Type, newType: Type) extends Breaking {
    override def toString: String = s"Incompatible type change $oldType to $newType at /${path.mkString("/")}"
  }

  private implicit class FocusStruct(val value: Struct) {
    def focus(subkey: String): Option[Field] = value.fields.collectFirst { case x if x.name == subkey => x }
  }

  private case class MergedType(migrations: ParquetSchemaMigrations, result: Option[Type])

  private case class MergedField(migrations: ParquetSchemaMigrations, result: Option[Field])


  private case class MigrationTypePair(path: ParquetSchemaPath, sourceType: Type, targetType: Type) {

    def migrations: MergedType = {
      var migrations: Set[ParquetMigration] = Set.empty[ParquetMigration]

      def addIncompatibleType(): Option[Type] = {
        migrations += IncompatibleType(path: ParquetSchemaPath, sourceType: Type, targetType: Type)
        None
      }

      def addTypeWidening(tgtType: Type): Option[Type] = {
        migrations += TypeWidening(path: ParquetSchemaPath, sourceType: Type, targetType: Type)
        tgtType.some
      }

      val mergedType: Option[Type] = sourceType match {
        case Type.String => targetType match {
          case Type.String => targetType.some
          case Type.Json => addTypeWidening(Type.Json)
          case _ => addIncompatibleType()
        }
        case Type.Boolean => targetType match {
          case Type.Boolean => targetType.some
          case _ => addIncompatibleType()
        }
        case Type.Integer => targetType match {
          case Type.Integer => targetType.some
          case Type.Long => addTypeWidening(Type.Long)
          case Type.Double => addTypeWidening(Type.Double)
          case _ => addIncompatibleType()
        }
        case Type.Long => targetType match {
          case Type.Long => Type.Long.some
          case Type.Integer => Type.Long.some
          case Type.Double => addTypeWidening(Type.Double)
          case _ => addIncompatibleType()
        }
        case Type.Double => targetType match {
          case Type.Long => Type.Double.some
          case Type.Integer => Type.Double.some
          case Type.Double => targetType.some
          case _ => addIncompatibleType()
        }
        case Type.Decimal(precision, scale) => targetType match {
          case Type.Decimal(targetPrecision, targetScale) =>
            if (targetPrecision == precision & targetScale == scale)
              targetType.some
            else
              addIncompatibleType()
          case _ => addIncompatibleType()
        }
        case Type.Date => targetType match {
          case Type.Date => targetType.some
          case _ => addIncompatibleType()
        }
        case Type.Timestamp => targetType match {
          case Type.Timestamp => targetType.some
          case _ => addIncompatibleType()
        }
        case sourceStruct@Struct(sourceFields) => targetType match {
          case targetStruct@Type.Struct(targetFields) =>
            val forwardFields = sourceFields.map(srcField => MigrationFieldPair(srcField.name :: path, srcField, targetStruct.focus(srcField.name)).migrations)

            // Comparing struct target fields to the source. This will detect additions.
            val reverseFields = targetFields.map(tgtField => MigrationFieldPair(tgtField.name :: path, tgtField, sourceStruct.focus(tgtField.name)).migrations)

            migrations ++= forwardFields.flatMap(_.migrations)

            migrations ++= reverseFields.flatMap(f => f.migrations.flatMap {
              case KeyRemoval(path, value) => if (path.length == 1) {
                List(TopLevelKeyAddition(path, value))
              } else {
                List(NestedKeyAddition(path, value))
              }
              case _ => Nil // discard the modification as they will be detected earlier
            })


            val maybeMergedField: Option[Type.Struct] = for {
              srcFields <- forwardFields.traverse(_.result)
              srcFieldNames = srcFields.map(_.name)
              extraTgtFields <- reverseFields.traverseFilter(
                tgtField => tgtField.result.map(tgtField =>
                  // filter out 
                  if (srcFieldNames.contains(tgtField.name)) tgtField.some else None)
              )
            } yield Type.Struct(srcFields ++ extraTgtFields)

            maybeMergedField

          case _ => addIncompatibleType()
        }
        case Array(sourceElement, sourceNullability) => targetType match {
          case Type.Array(targetElement, targetNullability) =>
            val mergedNullable = if (sourceNullability.nullable & targetNullability.required) {
              migrations += RequiredNullable("[arrayDown]" :: path)
              Type.Nullability.Required
            } else if (sourceNullability.required & targetNullability.nullable) {
              migrations += NullableRequired("[arrayDown]" :: path)
              Type.Nullability.Required
            } else if (sourceNullability.required & targetNullability.required) {
              Type.Nullability.Required
            } else {
              Type.Nullability.Nullable
            }
            val mergedType = MigrationTypePair("[arrayDown]" :: path, sourceElement, targetElement).migrations

            migrations ++= mergedType.migrations

            mergedType.result.map(Array(_, mergedNullable))
          case _ => addIncompatibleType()
        }
        case Type.Json => targetType match {
          // Json is cast down to srting by the transformer
          case Type.Json => targetType.some
          case Type.String => addTypeWidening(Type.Json)
          case _ => addIncompatibleType()
        }
      }
      MergedType(migrations, mergedType)
    }
  }

  private case class MigrationFieldPair(path: ParquetSchemaPath, sourceField: Field, maybeTargetField: Option[Field]) {
    def migrations: MergedField = maybeTargetField match {
      case None => MergedField(Set(KeyRemoval(path, sourceField.fieldType)), sourceField.some) // target schema does not have this field
      case Some(targetField) =>
        var migrations: ParquetSchemaMigrations = Set.empty[ParquetMigration]
        if (sourceField == targetField) {
          return MergedField(Set.empty[ParquetMigration], sourceField.some) // Schemas are equal
        }
        val mergedNullability: Type.Nullability = if (sourceField.nullability.nullable & targetField.nullability.required) {
          migrations += RequiredNullable(path)
          Type.Nullability.Required
        } else if (sourceField.nullability.required & targetField.nullability.nullable) {
          migrations += NullableRequired(path)
          Type.Nullability.Required
        } else if (sourceField.nullability.required & targetField.nullability.required) {
          Type.Nullability.Required
        } else {
          Type.Nullability.Nullable
        }

        val mergedType = MigrationTypePair(path, sourceField.fieldType, targetField.fieldType).migrations

        MergedField(mergedType.migrations ++ migrations, mergedType.result.map(Field(sourceField.name, _, mergedNullability)))
    }
  }

  /*
    Generates list of all migration for the Schema pair. Top level of schema is always Struct.
   */
  def assessSchemaMigration(source: Field, target: Field): ParquetSchemaMigrations =
    MigrationFieldPair(Nil, source, Some(target)).migrations.migrations

  /*
    Generates tuple of (Major, Minor, Patch) boolean flags. Indicating which part of schema version should be bumped
    for target migration.
   */

  // [parquet] to access this in tests
  private[parquet] def isSchemaMigrationBreakingFromMigrations(migrations: ParquetSchemaMigrations): Boolean =
    migrations.foldLeft(false)((flag, migration) =>
      migration match {
        case _: NonBreaking => flag
        case _: Breaking => true
      })

  def mergeSchemas(source: Field, target: Field): Either[List[Breaking], Field] = {
    val merged = MigrationFieldPair(Nil, source, Some(target)).migrations
    merged.result match {
      case Some(field) => field.asRight
      case None => merged.migrations.foldLeft(List.empty[Breaking])((accErr, migration) => migration match {
        case _: NonBreaking => accErr
        case breaking: Breaking => breaking :: accErr
      }).asLeft
    }
  }

  def isSchemaMigrationBreaking(source: Field, target: Field): Boolean =
    isSchemaMigrationBreakingFromMigrations(MigrationFieldPair(Nil, source, Some(target)).migrations.migrations)
}
