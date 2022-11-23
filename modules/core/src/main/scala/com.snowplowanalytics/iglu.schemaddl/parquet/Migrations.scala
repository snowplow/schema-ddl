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
      * `TopLevelKeyAddition` - key addition in top level schema
      * `KeyRemoval` historical data loss, such as removing field in Struct. Loading will succeed with data loss.
      * `RequiredNullable` Nullable to required field or array migration. Loading will fail.
   - Breaking - Breaking change that would either lead to oen of these outcomes:      
      * `IncompatibleType` Type casting is not allowed with an exception for Struct fields.
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

  case class KeyRemoval(override val reversedPath: ParquetSchemaPath, removedKey: Type) extends NonBreaking {
    override def toString: String = s"Key removal at /${path.mkString("/")}"
  }

  case class RequiredNullable(override val reversedPath: ParquetSchemaPath) extends NonBreaking {
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

      val mergedType: Option[Type] = sourceType match {
        case sourceStruct@Struct(sourceFields) => targetType match {
          case targetStruct@Type.Struct(targetFields) =>
            val forwardMigration = sourceFields.map(srcField => MigrationFieldPair(srcField.name :: path, srcField, targetStruct.focus(srcField.name)).migrations)

            // Comparing struct target fields to the source. This will detect additions.
            val reverseMigration = targetFields.map(tgtField => MigrationFieldPair(tgtField.name :: path, tgtField, sourceStruct.focus(tgtField.name)).migrations)

            migrations ++= forwardMigration.flatMap(_.migrations)

            migrations ++= reverseMigration.flatMap(_.migrations.flatMap {
              case KeyRemoval(path, value) => if (path.length == 1) {
                List(TopLevelKeyAddition(path, value))
              } else {
                List(NestedKeyAddition(path, value))
              }
              case _ => Nil // discard the modifications as they would have been detected in forward migration
            })

            val tgtFields = reverseMigration.traverse(_.result).toList.flatten
            val tgtFieldNames = tgtFields.map(_.name)
            val allSrcFields = forwardMigration.traverse(_.result).toList.flatten            
            val srcFields = allSrcFields.filter(srcField => !tgtFieldNames.contains(srcField.name))

            // failed migration would produce no fields in source
            if (allSrcFields.isEmpty) None else Type.Struct(tgtFields ++ srcFields).some

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
        case _ if targetType === sourceType => targetType.some
        case _ => addIncompatibleType()
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
