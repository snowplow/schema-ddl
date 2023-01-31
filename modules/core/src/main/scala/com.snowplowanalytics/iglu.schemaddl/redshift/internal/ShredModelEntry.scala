package com.snowplowanalytics.iglu.schemaddl.redshift.internal

import cats.Show
import cats.syntax.either._
import cats.syntax.show._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Pointer.SchemaPointer
import com.snowplowanalytics.iglu.schemaddl.jsonschema.{Pointer, Schema}
import ColumnTypeSuggestions.columnTypeSuggestions
import com.snowplowanalytics.iglu.schemaddl.redshift.FactoryError
import io.circe.{ACursor, Json}

import scala.annotation.tailrec

final case class ShredModelEntry(ptr: SchemaPointer, subSchema: Schema) {

  lazy val columnName: String = ptr.getName

  lazy val nullability: ShredModelEntry.Nullability =
    if (!subSchema.canBeNull) ShredModelEntry.Nullability.Null else ShredModelEntry.Nullability.NotNull

  lazy val columnType: ShredModelEntry.ColumnType = columnTypeSuggestions
    .find(_.apply(subSchema).isDefined)
    .flatMap(_.apply(subSchema))
    .getOrElse(ShredModelEntry.ColumnType.RedshiftVarchar(ShredModelEntry.VARCHAR_SIZE))

  lazy val compressionEncoding: ShredModelEntry.CompressionEncoding = (subSchema.`enum`, columnType) match {
    case (Some(_), ShredModelEntry.ColumnType.RedshiftVarchar(size)) if size <= 255 =>
      ShredModelEntry.CompressionEncoding.Text255Encoding
    case (_, ShredModelEntry.ColumnType.RedshiftBoolean) => ShredModelEntry.CompressionEncoding.RunLengthEncoding
    case (_, ShredModelEntry.ColumnType.RedshiftDouble) => ShredModelEntry.CompressionEncoding.RawEncoding
    case (_, ShredModelEntry.ColumnType.RedshiftReal) => ShredModelEntry.CompressionEncoding.RawEncoding
    case _ => ShredModelEntry.CompressionEncoding.ZstdEncoding
  }

  def stringFactory(json: Json): Either[FactoryError, String] = {
    @tailrec
    def go(cursor: List[Pointer.Cursor], data: ACursor): String =
      cursor match {
        case Nil => data.focus.map(
          _.fold(ShredModelEntry.NullCharacter,
            if (_) "1" else "0",
            _ => json.show,
            ShredModelEntry.escapeTsv,
            _ => ShredModelEntry.escapeTsv(json.noSpaces),
            _ => ShredModelEntry.escapeTsv(json.noSpaces)
          )
        ).getOrElse(ShredModelEntry.NullCharacter)
        case Pointer.Cursor.DownField(field) :: t =>
          go(t, data.downField(field))
        case Pointer.Cursor.At(i) :: t =>
          go(t, data.downN(i))
        case Pointer.Cursor.DownProperty(_) :: _ =>
          throw new IllegalStateException(s"Iglu Schema DDL tried to use invalid pointer ${ptr.show} for payload ${json.noSpaces}")
      }

    Either.catchOnly[IllegalStateException](go(ptr.get, json.hcursor)).leftMap(ex => FactoryError(ex.getMessage)
    )
  }


}

object ShredModelEntry {

  private val VARCHAR_SIZE = 65535

  private val NullCharacter: String = "\\N"

  private def escapeTsv(s: String): String =
    if (s == NullCharacter) "\\\\N"
    else s.replace('\t', ' ').replace('\n', ' ')


  sealed trait ColumnType

  implicit val showProps: Show[List[ShredModelEntry]] = Show.show(props => {
    val colsAsString = props.map(prop =>
      (s"\"${prop.columnName}\"", prop.columnType.show, prop.compressionEncoding.show, prop.nullability.show)
    )
    val extraCols = List(
      ("schema_vendor", "VARCHAR (128)", "ENCODE ZSTD", "NOT NULL"),
      ("schema_name", "VARCHAR (128)", "ENCODE ZSTD", "NOT NULL"),
      ("schema_format", "VARCHAR (128)", "ENCODE ZSTD", "NOT NULL"),
      ("schema_version", "VARCHAR (128)", "ENCODE ZSTD", "NOT NULL"),
      ("root_id", "CHAR (36)", "ENCODE RAW", "NOT NULL"),
      ("root_tstamp", "TIMESTAMP", "ENCODE ZSTD", "NOT NULL"),
      ("ref_root", "VARCHAR (255)", "ENCODE ZSTD", "NOT NULL"),
      ("ref_tree", "VARCHAR (1500)", "ENCODE ZSTD", "NOT NULL"),
      ("ref_parent", "VARCHAR (255)", "ENCODE ZSTD", "NOT NULL")
    )
    val allCols = extraCols ++ colsAsString
    val (mName, mType, mComp, mNull) = allCols.foldLeft((0, 0, 0, 0))(
      (acc, col) => (
        math.max(col._1.length, acc._1),
        math.max(col._2.length, acc._2),
        math.max(col._3.length, acc._3),
        math.max(col._4.length, acc._4)
      ))
    val fmtStr = s"  %-${mName}s %${mType}s %${mComp}s %${mNull}s"

    allCols
      .map(cols => fmtStr.format(cols._1, cols._2, cols._3, cols._4).stripTrailing)
      .mkString(",\n")
  })

  object ColumnType {

    implicit val typeShow: Show[ColumnType] = Show.show {
      case RedshiftTimestamp => "TIMESTAMP"
      case RedshiftDate => "DATE"
      case RedshiftSmallInt => "SMALLINT"
      case RedshiftInteger => "INT"
      case RedshiftBigInt => "BIGINT"
      case RedshiftDouble => "DOUBLE PRECISION"
      case RedshiftDecimal(precision, scale) => (precision, scale) match {
        case (Some(p), Some(s)) => s"DECIMAL ($p, $s)"
        case _ => "DECIMAL"
      }
      case RedshiftBoolean => "BOOLEAN"
      case RedshiftVarchar(size) => s"VARCHAR($size)"
      case RedshiftChar(size) => s"CHAR($size)"
      case ProductType(size) => s"VARCHAR(${size.getOrElse(4096)})"
    }

    case object RedshiftTimestamp extends ColumnType

    case object RedshiftDate extends ColumnType

    case object RedshiftSmallInt extends ColumnType

    case object RedshiftInteger extends ColumnType

    case object RedshiftBigInt extends ColumnType

    case object RedshiftReal extends ColumnType

    case object RedshiftDouble extends ColumnType

    case class RedshiftDecimal(precision: Option[Int], scale: Option[Int]) extends ColumnType

    case object RedshiftBoolean extends ColumnType

    case class RedshiftVarchar(size: Int) extends ColumnType

    case class RedshiftChar(size: Int) extends ColumnType

    /**
     * These predefined data types assembles into usual Redshift data types, but
     * can store additional information such as warnings.
     * Using to prevent output on DDL-generation step.
     */
    case class ProductType(size: Option[Int]) extends ColumnType
  }


  sealed trait CompressionEncoding

  object CompressionEncoding {

    implicit val compressionEncodingShow: Show[CompressionEncoding] = Show.show {
      case RawEncoding => s"ENCODE RAW"
      case Text255Encoding => s"ENCODE TEXT255"
      case ZstdEncoding => s"ENCODE ZSTD"
      case RunLengthEncoding => "ENCODE RUNLENGTH"
    }

    case object RawEncoding extends CompressionEncoding

    case object RunLengthEncoding extends CompressionEncoding

    case object Text255Encoding extends CompressionEncoding

    case object ZstdEncoding extends CompressionEncoding
  }

  sealed trait Nullability {
    def isNarrowing(other: Nullability): Boolean = other match {
      case Nullability.Null => false
      case Nullability.NotNull => this match {
        case Nullability.Null => true
        case Nullability.NotNull => false
      }
    }
  }

  object Nullability {

    implicit val nullabilityShow: Show[Nullability] = Show.show {
      case Null => ""
      case NotNull => "NOT NULL"
    }

    case object Null extends Nullability

    case object NotNull extends Nullability
  }
}