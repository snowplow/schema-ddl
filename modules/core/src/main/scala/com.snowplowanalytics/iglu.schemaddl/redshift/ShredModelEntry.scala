package com.snowplowanalytics.iglu.schemaddl.redshift

import cats.Show
import cats.syntax.show._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Pointer.SchemaPointer
import com.snowplowanalytics.iglu.schemaddl.jsonschema.{Pointer, Schema}
import com.snowplowanalytics.iglu.schemaddl.redshift.internal.ColumnTypeSuggestions.columnTypeSuggestions
import io.circe.{ACursor, Json}

import scala.annotation.tailrec

/**
 * Single bottom level entry of the schema model. Each entry matches a single column in warehouse.
 *
 * @param ptr            - json pointer. A cursor that could be used to extract the data from json event.
 * @param subSchema      - jsonschema of the element to where pointer is directed.
 * @param isLateAddition - entry added as a result of migration, so it must be NOT NULL.
 */
case class ShredModelEntry(
                            ptr: SchemaPointer,
                            subSchema: Schema,
                            isLateAddition: Boolean
                          ) {

  /**
   * columnName, nullability, columnType and compressionEncoding are used for SQL statement definition of corresponding
   * redshift column.
   */
  lazy val columnName: String = ptr.getName

  lazy val isNullable: Boolean = isLateAddition || subSchema.canBeNull

  lazy val columnType: ShredModelEntry.ColumnType = columnTypeSuggestions
    .find(_.apply(subSchema).isDefined)
    .flatMap(_.apply(subSchema))
    .getOrElse(ShredModelEntry.ColumnType.RedshiftVarchar(ShredModelEntry.VARCHAR_SIZE))

  lazy val compressionEncoding: ShredModelEntry.CompressionEncoding = (subSchema.`enum`, columnType) match {
    case (Some(_), ShredModelEntry.ColumnType.RedshiftVarchar(size)) if size <= 255 =>
      ShredModelEntry.CompressionEncoding.Text255Encoding
    case (_, ShredModelEntry.ColumnType.RedshiftBoolean) => ShredModelEntry.CompressionEncoding.RunLengthEncoding
    case (_, ShredModelEntry.ColumnType.RedshiftDouble) => ShredModelEntry.CompressionEncoding.RawEncoding
    case _ => ShredModelEntry.CompressionEncoding.ZstdEncoding
  }

  /**
   * Extract the string representation of this entry from the event body. Factory relies on the validation done by the 
   * enrich. So column type is not validated against the jsonTypes.
   *
   * @param json body of json event
   * @return Either a casting error (pointer was incompatible with the event) or string serialization of the payload, 
   */
  def stringFactory(json: Json): String = {
    @tailrec
    def go(cursor: List[Pointer.Cursor], data: ACursor): String =
      cursor match {
        case Nil => data.focus.map(
          json => json.fold(
            jsonNull = ShredModelEntry.NullCharacter,
            jsonBoolean = if (_) "1" else "0",
            jsonNumber = _ => json.show,
            jsonString = ShredModelEntry.escapeTsv,
            jsonArray = _ => ShredModelEntry.escapeTsv(json.noSpaces),
            jsonObject = _ => ShredModelEntry.escapeTsv(json.noSpaces)
          )
        ).getOrElse(ShredModelEntry.NullCharacter)
        case Pointer.Cursor.DownField(field) :: t =>
          go(t, data.downField(field))
        case Pointer.Cursor.At(i) :: t =>
          go(t, data.downN(i))
        case Pointer.Cursor.DownProperty(_) :: t => go(t, data)
      }

    go(ptr.get, json.hcursor)
  }
}

object ShredModelEntry {

  def apply(ptr: SchemaPointer, subSchema: Schema): ShredModelEntry =
    ShredModelEntry(ptr, subSchema, isLateAddition = false)

  val VARCHAR_SIZE = 4096

  val NullCharacter: String = "\\N"

  private def escapeTsv(s: String): String =
    if (s == NullCharacter) "\\\\N"
    else s.replace('\t', ' ').replace('\n', ' ')

  private val extraCols = List(
    ("schema_vendor", "VARCHAR(128)", "ENCODE ZSTD", "NOT NULL"),
    ("schema_name", "VARCHAR(128)", "ENCODE ZSTD", "NOT NULL"),
    ("schema_format", "VARCHAR(128)", "ENCODE ZSTD", "NOT NULL"),
    ("schema_version", "VARCHAR(128)", "ENCODE ZSTD", "NOT NULL"),
    ("root_id", "CHAR(36)", "ENCODE RAW", "NOT NULL"),
    ("root_tstamp", "TIMESTAMP", "ENCODE ZSTD", "NOT NULL"),
    ("ref_root", "VARCHAR(255)", "ENCODE ZSTD", "NOT NULL"),
    ("ref_tree", "VARCHAR(1500)", "ENCODE ZSTD", "NOT NULL"),
    ("ref_parent", "VARCHAR(255)", "ENCODE ZSTD", "NOT NULL")
  )

  /** List of column names common across all shredded tables */
  val commonColumnNames: List[String] = extraCols.map(_._1)

  sealed trait ColumnType

  implicit val showProps: Show[List[ShredModelEntry]] = Show.show(props => {
    val colsAsString = props.map(prop =>
      (prop.columnName, prop.columnType.show, prop.compressionEncoding.show, if (prop.isNullable) "" else "NOT NULL")
    )
    val allCols = extraCols ++ colsAsString
    val (mName, mType, mComp) = allCols.foldLeft((2, 0, 0))(
      (acc, col) => (
        math.max(col._1.length + 2, acc._1),
        math.max(col._2.length, acc._2),
        math.max(col._3.length, acc._3),
      ))
    val fmtStr = s"""  %-${mName}s %${-mType}s %-${mComp}s %s"""

    allCols
      .map(cols => fmtStr.format(s""""${cols._1}"""", cols._2, cols._3, cols._4).replaceAll("""\s+$""", ""))
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
    }

    case object RedshiftTimestamp extends ColumnType

    case object RedshiftDate extends ColumnType

    case object RedshiftSmallInt extends ColumnType

    case object RedshiftInteger extends ColumnType

    case object RedshiftBigInt extends ColumnType

    case object RedshiftDouble extends ColumnType

    case class RedshiftDecimal(precision: Option[Int], scale: Option[Int]) extends ColumnType

    case object RedshiftBoolean extends ColumnType

    case class RedshiftVarchar(size: Int) extends ColumnType

    case class RedshiftChar(size: Int) extends ColumnType
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

}
