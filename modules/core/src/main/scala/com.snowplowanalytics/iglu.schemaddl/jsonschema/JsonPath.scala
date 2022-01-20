/*
 * Copyright (c) 2016-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.iglu.schemaddl.jsonschema

import cats.data.NonEmptyList
import cats.syntax.either._
import cats.parse.{Numbers, Parser}

/**
 * An extremely small subset of JSONPath spec
 * (@see https://goessner.net/articles/JsonPath/index.html)
 *
 * It meant only to match output of json-schema-validator library
 * and be immediately transformed into [[Pointer]], which is a common
 * format for schema-ddl
 */
object JsonPath {

  sealed trait Cursor
  object Cursor {
    case object Root extends Cursor
    case class Down(key: String) extends Cursor
    case class Index(at: Option[Int]) extends Cursor
    case object Current extends Cursor
  }

  val RootChar = '$'
  val CurrentChar = '.'
  val IndexStart = '['
  val IndexStop = ']'
  val SpecialChars = Set(RootChar, CurrentChar, IndexStart, IndexStop)

  val root: Parser[Cursor] = Parser.char(RootChar).as(Cursor.Root)
  val current: Parser[Cursor] = Parser.char(CurrentChar).as(Cursor.Current)
  val index = Numbers
    .digits0
    .map(s => if (s.isEmpty) None else Some(s.toInt))
    .with1
    .between(Parser.char(IndexStart), Parser.char(IndexStop))
    .map(Cursor.Index.apply)
  val down = Parser.charsWhile(c => !SpecialChars.contains(c)).map(Cursor.Down.apply)

  val operator = Parser.oneOf(current :: index :: down :: Nil).rep

  val jsonPath = for {
    r <- root
    op <- operator
  } yield r :: op

  def parse(str: String): Either[String, NonEmptyList[Cursor]] =
    jsonPath.parseAll(str).leftMap {
      case Parser.Error(failedAtOffset, expected) =>
        s"Failed to parse $str as JSONPath at $failedAtOffset. Expected: ${expected.toList.mkString(", ")}"
    }

  /**
   * Try to convert to [[Pointer.SchemaPointer]]. We're interested only in `SchemaPointer` because
   * [[JsonPath]] is used only within [[SelfSyntaxChecker]], which works only with schemas
   *
   * @param jsonPath parsed minimal JSONPath string
   * @return either object with same semantics as [[Pointer.parseSchemaPointer]] - in case of unexpected
   *         schema property it fallsback to `DownField`, which will work out as a pointer, but can be
   *         invalid semantically
   */
  def toPointer(jsonPath: NonEmptyList[Cursor]): Either[Pointer.SchemaPointer, Pointer.SchemaPointer] = {
    val (result, isFull) = jsonPath.foldLeft((List.empty[Pointer.Cursor], true)) { case (acc @ (pointer, isSchema), cur) =>
      cur match {
        case Cursor.Root => acc
        case Cursor.Down(key) => Pointer.SchemaProperty.fromString(key) match {
          case Right(value) if isSchema => (Pointer.Cursor.DownProperty(value) :: pointer, true)
          case Right(_) => (Pointer.Cursor.DownField(key) :: pointer, false)
          case Left(_) =>  (Pointer.Cursor.DownField(key) :: pointer, false)
        }
        case Cursor.Index(at) => (Pointer.Cursor.At(at.getOrElse(0)) :: pointer, isSchema)
        case Cursor.Current => acc
      }
    }

    // If at some point it had unexpected non-schema field - turn downstream into data-pointers
    val output = Pointer.SchemaPointer(result)
    Either.cond(isFull, output, output)
  }

}

