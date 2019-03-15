/*
 * Copyright (c) 2014-2019 Snowplow Analytics Ltd. All rights reserved.
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

package com.snowplowanalytics.iglu.schemaddl.migrations

import scala.util.Random
import io.circe.literal._
import cats.data._
import com.snowplowanalytics.iglu.core.{SchemaMap, SchemaVer, SelfDescribingSchema}
import com.snowplowanalytics.iglu.schemaddl.SpecHelpers._
import com.snowplowanalytics.iglu.schemaddl.migrations.SchemaList.BuildError._
import SchemaList._
import org.specs2.Specification

class SchemaListSpec extends Specification { def is = s2"""
  Check SchemaList
    extract correct segments $e1
    afterIndex returns correct segment when given index in the range $e2
    create correct groups $e3
    safe build from model group function return error when given list contains ambiguous schema list $e4
    safe build from model group function return error when given list contains gaps $e5
    safe build from model group function creates SchemaList correctly when everything is okay $e6
    unsafe build from model group with reordering function return error when given list contains gaps $e7
    unsafe build from model group with reordering function creates SchemaList correctly when everything is okay $e8
    multiple build function return as expected $e9
    single schema build function return as expected $e10
  """

  def e1 = {
    val schemaMap = SchemaMap("com.acme", "example", "jsonschema", SchemaVer.Full(1,0,0))

    val schemaListFull1 = Full(createSchemas(schemaMap, 2))

    val expected1 = NonEmptyList.of(
      NonEmptyList.of(
        schemaMap.version(SchemaVer.Full(1,0,0)),
        schemaMap.version(SchemaVer.Full(1,0,1))
      )
    )

    val comp1 = schemaListFull1.extractSegments.extractSchemaMaps must beEqualTo(expected1)

    val schemaListFull2 = Full(createSchemas(schemaMap, 4))

    val expected2 = NonEmptyList.of(
      NonEmptyList.of(
        schemaMap.version(SchemaVer.Full(1,0,0)),
        schemaMap.version(SchemaVer.Full(1,0,1))
      ),
      NonEmptyList.of(
        schemaMap.version(SchemaVer.Full(1,0,0)),
        schemaMap.version(SchemaVer.Full(1,0,1)),
        schemaMap.version(SchemaVer.Full(1,0,2))
      ),
      NonEmptyList.of(
        schemaMap.version(SchemaVer.Full(1,0,0)),
        schemaMap.version(SchemaVer.Full(1,0,1)),
        schemaMap.version(SchemaVer.Full(1,0,2)),
        schemaMap.version(SchemaVer.Full(1,0,3))
      ),
      NonEmptyList.of(
        schemaMap.version(SchemaVer.Full(1,0,1)),
        schemaMap.version(SchemaVer.Full(1,0,2))
      ),
      NonEmptyList.of(
        schemaMap.version(SchemaVer.Full(1,0,1)),
        schemaMap.version(SchemaVer.Full(1,0,2)),
        schemaMap.version(SchemaVer.Full(1,0,3))
      ),
      NonEmptyList.of(
        schemaMap.version(SchemaVer.Full(1,0,2)),
        schemaMap.version(SchemaVer.Full(1,0,3))
      )
    )

    val comp2 = schemaListFull2.extractSegments.extractSchemaMaps must beEqualTo(expected2)

    comp1 and comp2
  }

  def e2 = {
    val schemaMap = SchemaMap("com.acme", "example", "jsonschema", SchemaVer.Full(1,0,0))
    val schemas = createSchemas(schemaMap, 4)

    val schemaListFull = Full(schemas)

    val res1 = schemaListFull.afterIndex(1).extractSchemaMaps must beSome(
      NonEmptyList.of(
        schemaMap.version(SchemaVer.Full(1,0,1)),
        schemaMap.version(SchemaVer.Full(1,0,2)),
        schemaMap.version(SchemaVer.Full(1,0,3))
      )
    )

    val res2 = schemaListFull.afterIndex(3).extractSchemaMaps must beSome(
      NonEmptyList.of(
        schemaMap.version(SchemaVer.Full(1,0,3))
      )
    )

    val res3 = schemaListFull.afterIndex(5).extractSchemaMaps must beNone

    res1 and res2 and res3
  }

  def e3 = {
    val schemaMap1 = SchemaMap("com.acme", "example1", "jsonschema", SchemaVer.Full(1,0,0))
    val schemaMap2 = SchemaMap("com.acme", "example2", "jsonschema", SchemaVer.Full(1,0,0))
    val schemaMap3 = SchemaMap("com.acme", "example3", "jsonschema", SchemaVer.Full(1,0,0))
    val schemaMap4 = SchemaMap("com.acme", "example4", "jsonschema", SchemaVer.Full(1,0,0))

    val group1 = createSchemas(schemaMap1, 2)
    val group2 = createSchemas(schemaMap2, 4)
    val group3 = createSchemas(schemaMap3, 6)
    val group4 = createSchemas(schemaMap4, 1)

    val output = ModelGroupSet.groupSchemas(
        group1.concatNel(group2).concatNel(group3).concatNel(group4)
      ).map(_.schemas)

    output.toList.toSet must beEqualTo(Set(group1, group2, group3, group4))
  }

  def e4 = {
    val schemaMap = SchemaMap("com.acme", "example1", "jsonschema", SchemaVer.Full(1,0,0))

    val tempGroup = createSchemas(schemaMap, 2)
    val group = tempGroup.append(tempGroup.last.copy(self = schemaMap.version(SchemaVer.Full(1,1,0))))

    val modelGroup = ModelGroupSet.groupSchemas(group).head

    val output = SchemaList.fromUnambiguous(modelGroup)

    output must beLeft(AmbiguousOrder(modelGroup))
  }

  def e5 = {
    val schemaMap = SchemaMap("com.acme", "example1", "jsonschema", SchemaVer.Full(1,0,0))

    val tempGroup1 = createSchemas(schemaMap, 2)
    val modelGroupWithGapInAddition = ModelGroupSet.groupSchemas(
      tempGroup1
        .append(tempGroup1.last.copy(self = schemaMap.version(SchemaVer.Full(1,0,2))))
        .append(tempGroup1.last.copy(self = schemaMap.version(SchemaVer.Full(1,0,4))))
    ).head

    val comp1 = SchemaList.fromUnambiguous(modelGroupWithGapInAddition) must beLeft(GapInModelGroup(modelGroupWithGapInAddition))

    val tempGroup2 = createSchemas(schemaMap, 2, addition = false)
    val modelGroupWithGapInRevision = ModelGroupSet.groupSchemas(
      tempGroup2
        .append(tempGroup2.last.copy(self = schemaMap.version(SchemaVer.Full(1,1,0))))
        .append(tempGroup2.last.copy(self = schemaMap.version(SchemaVer.Full(1,3,0))))
    ).head

    val comp2 = SchemaList.fromUnambiguous(modelGroupWithGapInRevision) must beLeft(GapInModelGroup(modelGroupWithGapInRevision))

    comp1 and comp2
  }

  def e6 = {
    val schemaMap = SchemaMap("com.acme", "example1", "jsonschema", SchemaVer.Full(1,0,0))

    val modelGroup1 = ModelGroupSet.groupSchemas(createSchemas(schemaMap, 4)).head
    val schemaListFullComp = SchemaList.fromUnambiguous(modelGroup1) must beRight(Full(modelGroup1.schemas))

    val modelGroup2 = ModelGroupSet.groupSchemas(createSchemas(schemaMap, 1)).head
    val singleSchemaComp = SchemaList.fromUnambiguous(modelGroup2) must beRight(Single(modelGroup2.schemas.head))

    schemaListFullComp and singleSchemaComp
  }

  def e7 = {
    val schemaMap = SchemaMap("com.acme", "example1", "jsonschema", SchemaVer.Full(1,0,0))

    val tempGroup1 = createSchemas(schemaMap, 2)
    val modelGroupWithGapInAddition = ModelGroupSet.groupSchemas(
      tempGroup1
        .append(tempGroup1.last.copy(self = schemaMap.version(SchemaVer.Full(1,0,2))))
        .append(tempGroup1.last.copy(self = schemaMap.version(SchemaVer.Full(1,0,4))))
    ).head

    val comp1 = SchemaList.unsafeBuildWithReorder(modelGroupWithGapInAddition) must beLeft(GapInModelGroup(modelGroupWithGapInAddition))

    val tempGroup2 = createSchemas(schemaMap, 2, addition = false)
    val modelGroupWithGapInRevision = ModelGroupSet.groupSchemas(
      tempGroup2
        .append(tempGroup2.last.copy(self = schemaMap.version(SchemaVer.Full(1,1,0))))
        .append(tempGroup2.last.copy(self = schemaMap.version(SchemaVer.Full(1,3,0))))
    ).head

    val comp2 = SchemaList.unsafeBuildWithReorder(modelGroupWithGapInRevision) must beLeft(GapInModelGroup(modelGroupWithGapInRevision))

    comp1 and comp2
  }

  def e8 = {
    val schemaMap = SchemaMap("com.acme", "example1", "jsonschema", SchemaVer.Full(1,0,0))

    val tempGroup = createSchemas(schemaMap, 2, true, 2)
    val group = tempGroup
      .append(tempGroup.last.copy(self = schemaMap.version(SchemaVer.Full(2,1,0))))
      .append(tempGroup.last.copy(self = schemaMap.version(SchemaVer.Full(2,1,1))))
    val shuffled = NonEmptyList.fromListUnsafe(Random.shuffle(group.toList))
    val shuffledModelGroup = ModelGroupSet.groupSchemas(shuffled).head
    val notShuffledModelGroup = ModelGroupSet.groupSchemas(group).head
    val comp1 = SchemaList.unsafeBuildWithReorder(shuffledModelGroup) must beRight(Full(notShuffledModelGroup.schemas))

    val modelGroupWithSingleItem = ModelGroupSet.groupSchemas(createSchemas(schemaMap, 1)).head
    val comp2 = SchemaList.unsafeBuildWithReorder(modelGroupWithSingleItem) must beRight(Single(modelGroupWithSingleItem.schemas.head))

    comp1 and comp2
  }

  def e9 = {
    val schemaMap1 = SchemaMap("com.acme", "example1", "jsonschema", SchemaVer.Full(1,0,0))
    val schemaMap2 = SchemaMap("com.acme", "example2", "jsonschema", SchemaVer.Full(1,0,0))
    val schemaMap3 = SchemaMap("com.acme", "example3", "jsonschema", SchemaVer.Full(1,0,0))
    val schemaMap4 = SchemaMap("com.acme", "example4", "jsonschema", SchemaVer.Full(1,0,0))

    val tempGroup1 = createSchemas(schemaMap1, 2)
    val ambiguousGroup = tempGroup1.append(tempGroup1.last.copy(self = schemaMap1.version(SchemaVer.Full(1,1,0))))
    val ambiguousModelGroup = ModelGroupSet.groupSchemas(ambiguousGroup).head

    val tempGroup2 = createSchemas(schemaMap2, 2)
    val groupWithGap = tempGroup2
      .append(tempGroup2.last.copy(self = schemaMap2.version(SchemaVer.Full(1,0,2))))
      .append(tempGroup2.last.copy(self = schemaMap2.version(SchemaVer.Full(1,0,4))))
    val modelGroupWithGap = ModelGroupSet.groupSchemas(groupWithGap).head

    val correctMultiple = createSchemas(schemaMap3, 4)
    val correctSingle = createSchemas(schemaMap4, 1)

    val res = SchemaList.buildMultiple(
      ambiguousGroup.concatNel(groupWithGap).concatNel(correctMultiple).concatNel(correctSingle)
    )

    val expected = Ior.both(
      NonEmptyList.of(
        AmbiguousOrder(ambiguousModelGroup),
        GapInModelGroup(modelGroupWithGap)
      ),
      NonEmptyList.of(
        Full(correctMultiple),
        Single(correctSingle.head)
      )
    )

    res must beEqualTo(expected)
  }

  def e10 = {
    val schemaMap = SchemaMap("com.acme", "example", "jsonschema", SchemaVer.Full(1,0,0))
    val schema = createSchemas(schemaMap, 1).head

    val schemaWithModelZero = schema.copy(self = schemaMap.version(SchemaVer.Full(0,0,0)))
    val schemaWithModelZeroComp = SchemaList.buildSingleSchema(schemaWithModelZero) must beNone

    val schemaWithNonZeroRevision = schema.copy(self = schemaMap.version(SchemaVer.Full(1,1,0)))
    val schemaWithNonZeroRevisionComp = SchemaList.buildSingleSchema(schemaWithNonZeroRevision) must beNone

    val schemaWithNonZeroAddition = schema.copy(self = schemaMap.version(SchemaVer.Full(1,0,1)))
    val schemaWithNonZeroAdditionComp = SchemaList.buildSingleSchema(schemaWithNonZeroAddition) must beNone

    val schemaWithCorrectVersion = schema.copy(self = schemaMap.version(SchemaVer.Full(1,0,0)))
    val schemaWithCorrectVersionComp = SchemaList.buildSingleSchema(schemaWithCorrectVersion) must beSome(Single(schemaWithCorrectVersion))

    schemaWithModelZeroComp
      .and(schemaWithNonZeroRevisionComp)
      .and(schemaWithNonZeroAdditionComp)
      .and(schemaWithCorrectVersionComp)
  }

  private def createSchemas(schemaMap: SchemaMap, count: Int, addition: Boolean = true, model: Int = 1) = {
    val schemaJson = json"""
      {
        "type": "object",
        "properties": {
          "foo": {
            "type": "string"
          }
        },
        "additionalProperties": false
      }
    """.schema
    val res = (0 until count).map { i =>
      val version = if (addition) SchemaVer.Full(model, 0, i) else SchemaVer.Full(model, i, 0)
      SelfDescribingSchema(schemaMap.version(version), schemaJson)
    }
    NonEmptyList.fromListUnsafe(res.toList)
  }

  private implicit class ChangeSchemaMapVersion(val schemaMap: SchemaMap) {
    def version(version: SchemaVer.Full): SchemaMap =
      SchemaMap(schemaMap.schemaKey.copy(version = version))
  }

  private implicit class ExtractSchemaMapsFromSegments(val schemaListSegments: NonEmptyList[Segment]) {
    def extractSchemaMaps: NonEmptyList[NonEmptyList[SchemaMap]] =
      schemaListSegments.map(_.schemas.map(_.self))
  }

  private implicit class ExtractSchemaMapsFromOptionalSegment(val schemaListSegment: Option[Segment]) {
    def extractSchemaMaps: Option[NonEmptyList[SchemaMap]] =
      schemaListSegment.map(_.schemas.map(_.self))
  }
}
