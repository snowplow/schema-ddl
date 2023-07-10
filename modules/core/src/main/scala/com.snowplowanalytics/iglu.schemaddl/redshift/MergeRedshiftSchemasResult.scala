package com.snowplowanalytics.iglu.schemaddl.redshift

import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.schemaddl.redshift.ShredModel.{GoodModel, RecoveryModel}

final case class MergeRedshiftSchemasResult(goodModel: GoodModel, recoveryModels: Map[SchemaKey, RecoveryModel])