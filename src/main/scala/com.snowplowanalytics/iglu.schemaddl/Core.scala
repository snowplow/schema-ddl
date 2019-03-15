package com.snowplowanalytics.iglu.schemaddl

// All entities should be migrated to core
object Core {

  sealed trait VersionPoint
  object VersionPoint {
    case object Model extends VersionPoint
    case object Revision extends VersionPoint
    case object Addition extends VersionPoint
  }
}
