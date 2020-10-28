/**
 * Copyright (c) 2014-2017 Snowplow Analytics Ltd. All rights reserved.
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

import sbt._
import Keys._

import bintray.BintrayPlugin._
import bintray.BintrayKeys._

import scoverage.ScoverageKeys._

import com.typesafe.sbt.site.SitePlugin.autoImport._
import com.typesafe.sbt.site.SiteScaladocPlugin.autoImport._
import com.typesafe.sbt.site.preprocess.PreprocessPlugin.autoImport._

object BuildSettings {

  lazy val basicSettigns = Seq(
    shellPrompt := { _ => "schema-ddl> " },
    addCompilerPlugin("org.typelevel" % "kind-projector" % "0.11.0" cross CrossVersion.full)
  )

  lazy val javaCompilerOptions = Seq(
    "-source", "1.8",
    "-target", "1.8"
  )

  // Publish settings
  lazy val bintrayPublish = bintraySettings ++ Seq[Setting[_]](
    licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0.html")),
    bintrayOrganization := Some("snowplow"),
    bintrayRepository := "snowplow-maven"
  )

  // Maven Central publishing settings
  lazy val mavenCentralExtras = Seq[Setting[_]](
    pomIncludeRepository := { _ => false },
    homepage := Some(url("http://snowplowanalytics.com")),
    scmInfo := Some(ScmInfo(url("https://github.com/snowplow-incubator/schema-ddl"), "scm:git@github.com:snowplow-incubator/schema-ddl.git")),
    pomExtra := (
      <developers>
        <developer>
          <name>Snowplow Analytics Ltd</name>
          <email>support@snowplowanalytics.com</email>
          <organization>Snowplow Analytics Ltd</organization>
          <organizationUrl>http://snowplowanalytics.com</organizationUrl>
        </developer>
      </developers>)
  )

  lazy val buildSettings = basicSettigns ++ bintrayPublish ++ mavenCentralExtras

  val scoverage = Seq(
    coverageMinimum := 50,
    coverageFailOnMinimum := true,
    coverageHighlighting := false,
    (test in Test) := {
      (coverageReport dependsOn (test in Test)).value
    }
  )

  lazy val sbtSiteSettings = Seq(
    siteSubdirName in SiteScaladoc := s"${version.value}"
  )
}
