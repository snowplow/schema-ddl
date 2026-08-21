addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.11.2")
addSbtPlugin("org.scoverage" % "sbt-coveralls" % "1.3.15")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.4.4")
addSbtPlugin("com.github.sbt" % "sbt-site" % "1.7.0")
addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")
addSbtPlugin("org.typelevel" % "sbt-tpolecat" % "0.5.6")

libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always
