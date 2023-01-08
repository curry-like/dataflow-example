import sbt._
import Keys._
// scalariform
import com.typesafe.sbt.SbtScalariform.autoImport._
import scalariform.formatter.preferences._

val scioVersion = "0.5.5"
val beamVersion = "2.4.0"
val circeVersion = "0.9.3"
val scalaMacrosVersion = "2.1.1"


lazy val commonSettings = Defaults.coreDefaultSettings ++ Seq(
  organization := "com.example",
  // Semantic versioning http://semver.org/
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.12.6",
  scalacOptions ++= Seq("-target:jvm-1.8",
    "-deprecation",
    "-feature",
    "-unchecked"),
  javacOptions ++= Seq("-source", "1.8",
    "-target", "1.8"),
  scalariformPreferences := scalariformPreferences.value
    .setPreference(PlaceScaladocAsterisksBeneathSecondAsterisk, true)
    .setPreference(DoubleIndentConstructorArguments, false)
)

lazy val paradiseDependency =
  "org.scalamacros" % "paradise" % scalaMacrosVersion cross CrossVersion.full
lazy val macroSettings = Seq(
  libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  addCompilerPlugin(paradiseDependency)
)

lazy val noPublishSettings = Seq(
  publish := {},
  publishLocal := {},
  publishArtifact := false
)

lazy val root: Project = Project(
  "dataflow-example",
  file(".")
).settings(
  commonSettings ++ macroSettings ++ noPublishSettings,
  description := "dataflow-example",
  libraryDependencies ++= Seq(
    "com.spotify" %% "scio-core" % scioVersion,
    "com.spotify" %% "scio-test" % scioVersion % "test",
    "com.spotify" %% "scio-jdbc" % scioVersion,
    //local runner
    "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
    //remote runner
    "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion,
    "org.slf4j" % "slf4j-simple" % "1.7.25",

    //json extraction libraries
    "com.spotify" %% "scio-extra" % scioVersion,
    "io.circe" %% "circe-core" % circeVersion,
    "io.circe" %% "circe-generic" % circeVersion,
    "io.circe" %% "circe-parser" % circeVersion
  )
).enablePlugins(PackPlugin)