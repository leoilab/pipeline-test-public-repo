import sbt.Keys._

lazy val pipeline = Project("pipeline", file("."))
  .settings(
    version := Settings.version,
    scalaVersion := Settings.versions.scala,
    scalacOptions ++= Settings.scalacOptions,
    scalacOptions in (Compile, console) ~= Settings.consoleOptionsFilter,
    libraryDependencies ++= Settings.dependencies.value
  )
