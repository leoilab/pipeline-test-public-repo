import sbt._

object Settings {

  val name = "pipeline"

  val version = "0.1"

  val scalacOptions = Seq(
    "-deprecation",
    "-encoding",
    "utf-8",
    "-explaintypes",
    "-feature",
    "-Ypartial-unification",
    "-language:existentials",
    "-language:higherKinds",
    "-language:implicitConversions",
    "-language:postfixOps",
    "-unchecked",
    "-Xfatal-warnings",
    "-Xfuture",
    "-Xlint",
    "-Yno-adapted-args",
    "-Ywarn-inaccessible",
    "-Ywarn-infer-any",
    "-Ywarn-nullary-override",
    "-Ywarn-nullary-unit",
    "-Ywarn-numeric-widen",
    "-Ywarn-value-discard",
    "-Ywarn-unused-import",
    "-Ywarn-dead-code"
  )

  def consoleOptionsFilter(options: Seq[String]): Seq[String] = {
    options.filterNot(x => x == "-Xfatal-warnings" || x == "-Xlint")
  }

  object versions {

    val scala = "2.11.9"

    val spark = "2.3.1"
  }

  val dependencies = Def.setting(
    Seq(
      "org.apache.spark" %% "spark-core" % versions.spark,
      "org.apache.spark" %% "spark-sql" % versions.spark,
      "org.scalatest" %% "scalatest" % "3.0.5", //% Test
      "org.junit.jupiter" % "junit-jupiter-api" % "5.3.0" //% Test
    )
  )

}
