package pipeline

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

package object SparkSessionAndConfigBuilder {
  def setupSpark(): SparkSession = {
    val appName = "pipeline"
    var conf    = new SparkConf()
    if (conf.contains("spark.master")) conf.setAppName(appName)
    else conf.setAppName(appName).setMaster("local[*]")

    println(s"Spark conf:\n${conf.getAll.mkString("\n")}")

    SparkSession.builder().config(conf).appName(appName).getOrCreate()
  }
}
