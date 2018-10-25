package pipeline

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

package object SparkSessionAndConfigBuilder {
  def setupSpark(): SparkSession = {
    val appName = "pipeline"
    val conf    = new SparkConf().setAppName(appName).setMaster("local[*]")

    SparkSession.builder().config(conf).appName(appName).getOrCreate()
  }
}
