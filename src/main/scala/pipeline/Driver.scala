package pipeline

import java.nio.file.Paths
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Driver {

  def main(args: Array[String]): Unit = {
    Transformer.transform(setupSpark(), Paths.get("./data"))
      .write
      .csv("./result.csv")
  }

  private def setupSpark(): SparkSession = {
    val appName = "pipeline"
    val conf = new SparkConf().setAppName(appName).setMaster("local[*]")

    SparkSession.builder().config(conf).appName(appName).getOrCreate()
  }
}
