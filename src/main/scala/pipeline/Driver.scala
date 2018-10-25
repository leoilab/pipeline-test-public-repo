package pipeline

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{lit, unix_timestamp}

object Driver {

  def main(args: Array[String]): Unit = {
    // Simple benchmark
    val start = System.nanoTime()

    implicit val spark = SparkSessionAndConfigBuilder.setupSpark()

    //runMultipleReadTransformer()
    runExperimentalSingleReadTransformer()
      .withColumn("timestamp", lit(unix_timestamp())) // for folder partitioning
      .coalesce(1) // create a single file by coalescing the Spark partitions
      .write
      .format("csv")
      .option("header","true")
      // good practice during the dev phase, to store temporary results in separate folders
      .mode("append")
      .partitionBy("timestamp")
      .save("./result.csv")

    println(s"total time: ${(System.nanoTime()-start)/1000000} miliseconds")
    Thread.sleep(300000) // when needs to look at the SparkUI
  }

  def runMultipleReadTransformer()(implicit spark: SparkSession): DataFrame = {
    MultipleReadTransformer
      .transform(
        DermatologicalEvaluationWarehouseReader.openClinicalCharacteristics,
        DermatologicalEvaluationWarehouseReader.openDerm,
        DermatologicalEvaluationWarehouseReader.openEvaluation,
        DermatologicalEvaluationWarehouseReader.openUnfitReason
      )
  }

  def runExperimentalSingleReadTransformer()(implicit spark: SparkSession): DataFrame = {
    ExperimentalSingleReadTransformer
      .transform(
        DermatologicalEvaluationWarehouseReader.openClinicalCharacteristicsAsStringDataFrame,
        DermatologicalEvaluationWarehouseReader.openDermAsStringDataFrame,
        DermatologicalEvaluationWarehouseReader.openEvaluationAsStringDataFrame,
        DermatologicalEvaluationWarehouseReader.openUnfitReasonAsStringDataFrame
      )
  }
}
