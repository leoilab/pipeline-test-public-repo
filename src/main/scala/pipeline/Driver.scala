package pipeline

import org.apache.spark.sql.functions.{lit, unix_timestamp}

object Driver {

  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSessionAndConfigBuilder.setupSpark()

    val clinicalCharacteristics = WarehouseReader.openClinicalCharacteristics()
    val derms = WarehouseReader.openDerm()
    val evaluations  = WarehouseReader.openEvaluation()
    val unfitReasons = WarehouseReader.openUnfitReason()

    Transformer
      .transform(
        clinicalCharacteristics,
        derms,
        evaluations,
        unfitReasons
      )(spark)
      .withColumn("timestamp", lit(unix_timestamp())) // for folder partitioning
      .coalesce(1) // create a single file by coalescing the Spark partitions
      .write
      .format("csv")
      .option("header","true")
      // good practice during the dev phase, to store temporary results in separate folders
      .mode("append")
      .partitionBy("timestamp")
      .save("./result.csv")

    //Thread.sleep(60000)
  }
}
