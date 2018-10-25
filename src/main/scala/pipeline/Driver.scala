package pipeline

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
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



  private def loadCsv[M : Encoder](file: String, schema: CsvSchema.Schema[M])(implicit spark: SparkSession): Dataset[M] = {
    import spark.implicits._

    spark.read
      .format("csv")
      .schema(schema)
      .option("header", "true")
      .option("mode", "FAILFAST")
      .option("treatEmptyValuesAsNulls", "true")
      .load(file)
      .toDF()
      .as[M]
  }
}
