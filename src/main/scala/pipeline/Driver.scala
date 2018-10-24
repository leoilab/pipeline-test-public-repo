package pipeline

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.apache.spark.sql.functions.{lit, unix_timestamp}
import Models._

object Driver {

  def main(args: Array[String]): Unit = {
    implicit val spark = setupSpark()
    import spark.implicits._

    val clinicalCharacteristics = loadCsv[ClinicalCharacteristic](
      "./data/clinicalCharacteristics.csv",
      CsvSchema.clinicalCharacteristic
    )
    val derms = loadCsv[Derm]("./data/derms.csv", CsvSchema.derm)
    val evaluations  = loadCsv[Evaluation]("./data/evaluations.csv", CsvSchema.evaluation)
    val unfitReasons = loadCsv[UnfitReason]("./data/unfitReasons.csv", CsvSchema.unfitReason)

    Transformer
      .transform(
        clinicalCharacteristics,
        derms,
        evaluations,
        unfitReasons
      )(spark)
      .withColumn("timestamp", lit(unix_timestamp())) // for folder partitioning
      .write
      .format("csv")
      .option("header","true")
      // good practice during the dev phase, to store temporary results
      .mode("append")
      .partitionBy("timestamp")
      .save("./result.csv")
  }

  private def setupSpark(): SparkSession = {
    val appName = "pipeline"
    val conf    = new SparkConf().setAppName(appName).setMaster("local[*]")

    SparkSession.builder().config(conf).appName(appName).getOrCreate()
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
