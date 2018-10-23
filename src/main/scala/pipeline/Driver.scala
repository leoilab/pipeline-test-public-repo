package pipeline

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, Dataset, Encoder}
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
    val evaluationProperties = loadCsv[EvaluationProperty](
      "./data/evaluationProperties.csv",
      CsvSchema.evaluationProperty
    )
    val evaluations  = loadCsv[Evaluation]("./data/evaluations.csv", CsvSchema.evaluation)
    val images       = loadCsv[Image]("./data/images.csv", CsvSchema.image)
    val unfitReasons = loadCsv[UnfitReason]("./data/unfitReasons.csv", CsvSchema.unfitReason)

    Transformer
      .transform(
        clinicalCharacteristics,
        derms,
        evaluationProperties,
        evaluations,
        images,
        unfitReasons
      )(spark)
      .write.format("csv")
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
