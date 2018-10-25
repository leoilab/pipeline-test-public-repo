package pipeline

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import pipeline.Models._

object WarehouseReader {

  private def loadCsv[M : Encoder](file: String, schema: CsvSchema.Schema[M])(implicit spark: SparkSession): Dataset[M] = {
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

  def openClinicalCharacteristics()(implicit spark: SparkSession): Dataset[Models.ClinicalCharacteristic] = {
    import spark.implicits._
    loadCsv[ClinicalCharacteristic](
      "./data/clinicalCharacteristics.csv",
      CsvSchema.clinicalCharacteristic
    )
  }

  def openDerm()(implicit spark: SparkSession): Dataset[Models.Derm] = {
    import spark.implicits._
    loadCsv[Derm]("./data/derms.csv", CsvSchema.derm)
  }

  def openEvaluation()(implicit spark: SparkSession): Dataset[Models.Evaluation] = {
    import spark.implicits._
    loadCsv[Evaluation]("./data/evaluations.csv", CsvSchema.evaluation)
  }

  def openUnfitReason()(implicit spark: SparkSession): Dataset[Models.UnfitReason] = {
    import spark.implicits._
    loadCsv[UnfitReason]("./data/unfitReasons.csv", CsvSchema.unfitReason)
  }

}
