package pipeline

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import pipeline.Models._

object DermatologicalEvaluationWarehouseReader {

  // TODO: normally from config file, but for this purpose this is good enough
  private val clinicalCharacteristicPath = "./data/clinicalCharacteristics.csv"
  private val dermPath = "./data/derms.csv"
  private val evaluationsPath = "./data/evaluations.csv"
  private val unfitReasonsPath = "./data/unfitReasons.csv"

  private def loadCsvDataFrame(file: String, schema: StructType)(implicit spark: SparkSession): DataFrame = {
    spark.read
      .format("csv")
      .schema(schema)
      .option("header", "true")
      .option("mode", "FAILFAST")
      .option("treatEmptyValuesAsNulls", "true")
      .load(file)
      .toDF()
  }
  private def loadCsv[M : Encoder](file: String, schema: SchemaProvider.Schema[M])(implicit spark: SparkSession): Dataset[M] = {
    loadCsvDataFrame(file, schema).as[M]
  }

  def openClinicalCharacteristicsAsStringDataFrame()(implicit spark: SparkSession): DataFrame = loadCsvDataFrame(clinicalCharacteristicPath, SchemaProvider.clinicalCharacteristicString)
  def openClinicalCharacteristics()(implicit spark: SparkSession): Dataset[Models.ClinicalCharacteristic] = {
    import spark.implicits._
    loadCsv[ClinicalCharacteristic](clinicalCharacteristicPath,SchemaProvider.clinicalCharacteristic)
  }

  def openDermAsStringDataFrame()(implicit spark: SparkSession): DataFrame = loadCsvDataFrame(dermPath, SchemaProvider.dermString)
  def openDerm()(implicit spark: SparkSession): Dataset[Models.Derm] = {
    import spark.implicits._
    loadCsv[Derm](dermPath, SchemaProvider.derm)
  }

  def openEvaluationAsStringDataFrame()(implicit spark: SparkSession): DataFrame = loadCsvDataFrame(evaluationsPath, SchemaProvider.evaluationString)
  def openEvaluation()(implicit spark: SparkSession): Dataset[Models.Evaluation] = {
    import spark.implicits._
    loadCsv[Evaluation](evaluationsPath, SchemaProvider.evaluation)
  }

  def openUnfitReasonAsStringDataFrame()(implicit spark: SparkSession): DataFrame = loadCsvDataFrame(unfitReasonsPath, SchemaProvider.unfitReasonString)
  def openUnfitReason()(implicit spark: SparkSession): Dataset[Models.UnfitReason] = {
    import spark.implicits._
    loadCsv[UnfitReason](unfitReasonsPath, SchemaProvider.unfitReason)
  }

}
