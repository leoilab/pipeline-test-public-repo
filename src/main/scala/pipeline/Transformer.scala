package pipeline

import org.apache.spark.sql.{SparkSession, Dataset, DataFrame}
import Models._

object Transformer {

  def transform(
      clinicalCharacteristics: Dataset[ClinicalCharacteristic],
      derms:                   Dataset[Derm],
      evaluationProperties:    Dataset[EvaluationProperty],
      evaluations:             Dataset[Evaluation],
      images:                  Dataset[Image],
      unfitReasons:            Dataset[UnfitReason]
  )(implicit spark:            SparkSession): DataFrame = {
    derms.toDF
  }

}
