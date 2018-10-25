package pipeline

import org.apache.spark.sql.{SparkSession, Dataset, DataFrame}
import org.apache.spark.sql.functions._
import Models._

object MultipleReadTransformer {

  def transform(
      clinicalCharacteristics: Dataset[ClinicalCharacteristic],
      derms:                   Dataset[Derm],
      evaluations:             Dataset[Evaluation],
      unfitReasons:            Dataset[UnfitReason]
  )(implicit spark:            SparkSession): DataFrame = {

    val evaluationsUpdated = evaluations.withColumnRenamed("id", "evaluationId")
    val dermsUpdated = derms.withColumnRenamed("id", "dermId") // broadcasted
    val clinicalCharacteristicsUpdated = clinicalCharacteristics.withColumnRenamed("name", "clinicalCharacteristicsName")
    val unfitReasonsUpdated = unfitReasons // in the end no transformation

    // TODO: these two should be set for large size jobs
    //spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")
    //spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1) // disable auto broadcast

    val evaluationsJoined = evaluationsUpdated
      // force to broadcast as derms likely is a dim table with number of rows that will not cause out of memory (assumption)
      // inner join as I assume derms must exist per each evaluation
      .join(broadcast(dermsUpdated), dermsUpdated("dermId") === evaluationsUpdated("dermId"), "inner")
      // as unfitReasons and clinicalCharacteristics do not need to exist, left_outer join is used
      .join(unfitReasonsUpdated, unfitReasonsUpdated("evaluationId") === evaluationsUpdated("evaluationId"), "left_outer")
      .join(clinicalCharacteristicsUpdated, clinicalCharacteristicsUpdated("evaluationId") === evaluationsUpdated("evaluationId"), "left_outer")
      // to get rid of column name clash
      .drop(evaluationsUpdated("dermId"))
      .drop(unfitReasonsUpdated("evaluationId"))
      .drop(clinicalCharacteristicsUpdated("evaluationId"))
      .toDF

    // final table is constructed by 4 separate pivots
    val evp1 = evaluationsJoined
      .withColumn("pivot_column", concat(evaluationsJoined("name"), lit("_evaluationId")))
      .groupBy("imageId")
      .pivot("pivot_column")
      .agg(max("evaluationId"))

    val evp2 = evaluationsJoined
      .withColumn("pivot_column", concat(evaluationsJoined("name"), lit("_diagnosis")))
      .groupBy("imageId")
      .pivot("pivot_column")
      .agg(max("diagnosis"))

    val evp3 = evaluationsJoined
      .withColumn("pivot_column", concat(evaluationsJoined("name"), lit("_"), evaluationsJoined("clinicalCharacteristicsName")))
      .groupBy("imageId")
      .pivot("pivot_column")
      .agg(max("selected"))

    val evp4 = evaluationsJoined
      .withColumn("pivot_column", concat(evaluationsJoined("name"), lit("_"), evaluationsJoined("unfitReason")))
      .withColumn("unfitReasonBool", !evaluationsJoined("unfitReason").isNull)
      .groupBy("imageId")
      .pivot("pivot_column")
      .agg(max("unfitReasonBool"))

    // joining separate pivots into a single job
    val evaluationPivot =
      evp1
        .join(evp2, "imageId")
        .join(evp3, "imageId")
        .join(evp4, "imageId")
        .drop("null")

    // order the columns in the same way as the expected result
    val columnNames = new collection.mutable.ArrayBuffer[String]
    columnNames.append("imageId")
    columnNames.appendAll(evaluationPivot.columns.filter(c => !Seq("imageId", "null").contains(c)).sorted(Ordering[String].reverse))
    val columns = columnNames.map(c => evaluationPivot(c)).toArray
    // order the rows in the same way as in the expected result
    val evaluationPivotOrdered = evaluationPivot.select(columns :_*).orderBy(evaluationPivot("imageId"))

    evaluationPivotOrdered
  }
}
