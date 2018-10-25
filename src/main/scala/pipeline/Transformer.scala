package pipeline

import org.apache.spark.sql.{SparkSession, Dataset, DataFrame}
import org.apache.spark.sql.functions._
import Models._

object Transformer {

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

    //println(evaluationsJoined.explain)
    //== Physical Plan == without setting spark.sql.autoBroadcastJoinThreshold, add done as broadcast, a very poor test
    //*(4) Project [evaluationId#48L, imageId#27L, diagnosis#28, dermId#53, name#16, unfitReason#42, clinicalCharacteristicsName#56, selected#5]
    //+- *(4) BroadcastHashJoin [evaluationId#48L], [evaluationId#3L], LeftOuter, BuildRight
    //:- *(4) Project [evaluationId#48L, imageId#27L, diagnosis#28, dermId#53, name#16, unfitReason#42]
    //:  +- *(4) BroadcastHashJoin [evaluationId#48L], [evaluationId#41L], LeftOuter, BuildRight
    //  :     :- *(4) Project [evaluationId#48L, imageId#27L, diagnosis#28, dermId#53, name#16]
    //:     :  +- *(4) BroadcastHashJoin [dermId#29], [dermId#53], Inner, BuildRight
    //  :     :     :- *(4) Project [id#26L AS evaluationId#48L, imageId#27L, diagnosis#28, dermId#29]

    // == Physical Plan == with setting spark.sql.autoBroadcastJoinThreshold -1, better test (still poor), note enforced broadcast on the bottom
    // *(9) Project [evaluationId#48L, imageId#27L, diagnosis#28, dermId#53, name#16, unfitReason#42, clinicalCharacteristicsName#56, selected#5]
    // +- SortMergeJoin [evaluationId#48L], [evaluationId#3L], LeftOuter
    //    :- *(6) Project [evaluationId#48L, imageId#27L, diagnosis#28, dermId#53, name#16, unfitReason#42]
    //    :  +- SortMergeJoin [evaluationId#48L], [evaluationId#41L], LeftOuter
    //    :     :- *(3) Sort [evaluationId#48L ASC NULLS FIRST], false, 0
    //    :     :  +- Exchange hashpartitioning(evaluationId#48L, 200)
    //    :     :     +- *(2) Project [evaluationId#48L, imageId#27L, diagnosis#28, dermId#53, name#16]
    //    :     :        +- *(2) BroadcastHashJoin [dermId#29], [dermId#53], Inner, BuildRight
    //    :     :           :- *(2) Project [id#26L AS evaluationId#48L, imageId#27L, diagnosis#28, dermId#29]

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

    val evaluationPivotJoined =
      evp1
        .join(evp2, "imageId")
        .join(evp3, "imageId")
        .join(evp4, "imageId")
        .drop("null")

    val columnNames = new collection.mutable.ArrayBuffer[String]
    columnNames.append("imageId")
    columnNames.appendAll(evaluationPivotJoined.columns.filter(_ != "imageId").sorted(Ordering[String].reverse))

    val columns = columnNames.map(c => evaluationPivotJoined(c)).toArray

    val evaluationPivotOrdered = evaluationPivotJoined.select(columns :_*).orderBy(evaluationPivotJoined("imageId"))

    evaluationPivotOrdered
  }

}
