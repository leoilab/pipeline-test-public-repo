package pipeline

import org.apache.spark.sql.{SparkSession, Dataset, DataFrame}
import org.apache.spark.sql.functions.{broadcast}
import Models._

object Transformer {

  def transform(
      clinicalCharacteristics: Dataset[ClinicalCharacteristic],
      derms:                   Dataset[Derm],
      evaluations:             Dataset[Evaluation],
      unfitReasons:            Dataset[UnfitReason]
  )(implicit spark:            SparkSession): DataFrame = {
    //derms.toDF // original code, is the transformation missing?

    val evaluationsUpdated = evaluations.withColumnRenamed("id", "evaluationId")
    val dermsUpdated = derms.withColumnRenamed("id", "dermId") // broadcasted
    val clinicalCharacteristicsUpdated = clinicalCharacteristics.withColumnRenamed("name", "clinicalCharacteristicsName")
    val unfitReasonsUpdated = unfitReasons

    spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1) // disable auto broadcast

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

    println(evaluationsJoined.explain)
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

    // NOTE: sort-merge-join should not cause driver failure, so I assume this was not your case. Sort-merge-join would
    // trigger a full shuffle phase which likely would be costly, but should not affect the driver. Worst cast scenario
    // would be heavy skew on the partitions, which would cause some workers to take longer to complete, and in extreme
    // case would start spillng to the disk. In absolute worst case, the disk space could run out, thus causing the
    // worker to fail, and which would be unrecoverable as the defualt recovery scenario (4 retries) would of course
    // not help.
    // After giving it some thought, the best candidate for failing your code due to out of memory of the driver, would
    // be if you enforced broadcast join on one of the fact tables. However this is just my guess based on simply
    // thinking about the problem, and not seeing your case.

    evaluationsJoined
  }

}
