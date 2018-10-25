package pipeline

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.udf

object ExperimentalSingleReadTransformer {

  // Spark UDF: Creates a denormalized value column for the pivot
  def getPivotValueUDF = udf((r:Row) => {
    val pivotKey = r.getAs[String]("pivotKey")
    pivotKey match {
      case "unfitReason" => if(r.getAs[String]("unfitReason") != null) "true" else null
      case "clinicalCharacteristicsName" => r.getAs[String]("selected")
      case _ => r.getAs[String](pivotKey)
    }
  })
  // Spark UDF: Creates a denormalized key column for the pivot
  def getPivotKeyCompositeUDF = udf((r:Row) => {
    val pivotKey = r.getAs[String]("pivotKey")
    pivotKey match {
      case "unfitReason" => r.getAs[String]("unfitReason")
      case "clinicalCharacteristicsName" => r.getAs[String]("clinicalCharacteristicsName")
      case _ => pivotKey
    }
  })

  def transform(
      clinicalCharacteristics: DataFrame,
      derms:                   DataFrame,
      evaluations:             DataFrame,
      unfitReasons:            DataFrame
  )(implicit spark:            SparkSession): DataFrame = {
    // initial join is identical to the MultipleReadTransformer
    val evaluationsUpdated = evaluations.withColumnRenamed("id", "evaluationId")
    val dermsUpdated = derms.withColumnRenamed("id", "dermId") // broadcasted
    val clinicalCharacteristicsUpdated = clinicalCharacteristics.withColumnRenamed("name", "clinicalCharacteristicsName")
    val unfitReasonsUpdated = unfitReasons // in the end no transformation

    // TODO: these two should be set for large size jobs
    //spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")
    //spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1) // disable auto broadcast

    val evaluationsJoined = evaluationsUpdated
      .join(broadcast(dermsUpdated), dermsUpdated("dermId") === evaluationsUpdated("dermId"), "inner")
      .join(unfitReasonsUpdated, unfitReasonsUpdated("evaluationId") === evaluationsUpdated("evaluationId"), "left_outer")
      .join(clinicalCharacteristicsUpdated, clinicalCharacteristicsUpdated("evaluationId") === evaluationsUpdated("evaluationId"), "left_outer")
      .drop(evaluationsUpdated("dermId"))
      .drop(unfitReasonsUpdated("evaluationId"))
      .drop(clinicalCharacteristicsUpdated("evaluationId"))
      .toDF

    // explode the rows (multiply the row count), creating 4 rows for each one with values:
    // row 1, column 'pivotKey': value evaluationId
    // row 2, column 'pivotKey': value diagnosis
    // row 3, column 'pivotKey': value unfitReason
    // row 4, column 'pivotKey': value clinicalCharacteristicsName
    val evaluationExploded = evaluationsJoined
      .drop("dermId")
      .withColumn("pivotKey", explode(split(lit("evaluationId,diagnosis,unfitReason,clinicalCharacteristicsName"), ",")))
    // pack whole row into a single structred column under alias 'r'
    val evaluationRowPacked = evaluationExploded
      .select(struct("*").alias("r"), evaluationExploded("imageId"), evaluationExploded("name"), evaluationExploded("pivotKey"), evaluationExploded("unfitReason"), evaluationExploded("clinicalCharacteristicsName"))
    // table denormalisation: create the pivot key and pivot value columns denormalizing the column structure into rows
    val evaluationPivotValue = evaluationRowPacked
      .withColumn("pivotKeyComposite", getPivotKeyCompositeUDF(evaluationRowPacked("r")))
      .withColumn("pivotValue", getPivotValueUDF(evaluationRowPacked("r")))

    // perform the pivot creating all columns in a single Spark operation (of course it will still be more then one map
    // reduce steps, but at least asingle job
    val evaluationPivot = evaluationPivotValue
      .withColumn("pivotKeyConcatenated", concat(evaluationPivotValue("name"), lit("_"), evaluationPivotValue("pivotKeyComposite")))
      .groupBy("imageId")
      .pivot("pivotKeyConcatenated")
      .agg(max("pivotValue"))

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
