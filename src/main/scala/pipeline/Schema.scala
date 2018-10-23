package pipeline

import org.apache.spark.sql.types._
import Models._

object CsvSchema {

  type Schema[T] = StructType

  val clinicalCharacteristic: Schema[ClinicalCharacteristic] = new StructType()
      .add("evaluationId", LongType, nullable = false)
      .add("name", StringType, nullable = false)
      .add("selected", BooleanType, nullable = true)

  val evaluationProperty = new StructType()
      .add("evaluationId", LongType, nullable = false)
      .add("propertyKey", StringType, nullable = false)
      .add("propertyValue", StringType, nullable = false)

  val unfitReason = new StructType()
      .add("evaluationId", LongType, nullable = false)
      .add("unfitReason", StringType, nullable = false)

  val image = new StructType()
      .add("id", LongType, nullable = false)
      .add("externalImageId", StringType, nullable = false)
      .add("platformImageId", StringType, nullable = false)
      .add("patientId", StringType, nullable = false)
      .add("isHoldout", BooleanType, nullable = false)
      .add("isFace", BooleanType, nullable = false)
      .add("source", StringType, nullable = false)
      .add("createdAt", TimestampType, nullable = false)

  val evaluation = new StructType()
      .add("id", LongType, nullable = false)
      .add("imageId", LongType, nullable = false)
      .add("outlineId", LongType, nullable = false)
      .add("batchId", StringType, nullable = false)
      .add("diagnosis", StringType, nullable = false)
      .add("certainty", StringType, nullable = true)
      .add("dermId", StringType, nullable = false)
      .add("createdAt", TimestampType, nullable = false)
      .add("tagVersion", StringType, nullable = false)
      .add("isBackboneTest", BooleanType, nullable = false)

  val derm = new StructType()
      .add("id", StringType, nullable = false)
      .add(name = "name", StringType, nullable = false)
      .add("createdAt", TimestampType, nullable = false)

}
