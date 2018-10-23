package pipeline

import org.apache.spark.sql.types._
import Models._

object CsvSchema {

  type Schema[T] = StructType

  val clinicalCharacteristic: Schema[ClinicalCharacteristic] = new StructType()
      .add("evaluationId", LongType, nullable = false)
      .add("name", StringType, nullable = false)
      .add("selected", BooleanType, nullable = true)

  val unfitReason = new StructType()
      .add("evaluationId", LongType, nullable = false)
      .add("unfitReason", StringType, nullable = false)

  val evaluation = new StructType()
      .add("id", LongType, nullable = false)
      .add("imageId", LongType, nullable = false)
      .add("diagnosis", StringType, nullable = true)
      .add("dermId", StringType, nullable = false)

  val derm = new StructType()
      .add("id", StringType, nullable = false)
      .add(name = "name", StringType, nullable = false)

}
