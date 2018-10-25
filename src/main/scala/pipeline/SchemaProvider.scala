package pipeline

import org.apache.spark.sql.types._
import Models._

object SchemaProvider {

  type Schema[T] = StructType

  val clinicalCharacteristic: Schema[ClinicalCharacteristic] = new StructType()
    .add("evaluationId", LongType, nullable = false)
    .add("name", StringType, nullable = false)
    .add("selected", BooleanType, nullable = true)
  val clinicalCharacteristicString: StructType = createStringStructType(clinicalCharacteristic)

  val unfitReason = new StructType()
    .add("evaluationId", LongType, nullable = false)
    .add("unfitReason", StringType, nullable = false)
  val unfitReasonString: StructType = createStringStructType(unfitReason)

  val evaluation = new StructType()
    .add("id", LongType, nullable = false)
    .add("imageId", LongType, nullable = false)
    .add("diagnosis", StringType, nullable = true)
    .add("dermId", StringType, nullable = false)
  val evaluationString: StructType = createStringStructType(evaluation)

  val derm = new StructType()
    .add("id", StringType, nullable = false)
    .add(name = "name", StringType, nullable = false)
  val dermString: StructType = createStringStructType(derm)

  private def createStringStructType(t: StructType) = new StructType(t.map(f => new StructField(f.name, StringType, f.nullable)).toArray)
}
