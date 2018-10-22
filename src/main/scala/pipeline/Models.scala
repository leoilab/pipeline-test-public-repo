package pipeline

object Models {

  case class ClinicalCharacteristic(evaluationId: Long, name:        String, selected: Option[Boolean])
  case class EvaluationQueue(dermId: String, imageId: Long, externalImageId: String, isParked: Boolean, isBackboneTest: Boolean)
  case class EvaluationProperty(evaluationId:     Long, propertyKey: String, propertyValue: String)
  case class UnfitReason(evaluationId:            Long, unfitReason: String)
  case class Image(
      id:              Long,
      externalImageId: String,
      platformImageId: String,
      patientId:       String,
      isHoldout:       Boolean,
      isFace:          Boolean,
      source:          String,
      createdAt:       java.sql.Timestamp
  )
  case class Evaluation(
      id:             Long,
      imageId:        Long,
      outlineId:      Option[Long],
      batchId:        String,
      diagnosis:      String,
      certainty:      Option[String],
      dermId:         String,
      createdAt:      java.sql.Timestamp,
      tagVersion:     String,
      isBackboneTest: Boolean
  )
  case class Outline(
      id:             Long,
      outlineSetId:   Long,
      imageId:        Long,
      dermId:         String,
      createdAt:      java.sql.Timestamp,
      outlineType:    String,
      tagVersion:     String,
      batchId:        String,
      isBackboneTest: Boolean
  )

  case class Derm(
      id:        String,
      name:      String,
      createdAt: java.sql.Timestamp
  )
}
