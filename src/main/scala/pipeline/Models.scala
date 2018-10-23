package pipeline

object Models {

  case class ClinicalCharacteristic(evaluationId: Long, name:        String, selected: Boolean)

  case class UnfitReason(evaluationId:            Long, unfitReason: String)

  case class Evaluation(
      id:             Long,
      imageId:        Long,
      diagnosis:      String,
      dermId:         String
  )

  case class Derm(
      id:        String,
      name:      String
  )
}
