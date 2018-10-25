package pipeline

import org.apache.spark.sql.{Dataset, SparkSession}
//import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}
//import scala.collection.mutable

class CoreUnitTest extends FunSuite with
  BeforeAndAfterEach with BeforeAndAfterAll{

  implicit var spark: SparkSession = null
  var clinicalCharacteristics: Dataset[Models.ClinicalCharacteristic] = null
  var derms: Dataset[Models.Derm] = null
  var evaluations: Dataset[Models.Evaluation] = null
  var unfitReasons: Dataset[Models.UnfitReason] = null

  override def beforeAll(): Unit = {
    spark = SparkSessionAndConfigBuilder.setupSpark()

    clinicalCharacteristics = WarehouseReader.openClinicalCharacteristics()
    derms = WarehouseReader.openDerm()
    evaluations  = WarehouseReader.openEvaluation()
    unfitReasons = WarehouseReader.openUnfitReason()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("Test word count") {
    val expectedStringOutput =
      """1,1,L40.0,true,false,2,NEP,true,true,,#2,3,L20.2,false,false,4,L41.0,,,true,true#3,,,,,5,NEP,true,true,,"""
    val dataFrameResult = Transformer.transform(clinicalCharacteristics, derms, evaluations, unfitReasons)
    val actualStringOutput = dataFrameResult.collect.map(_.mkString(",")).mkString("#").replace("null", "")

    assert(expectedStringOutput == actualStringOutput)
  }
}
