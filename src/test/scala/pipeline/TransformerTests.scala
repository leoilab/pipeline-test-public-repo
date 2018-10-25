package pipeline

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

class TransformerTests extends FunSuite with
  BeforeAndAfterEach with BeforeAndAfterAll{

  implicit var spark: SparkSession = null

  override def beforeAll(): Unit = spark = SparkSessionAndConfigBuilder.setupSpark()
  override def afterAll(): Unit = spark.stop()

  test("Test MultipleReadTransformer") {
    // arrange
    val clinicalCharacteristics = DermatologicalEvaluationWarehouseReader.openClinicalCharacteristics
    val derms = DermatologicalEvaluationWarehouseReader.openDerm
    val evaluations  = DermatologicalEvaluationWarehouseReader.openEvaluation
    val unfitReasons = DermatologicalEvaluationWarehouseReader.openUnfitReason
    val expectedStringOutput = """1,1,L40.0,true,false,2,NEP,true,true,,#2,3,L20.2,false,false,4,L41.0,,,true,true#3,,,,,5,NEP,true,true,,"""
    // act
    val dataFrameResult = MultipleReadTransformer.transform(clinicalCharacteristics, derms, evaluations, unfitReasons)
    // assert
    val actualStringOutput = dataFrameResult.collect.map(_.mkString(",")).mkString("#").replace("null", "")
    assert(expectedStringOutput == actualStringOutput)
  }

  test("Test ExperimentalSingleReadTransformer") {
    // arrange
    val clinicalCharacteristics = DermatologicalEvaluationWarehouseReader.openClinicalCharacteristicsAsStringDataFrame
    val derms = DermatologicalEvaluationWarehouseReader.openDermAsStringDataFrame
    val evaluations  = DermatologicalEvaluationWarehouseReader.openEvaluationAsStringDataFrame
    val unfitReasons = DermatologicalEvaluationWarehouseReader.openUnfitReasonAsStringDataFrame
    val expectedStringOutput = """1,1,L40.0,true,false,2,NEP,true,true,,#2,3,L20.2,false,false,4,L41.0,,,true,true#3,,,,,5,NEP,true,true,,"""
    // act
    val dataFrameResult = ExperimentalSingleReadTransformer.transform(clinicalCharacteristics, derms, evaluations, unfitReasons)
    // assert
    val actualStringOutput = dataFrameResult.collect.map(_.mkString(",")).mkString("#").replace("null", "")
    assert(expectedStringOutput == actualStringOutput)
  }
}
