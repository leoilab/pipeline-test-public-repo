package pipeline

import org.apache.spark.sql.SparkSession
//import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}
//import scala.collection.mutable

class CoreUnitTest extends FunSuite with
  BeforeAndAfterEach with BeforeAndAfterAll{

  @transient implicit var spark: SparkSession = null
  @transient private val clinicalCharacteristics = WarehouseReader.openClinicalCharacteristics()(spark)
  @transient private val derms = WarehouseReader.openDerm()(spark)
  @transient private val evaluations  = WarehouseReader.openEvaluation()(spark)
  @transient private val unfitReasons = WarehouseReader.openUnfitReason()(spark)

  override def beforeAll(): Unit = {
    spark = SparkSessionAndConfigBuilder.setupSpark()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("Test word count") {
    val expectedStringOutput = """1,1,L40.0,true,false,2,NEP,true,true,,
                                 |2,3,L20.2,false,false,4,L41.0,,,true,true
                                 |3,,,,,5,NEP,true,true,,"""
    val dataFrameResult = Transformer.transform(clinicalCharacteristics, derms, evaluations, unfitReasons)
    val actualStringOutput = dataFrameResult.collect.map(_.mkString(",")).mkString("\n").replace("null", "")

    assert(expectedStringOutput == actualStringOutput)
//    val quotesRDD = sc.parallelize(Seq("Courage is not simply one of the virtues, but the form of every virtue at the testing point",
//      "We have a very active testing community which people don't often think about when you have open source",
//      "Program testing can be used to show the presence of bugs, but never to show their absence",
//      "Simple systems are not feasible because they require infinite testing",
//      "Testing leads to failure, and failure leads to understanding"))
//
//    val wordCountRDD = quotesRDD.flatMap(r => r.split(' ')).
//      map(r => (r.toLowerCase, 1)).
//      reduceByKey((a,b) => a + b)
//
//    val wordMap = new mutable.HashMap[String, Int]()
//    wordCountRDD.take(100).
//      foreach{case(word, count) => wordMap.put(word, count)}
//    //Note this is better then foreach(r => wordMap.put(r._1, r._2)
//
//    assert(wordMap.get("to").get == 4, "The word count for 'to' should had been 4 but it was " + wordMap.get("to").get)
//    assert(wordMap.get("testing").get == 5, "The word count for 'testing' should had been 5 but it was " + wordMap.get("testing").get)
//    assert(wordMap.get("is").get == 1, "The word count for 'is' should had been 1 but it was " + wordMap.get("is").get)
  }
}

//class FibonacciFunSuite extends FunSuite with  {
//
//    test("Test Transformer.transform: output file should match") {
//      val expectedOutput = """1,1,L40.0,true,false,2,NEP,true,true,,
//                     |2,3,L20.2,false,false,4,L41.0,,,true,true
//                     |3,,,,,5,NEP,true,true,,"""
//
//      val assertResult = Set.empty.size == 0
//      assert(assertResult)
//    }
//
//}
