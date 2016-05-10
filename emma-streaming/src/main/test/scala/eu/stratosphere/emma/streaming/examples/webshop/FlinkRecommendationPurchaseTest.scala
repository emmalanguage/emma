package eu.stratosphere.emma.streaming.examples.webshop

import eu.stratosphere.emma.streaming.examples.webshop.RecommendationPurchase._
import eu.stratosphere.emma.streaming.examples.webshop.RecommendationPurchaseTest._
import org.apache.flink.runtime.client.JobExecutionException
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._
import org.scalatest._

class FlinkRecommendationPurchaseTest extends FlatSpec with Matchers {

  "Recommendations " should "be matched with purchases in 2 milliseconds" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inpSq = Seq(
      Left(recom0_10)
      , Left(recom0_30)
      , Right(purchase1_10)
      , Left(recom2_10)
      , Right(purchase2_30)
      , Right(purchase3_30)
      , Right(purchase3_10)
    )

    val expected = Seq(
      (purchase1_10, recom0_10)
      , (purchase2_30, recom0_30)
      , (purchase3_10, recom2_10)
    )

    val src = env.fromCollection(inpSq)
    val recoms = src.flatMap(_.left.toOption.toTraversable)
    val purchases = src.flatMap(_.right.toOption.toTraversable)

    FlinkRecommendationPurchase.recomPurchaseJoin(purchases, recoms, 2)
      .addSink(new FlinkTestResultSink[(Purchase, Recommendation)]())

    try {
      env.execute()
    } catch {
      case e: JobExecutionException => {
        e.getCause match {
          case resExcept: ResultRetrieveSuccess[Seq[(Purchase, Recommendation)]] => {
            resExcept.result should be(expected)
          }
        }
      }
      case _ => fail
    }
  }

}

case class ResultRetrieveSuccess[A](result: A) extends Exception

case class FlinkTestResultSink[A]() extends RichSinkFunction[A] with Serializable {
  var result: Seq[A] = Seq()

  override def invoke(value: A): Unit = {
    result = result :+ value
  }

  override def close(): Unit = {
    throw new ResultRetrieveSuccess[Seq[A]](result)
  }
}
