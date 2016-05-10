package eu.stratosphere.emma.streaming.examples.webshop

import eu.stratosphere.emma.streaming.examples.webshop.RecommendationPurchase._
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.typeinfo.{TypeInformation, TypeHint}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object FlinkRecommendationPurchase {

  def recomPurchaseJoin(purchases: DataStream[Purchase], recoms: DataStream[Recommendation], closeness: Int)
  : DataStream[(Purchase, Recommendation)] = {

    val xs = recoms.connect(purchases)
      .keyBy(r => r.userId, p => p.userId)
      .flatMap(new RichCoFlatMapFunction[Recommendation, Purchase, (Purchase, Recommendation)] {

        var prevRecoms: ValueState[TreeTimedBuffer[Int, Recommendation]] = null

        override def open(parameters: Configuration): Unit = {
          val desc = new ValueStateDescriptor[TreeTimedBuffer[Int, Recommendation]](
            "prevRecoms",
            TypeInformation.of(new TypeHint[TreeTimedBuffer[Int, Recommendation]] {}),
            new TreeTimedBuffer[Int, Recommendation](_.time)
          )

          prevRecoms = getRuntimeContext.getState[TreeTimedBuffer[Int, Recommendation]](desc)
        }

        override def flatMap2(p: Purchase, out: Collector[(Purchase, Recommendation)]): Unit = {
          println(p)
          val closeRecoms = prevRecoms.value().removeOlderThan(p.time - closeness)
          for (r <- closeRecoms.getRange(p.time - closeness, p.time)) {
            out.collect((p, r))
          }

          prevRecoms.update(closeRecoms)
        }

        override def flatMap1(r: Recommendation, out: Collector[(Purchase, Recommendation)]): Unit = {
          println(r)
          prevRecoms.update(prevRecoms.value().append(r))
        }
      })

    xs
  }
}
