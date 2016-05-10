package eu.stratosphere.emma.streaming.examples.webshop

import eu.stratosphere.emma.api.DataBag
import eu.stratosphere.emma.streaming.api.StreamBag
import eu.stratosphere.emma.streaming.examples.webshop.RecommendationPurchase._
import eu.stratosphere.emma.streaming.examples.webshop.RecommendationPurchaseTest._
import org.scalatest._

class RecommendationPurchaseTest extends FlatSpec with Matchers {

  "Ads " should "be matched with purchases in 3 milliseconds" in {

    val ads = StreamBag.fromListOfBags(Seq(
      DataBag(Seq(recom0_10, recom0_30))
      , DataBag(Seq())
      , DataBag(Seq(recom2_10))
      , DataBag(Seq())
    ))

    val purchases = StreamBag.fromListOfBags(Seq(
      DataBag(Seq())
      , DataBag(Seq(purchase1_10))
      , DataBag(Seq(purchase2_30))
      , DataBag(Seq(purchase3_30, purchase3_10))
    ))

    val expected = StreamBag.fromListOfBags(Seq(
      DataBag(Seq())
      , DataBag(Seq((purchase1_10, recom0_10)))
      , DataBag(Seq((purchase2_30, recom0_30)))
      , DataBag(Seq((purchase3_10, recom2_10)))
    ))

    val result = RecommendationPurchase.recomPurchaseJoin(purchases, ads, 3)

    result.take(4) should be (expected.take(4))

  }
}

object RecommendationPurchaseTest {

  val recom0_10 = Recommendation(recomId = 0, userId = 10, time = 0)
  val recom0_30 = Recommendation(recomId = 1, userId = 30, time = 0)
  val recom2_10 = Recommendation(recomId = 2, userId = 10, time = 2)

  val purchase1_10 = Purchase(userId = 10, itemId = 200, time = 1)
  val purchase2_30 = Purchase(userId = 30, itemId = 400, time = 2)
  val purchase3_30 = Purchase(userId = 30, itemId = 400, time = 3)
  val purchase3_10 = Purchase(userId = 10, itemId = 300, time = 3)

}
