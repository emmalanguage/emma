package eu.stratosphere.emma.streaming.examples.webshop

import eu.stratosphere.emma.api.DataBag
import eu.stratosphere.emma.streaming.api.StreamBag
import eu.stratosphere.emma.streaming.examples.webshop.RecommendationPurchase._
import org.scalatest._

class RecommendationPurchaseTest extends FlatSpec with Matchers {

  "Ads " should "be matched with purchases in 3 milliseconds" in {
    val ads = StreamBag.fromListOfBags(Seq(
      DataBag(Seq(
        Recommendation(recomId = 0, userId = 10, time = 0)
        , Recommendation(recomId = 1, userId = 30, time = 0)))
      , DataBag(Seq())
      , DataBag(Seq(Recommendation(recomId = 2, userId = 10, time = 2)))
      , DataBag(Seq())
    ))

    val purchases = StreamBag.fromListOfBags(Seq(
      DataBag(Seq())
      , DataBag(Seq(Purchase(userId = 10, itemId = 200, time = 1)))
      , DataBag(Seq(Purchase(userId = 30, itemId = 400, time = 2)))
      , DataBag(Seq(Purchase(userId = 30, itemId = 400, time = 3), Purchase(userId = 10, itemId = 300, time = 3)))
    ))

    val expected = StreamBag.fromListOfBags(Seq(
      DataBag(Seq())
      , DataBag(Seq(
        (Purchase(userId = 10, itemId = 200, time = 1), Recommendation(recomId = 0, userId = 10, time = 0))))
      , DataBag(Seq(
        (Purchase(userId = 30, itemId = 400, time = 2), Recommendation(recomId = 1, userId = 30, time = 0))))
      , DataBag(Seq(
        (Purchase(userId = 10, itemId = 300, time = 3), Recommendation(recomId = 2, userId = 10, time = 2))))
    ))

    val result = RecommendationPurchase.recomPurchaseJoin(purchases, ads, 3)

    result.take(4) should be (expected.take(4))

  }
}
