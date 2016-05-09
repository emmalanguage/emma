package eu.stratosphere.emma.streaming.examples

import eu.stratosphere.emma.api.DataBag
import eu.stratosphere.emma.streaming.api.StreamBag
import eu.stratosphere.emma.streaming.examples.RecommendationPurchase._
import org.scalatest._

class RecommendationPurchaseTest extends FlatSpec with Matchers {

  "Ads " should "be matched with purchases in 3 milliseconds" in {
    val ads = StreamBag.fromListOfBags(Seq(
      DataBag(Seq(Recommendation(recomId = 0, userId = 10), Recommendation(recomId = 1, userId = 30)))
      , DataBag(Seq())
      , DataBag(Seq(Recommendation(recomId = 2, userId = 10)))
      , DataBag(Seq())
    ))

    val purchases = StreamBag.fromListOfBags(Seq(
      DataBag(Seq())
      , DataBag(Seq(Purchase(userId = 10, itemId = 200)))
      , DataBag(Seq(Purchase(userId = 30, itemId = 400)))
      , DataBag(Seq(Purchase(userId = 30, itemId = 400), Purchase(userId = 10, itemId = 300)))
    ))

    val expected = StreamBag.fromListOfBags(Seq(
      DataBag(Seq())
      , DataBag(Seq((Purchase(userId = 10, itemId = 200), Recommendation(recomId = 0, userId = 10))))
      , DataBag(Seq((Purchase(userId = 30, itemId = 400), Recommendation(recomId = 1, userId = 30))))
      , DataBag(Seq((Purchase(userId = 10, itemId = 300), Recommendation(recomId = 2, userId = 10))))
    ))

    val result = RecommendationPurchase.recomPurchaseJoin(purchases, ads, 3)

    result.take(4) should be (expected.take(4))

  }
}
