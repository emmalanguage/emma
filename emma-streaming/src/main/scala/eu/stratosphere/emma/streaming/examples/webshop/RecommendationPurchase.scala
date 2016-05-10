package eu.stratosphere.emma.streaming.examples.webshop

import eu.stratosphere.emma.streaming.api.StreamBag

/*
  Match recommendations with the purchases
  that happened within 1 minute after showing the recommendation.
 */
object RecommendationPurchase {

  case class Purchase(userId: Int, itemId: Int, time: Int)

  case class Recommendation(recomId: Int, userId: Int, time: Int)

  def recomPurchaseJoinOneMinute(purchases: StreamBag[Purchase], recom: StreamBag[Recommendation])
  : StreamBag[(Purchase, Recommendation)] =
    recomPurchaseJoin(purchases, recom, 60*1000)

  def recomPurchaseJoin(purchases: StreamBag[Purchase], recoms: StreamBag[Recommendation], closeness: Int)
  : StreamBag[(Purchase, Recommendation)] =
    for {
      p <- purchases
      a <- recoms
      if a.userId == p.userId
      if p.time - a.time <= closeness
      if p.time >= a.time
    } yield {
      (p, a)
    }

}
