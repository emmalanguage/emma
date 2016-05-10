package eu.stratosphere.emma.streaming.examples.webshop

import eu.stratosphere.emma.streaming.api.StreamBag

/*
  Match recommendations with the purchases
  that happened within 1 minute after showing the recommendation.
 */
object RecommendationPurchase {

  case class Purchase(userId: Int, itemId: Int)

  case class Recommendation(recomId: Int, userId: Int)

  def recomPurchaseJoinOneMinute(purchases: StreamBag[Purchase], recom: StreamBag[Recommendation])
  : StreamBag[(Purchase, Recommendation)] =
    recomPurchaseJoin(purchases, recom, 60*1000)

  def recomPurchaseJoin(purchases: StreamBag[Purchase], recoms: StreamBag[Recommendation], closeness: Int)
  : StreamBag[(Purchase, Recommendation)] =
    for {
      p <- purchases.withTimestamp
      a <- recoms.withTimestamp
      if a.v.userId == p.v.userId
      if p.t - a.t < closeness
      if p.t > a.t
    } yield {
      (p.v, a.v)
    }

}
