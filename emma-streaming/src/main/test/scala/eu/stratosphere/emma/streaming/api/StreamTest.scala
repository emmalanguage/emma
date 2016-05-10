package eu.stratosphere.emma.streaming.api

import eu.stratosphere.emma.api.DataBag
import org.scalatest._

class StreamTest extends FlatSpec with Matchers {

  "Take " should "return the first n elements" in {
    val sq = Seq(
      DataBag(Seq(1,2))
      , DataBag(Seq(3,4))
      , DataBag()
      , DataBag(Seq(5,6))
      , DataBag()
      , DataBag(Seq(7))
      , DataBag()
    )

    val xs = StreamBag.fromListOfBags(sq)

    xs.take(5) should be (sq.take(5))

  }

}
