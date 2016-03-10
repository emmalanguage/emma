package eu.stratosphere.emma.streaming.api

import eu.stratosphere.emma.api.DataBag
import org.scalatest._

class StreamDataBagTest extends FlatSpec with Matchers {
  "Stateful computation" should "can count records" in {
    val xs: StreamBag[(Int, Int)] = Seq(
      DataBag(),
      DataBag(Seq((1,3),(1,3),(1,3))),
      DataBag(Seq((4,5), (4,5))),
      DataBag(),
      DataBag(),
      DataBag(Seq((6,8), (6,8))))

    val cnts = xs.stateful[Int, ((Int, Int), Int)] (0) { case (cnt, i) => (cnt + 1, (i, cnt + 1)) }

    val cntIsInInterval =
      cnts.map { case (interval, cnt) => interval._1 <= cnt && cnt <= interval._2 }

    cntIsInInterval.scan[Boolean](true)(identity, (b1,b2) => b1 && b2)
      .getAtTime(5) should be (true)
  }

  "Stateful computation" should "concat strings" in {

    val xs: StreamBag[Char] = Seq(
      DataBag(Seq('a', 'a', 'a')),
      DataBag(Seq('b', 'b')),
      DataBag(),
      DataBag(),
      DataBag(Seq('c', 'c', 'c')))

    val strs = xs.stateful[String, String]("") { case (str, c) => (str + c, str + c)}

    strs.getAtTime(4).fetch() should contain ("aaabbccc")
  }
}

