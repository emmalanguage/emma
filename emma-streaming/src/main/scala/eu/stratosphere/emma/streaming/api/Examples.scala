package eu.stratosphere.emma.streaming.api

import eu.stratosphere.emma.api.DataBag

object Examples {

  def main(args: Array[String]): Unit = {

    println(Stream.naturals)

    val x: Int = 42
    val xs: StreamBag[Int] = StreamBag.unit[Int](x)

    println(xs)

    val nats = for {
      x <- StreamBag.naturals
      y <- StreamBag.naturals
    } yield {
      (x, y)
    }

    println(StreamBag.naturals)
    println(nats)

    val b1 = DataBag[Int](Seq(1, 2, 3))
    val b2 = for {
      x <- b1
    } yield {
      x + 1
    }

    println(b2)

    println(Stream.unit(Stream.naturals))

    val sb2: StreamBag[Int] = Seq(DataBag(Seq(1, 2)), DataBag(Seq(3)), DataBag(), DataBag(Seq(4, 5, 6)))
    println(sb2)

    val sb3: StreamBag[Int] = for {
      x <- sb2.withTimestamp
      if x.t > 1
    } yield {
      x.v
    }

    println(sb3)
  }

}
