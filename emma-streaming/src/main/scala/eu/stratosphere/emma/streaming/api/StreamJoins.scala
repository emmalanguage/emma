package eu.stratosphere.emma.streaming.api

import eu.stratosphere.emma.api.DataBag

object StreamJoins {

  def crossJoin[A, B](xs: StreamBag[A], ys: StreamBag[B]): StreamBag[(A, B)] =
    for {
      x <- xs
      y <- ys
    } yield {
      (x, y)
    }

  def joinOnPredicate[A, B](p: (A, B) => Boolean)(xs: StreamBag[A], ys: StreamBag[B]): StreamBag[(A, B)] =
    for {
      x <- xs
      y <- ys
      if p(x, y)
    } yield {
      (x, y)
    }

  def joinEventsThatAreCloseInTime[A, B](xs: StreamBag[A], ys: StreamBag[B], maxDiffInTime: Int): StreamBag[(A, B)] =
    for {
      x <- xs.withTimestamp
      y <- ys.withTimestamp
      if Math.abs(x.t - y.t) < maxDiffInTime
    } yield {
      (x.v, y.v)
    }

  def joinWithBag[A, B](xs: StreamBag[A], b: DataBag[B]): StreamBag[(A, B)] =
    for {
      x <- xs
      //  without implicit conversion:
      //  y <- StreamBag.fromBag(b)
      y <- b
    } yield {
      (x, y)
    }

  def main(args: Array[String]): Unit = {
    val xs: StreamBag[Int] = Stream.naturals
    val ys: StreamBag[Char] = Seq(
      DataBag(Seq('a', 'b', 'b')),
      DataBag(Seq('c', 'd')),
      DataBag(),
      DataBag(),
      DataBag(Seq('d', 'a', 'e')))
    val zs: StreamBag[Int] = Seq(DataBag(Seq(2)), DataBag(Seq(3, 1)))
    val b: DataBag[String] = DataBag(Seq("anna", "emma"))

    println(crossJoin(xs, ys))
    println(joinOnPredicate((x: Int, z: Int) => x == z)(xs, zs))

    println(StreamBag.fromBag(b))
    println(joinWithBag(xs, b))
    println(ys.groupBy[Char]((x: Char) => x))
    println(ys.distinct())
    println(StreamBag.flatten(ys.groupBy[Char]((x: Char) => x).map(_._2)))
  }
}
