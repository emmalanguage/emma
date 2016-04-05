package eu.stratosphere.emma.streaming.extended

import eu.stratosphere.emma.api.DataBag
import eu.stratosphere.emma.streaming.api.{Timed, StreamBag, Stream}
import eu.stratosphere.emma.streaming.extended.ExtendedStreamBag._

import scala.collection.immutable.{Stream => _}

/**
  * This object is for StreamBag operations that should be used in the future,
  * but are currently not important and/or hard to compile.
  * These should be eventually moved to StreamBag, or removed.
  */
class ExtendedStreamBag[+A](private val sb: StreamBag[A]) {

  // --------------------------------------------------------
  // Group by
  // --------------------------------------------------------

  //  def distinct(): StreamBag[A] = accumulate().sb.map(_.distinct())
  // todo def by unfold
  def distinct(): StreamBag[A] = {
    val h = sb.head.distinct()
    val t = sb.tail.map { case x => x.distinct().minus(h) }
    Stream(h, new ExtendedStreamBag[A](StreamBag(t)).distinct())
  }

  def groupBy[K](k: (A) => K): StreamBag[(K, StreamBag[A])] =
  // this is exactly the same as for Bag
    for (key <- new ExtendedStreamBag[K](sb.map(k)).distinct()) yield (key, for (y <- sb; if k(y) == key) yield y)

  // --------------------------------------------------------
  // Miscellaneous.
  // --------------------------------------------------------

  implicit def withTimestamp: StreamBag[Timed[A]] =
  // this is Stream comprehension, NOT StreamBag
    for {
      b <- sb.sb
      t <- Stream.naturals
    } yield {
      b.map(new Timed(t, _))
    }

  override def toString: String = showFirstN(10)

  def showFirstN(n: Int): String = sb.sb.take(n)
    .map((b: DataBag[A]) => b.fetch().mkString("{", ",", "}"))
    .mkString("[\n\t", "\n\t", "\n...]")

  // --------------------------------------------------------
  // Further helper methods. These should be only used when necessary.
  // --------------------------------------------------------

  def timesNotEmpty(): Stream[Option[Int]] = sb.sb
    .map(_.isEmpty)
    .zip(Stream.naturals)
    .map { case (bagIsEmpty, t) => if (bagIsEmpty) Option.empty[Int] else Some(t) }

  /**
    * Postpones a StreamBag, i.e. prefixes the stream with empty Bags.
    *
    * @param t
    * Number of Bags to prefix the StreamBag with.
    * @return
    * Postponed stream.
    */
  def postpone(t: Int): StreamBag[A] =
    if (t == 0) {
      sb
    } else {
      Stream(DataBag(), postpone(t - 1))
    }

  /**
    * Postpone expressed with monad comprehension. Placed here, to avoid creating more
    * primitives. The default native implementation is faster.
    *
    * @param t
    * Number of Bags to prefix the StreamBag with.
    * @return
    * Postponed stream.
    */
  private def postponeWithComprehension(t: Int): StreamBag[A] =
    for {
      x <- withTimestamp
      y <- withTimestamp
      if y.t - x.t == t
    } yield x.v


  def mapPostpone(t: (A) => Int): StreamBag[A] =
    sb.withTimestamp.flatMap((x: Timed[A]) => StreamBag.unit(x.v).postpone(x.t + t(x.v)))

  /*
     This implementation uses the "scan" structure of Stream and the "fold"
     structure of bags separately. It aggregates to "collection" values, i.e.
     the aggregated value in the fold is a Bag again.

     Using this might not be beneficial.
    */
  def stateful[S, B](init: S)(op: (S, A) => (S, B)): StreamBag[B] = {
    val stateTrans: StreamBag[(S => (S, B))] = sb.map(a => s => op(s, a))
    val zeroStateBag = (s: S) => (s, DataBag())
    val unitStateBag: ((S) => (S, B)) => (S) => (S, DataBag[B]) =
      (st: S => (S, B)) => (s: S) => {
        val (s2, b) = st(s)
        (s2, DataBag(Seq(b)))
      }
    val plusStateBag: ((S) => (S, DataBag[B]), (S) => (S, DataBag[B])) => (S) => (S, DataBag[B]) = {
      case (st1, st2) => (s0: S) =>
        val (s1, b1) = st1(s0)
        val (s2, b2) = st2(s1)
        (s2, b1.plus(b2))
    }

    val stBagTrans: Stream[(S => (S, DataBag[B]))] = stateTrans.sb
      .map(_.fold[S => (S, DataBag[B])](zeroStateBag)(unitStateBag, plusStateBag))

    stBagTrans.scan[(S, DataBag[B])]((init, DataBag())) { case ((s, _), st) => st(s) }
      .map { case (_, b) => b }
  }
}

object ExtendedStreamBag {

  implicit def fromStreamBag[A](sb: StreamBag[A]): ExtendedStreamBag[A] = new ExtendedStreamBag[A](sb)

  implicit def toStreamBag[A](esb: ExtendedStreamBag[A]): StreamBag[A] = esb.sb

}
