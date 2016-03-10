package eu.stratosphere.emma.streaming.api

import eu.stratosphere.emma.api.DataBag

import scala.collection.immutable.{Stream => _}

/*
  todo
    tests
    stateful computation reconsideration
    count-based windows
      (delta-windows, finding peeks)
    further examples
 */
sealed class StreamBag[+A](private val sb: Stream[DataBag[A]]) {

  // --------------------------------------------------------
  // Monad definition
  // --------------------------------------------------------

  def withFilter(p: (A) => Boolean): StreamBag[A] = StreamBag(sb.map[DataBag[A]](_.withFilter(p)))

  def flatMap[B](f: A => StreamBag[B]): StreamBag[B] = StreamBag.flatten(map(f))

  def map[B](f: A => B): StreamBag[B] = StreamBag(sb.map(_.map(f)))


  // --------------------------------------------------------
  // Structural recursion
  // --------------------------------------------------------

  def scan[B](foldFunc: (B, A => B, (B, B) => B)): Stream[B] = foldFunc match {
    case (z, s, p) => scan(z)(s, p)
  }

  def scan[B](z: B)(s: A => B, p: (B, B) => B): Stream[B] = scanFold[B, B](z)(p)(z)(s, p)

  def scanFold[B, C](zC: C)(opC: (C, B) => C)(zB: B)(s: A => B, opB: (B, B) => B): Stream[C] =
    sb.accumulate()
      .map(sq => sq.map(b => b.fold(zB)(s, opB)))
      .map(sq => sq.foldLeft(zC)(opC))


  // --------------------------------------------------------
  // Group by
  // --------------------------------------------------------

  //  def distinct(): StreamBag[A] = accumulate().sb.map(_.distinct())
  def distinct(): StreamBag[A] = {
    val h = sb.head.distinct()
    val t = sb.tail.map { case x => x.distinct().minus(h) }
    Stream(h, StreamBag(t).distinct())
  }

  def groupBy[K](k: (A) => K): StreamBag[(K, StreamBag[A])] =
  // this is exactly the same as for Bag
    for (key <- this.map(k).distinct()) yield (key, for (y <- this; if k(y) == key) yield y)


  // --------------------------------------------------------
  // Miscellaneous.
  // --------------------------------------------------------

  // todo express this as Monoid with ScalaZ
  def plus[B >: A](addend: StreamBag[B]): StreamBag[B] =
  // this is Stream comprehension, NOT StreamBag
    for {
      x <- this.sb
      y <- addend.sb
    } yield {
      x plus y
    }

  implicit def withTimestamp: StreamBag[Timed[A]] =
  // this is Stream comprehension, NOT StreamBag
    for {
      b <- sb
      t <- Stream.naturals
    } yield {
      b.map(new Timed(t, _))
    }

  override def toString: String = showFirstN(10)

  def showFirstN(n: Int): String = sb.take(n).mkString("[\n\t", "\n\t", "\n...]")

  // --------------------------------------------------------
  // Further helper methods. These should be only used when necessary.
  // --------------------------------------------------------

  def timesNotEmpty(): Stream[Option[Int]] = sb
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
      this
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
    withTimestamp.flatMap((x: Timed[A]) => StreamBag.unit(x.v).postpone(x.t + t(x.v)))

  /*
     This implementation uses the "scan" structure of Stream and the "fold"
     structure of bags separately. It aggregates to "collection" values, i.e.
     the aggregated value in the fold is a Bag again.

     Using this might not be beneficial.
    */
  def stateful[S, B](init: S)(op: (S, A) => (S, B)): StreamBag[B] = {
    val stateTrans: StreamBag[(S => (S, B))] = map(a => s => op(s, a))
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

object StreamBag {

  // --------------------------------------------------------
  // Wrap/unwrap.
  // --------------------------------------------------------

  implicit def apply[A](sb: Stream[DataBag[A]]): StreamBag[A] = new StreamBag[A](sb)

  implicit def unapply[A](sb: StreamBag[A]): Stream[DataBag[A]] = sb.sb


  // --------------------------------------------------------
  // Monad and monoid operations.
  // --------------------------------------------------------

  def flatten[B](xss: StreamBag[StreamBag[B]]): StreamBag[B] = {
    val ssb: Stream[Stream[DataBag[B]]] =
      xss.sb.map(_.fold(StreamBag.empty[B])(identity, (x, y) => x.plus(y)))
    StreamBag.alternativeStreamFlatten(ssb)
  }

  def unit[A](a: => A): StreamBag[A] = Stream(DataBag.unit(a), Stream.unit(DataBag()))

  def empty[A]: StreamBag[A] = Stream.unit(DataBag())


  // --------------------------------------------------------
  // Conversions.
  // --------------------------------------------------------

  implicit def fromStream[A](xs: Stream[A]): StreamBag[A] = xs.map(DataBag.unit)

  implicit def fromBag[A](b: DataBag[A]): StreamBag[A] = Stream(b, Stream.unit(DataBag()))

  implicit def fromListOfBags[A](xs: Seq[DataBag[A]]): StreamBag[A] = xs.foldRight(empty[A])((b, sb) => Stream(b, sb))

  val naturals: StreamBag[Int] = Stream.naturals


  // --------------------------------------------------------
  // Miscellaneous.
  // --------------------------------------------------------

  def fromStreamWithStrictlyIncTimestamp[A](t: (A) => Int)(s: Stream[A]): StreamBag[A] = s match {
    case Stream(x, xs) => {
      val tail = fromStreamWithStrictlyIncTimestamp[A](t(_) - 1)(xs)
      if (t(x) == 0) {
        Stream(DataBag.unit(x), tail)
      } else {
        tail
      }
    }

  }

  def isEmptyFold[A]: (Boolean, A => Boolean, (Boolean, Boolean) => Boolean) =
    (true, _ => false, (b1, b2) => b1 && b2)

  // todo express Bag as Monoid
  /**
    * Monad join of Stream[M] where M is a commutative monoid. Used to combine
    * this with Bag monad.
    *
    * @param xss
    * Doubly-nested stream to flatten.
    * @tparam A
    * Type of stream.
    * @return
    * Flat stream.
    */
  def alternativeStreamFlatten[A](xss: Stream[Stream[DataBag[A]]]): Stream[DataBag[A]] = {
    def colHead[B](ss: Stream[Stream[B]]) = ss.map(_.head)
    def colTail[B](ss: Stream[Stream[B]]) = ss.map(_.tail)
    def colCons[B](s: => Stream[B], ss: Stream[Stream[B]]): Stream[Stream[B]] = {
      s.zipWith[Stream[B], Stream[B]]((b: B, bs: Stream[B]) => Stream.cons(b, bs))(ss)
    }
    def madd(a: DataBag[A], b: DataBag[A]) = a.plus(b)

    val x: DataBag[A] = xss.head.head
    val xs = xss.head.tail
    val ys = colHead(xss.tail)
    val mss = colTail(xss.tail)
    val mss2 = Stream(
      mss.head.zipWith(madd)(xs),
      mss.tail)
    val mss3 = colCons(
      colHead(mss2).zipWith(madd)(ys),
      colTail(mss2)
    )

    Stream(x, alternativeStreamFlatten(mss3))
  }

}
