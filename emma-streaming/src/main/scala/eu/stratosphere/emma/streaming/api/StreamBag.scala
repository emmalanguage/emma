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
sealed class StreamBag[+A](private[streaming] val sb: Stream[DataBag[A]]) {

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
  // (Commutative) monoid operations
  // --------------------------------------------------------

  def plus[B >: A](addend: StreamBag[B]): StreamBag[B] =
  // this is Stream comprehension, NOT StreamBag
    for {
      x <- sb
      y <- addend.sb
    } yield {
      x plus y
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

  def unit[A](a: => A): StreamBag[A] = Stream(DataBag(Seq(a)), Stream.unit(DataBag()))

  def empty[A]: StreamBag[A] = Stream.unit(DataBag())


  // --------------------------------------------------------
  // Conversions.
  // --------------------------------------------------------

  implicit def fromStream[A](xs: Stream[A]): StreamBag[A] = xs.map((x: A) => DataBag(Seq(x)))

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
        Stream(DataBag(Seq(x)), tail)
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

//  def alternativeStreamFlatten1[A](xss: => Stream[Stream[DataBag[A]]]): Stream[DataBag[A]] = {
//    def next(n: => DataBag[Stream[DataBag[A]]]): (DataBag[A], DataBag[Stream[DataBag[A]]]) = {
//      val head: DataBag[A] = n.flatMap(_.head)
//      val neighbours: DataBag[Stream[DataBag[A]]] = n.map(_.tail)
//      (head, neighbours)
//    }
//
//    val h = xss.head.head
//
//    val neigh1: Stream[DataBag[A]] = xss.map(_.head).tail
//    val neigh2: Stream[DataBag[A]] = xss.head.tail
//    lazy val neigh3: Stream[DataBag[A]] = alternativeStreamFlatten1(xss.tail.tail)
//
//    lazy val neighbours = DataBag(Seq(neigh1, neigh2, neigh3))
//
//    Stream(h, Stream.unfold(neighbours, next))
//  }
}
