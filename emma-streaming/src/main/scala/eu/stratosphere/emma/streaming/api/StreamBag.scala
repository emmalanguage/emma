package eu.stratosphere.emma.streaming.api

import eu.stratosphere.emma.api.DataBag

import scala.collection.immutable.{Stream => _}

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
    sb.scan[C](zC) { case (c, bag) => opC(c, bag.fold(zB)(s, opB)) }

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

  // --------------------------------------------------------
  // Misc
  // --------------------------------------------------------

  def withTimestamp: StreamBag[Timed[A]] =
  // this is Stream comprehension, NOT StreamBag
    for {
      b <- sb
      t <- Stream.naturals
    } yield {
      b.map(new Timed(t, _))
    }

  def distinct() = StreamBag(Stream.unfold[DataBag[A], (Set[A], Stream[DataBag[A]])](
    (Set.empty[A], sb), {
      case (seen, xs) => {
        val h = xs.head.distinct()
        val t = xs.tail
        (h.minus(DataBag(seen.toSeq)), (seen -- h.fetch(), t))
      }
    }
  ))

  def groupBy[K](k: (A) => K): StreamBag[(K, StreamBag[A])] =
  // this is exactly the same as for Bag
    for (key <- this.map(k).distinct()) yield (key, for (y <- this; if k(y) == key) yield y)

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
    StreamBag.monoidStreamFlatten(ssb)
  }

  def unit[A](a: => A): StreamBag[A] = fromBag(DataBag(Seq(a)))

  def empty[A]: StreamBag[A] = Stream.unit(DataBag())


  // --------------------------------------------------------
  // Conversions.
  // --------------------------------------------------------

  implicit def fromStream[A](xs: Stream[A]): StreamBag[A] = StreamBag(xs.map((x: A) => DataBag(Seq(x))))

  implicit def fromBag[A](b: DataBag[A]): StreamBag[A] = StreamBag(Stream.unfold[DataBag[A], Option[DataBag[A]]](
    Some(b), {
      case Some(bag) => (bag, None)
      case _ => (DataBag(), None)
    }))

  implicit def fromListOfBags[A](xs: Seq[DataBag[A]]): StreamBag[A] = Stream.unfold[DataBag[A], Seq[DataBag[A]]](
    xs, sq => {
      if (sq.isEmpty) {
        (DataBag(), sq)
      } else {
        (sq.head, sq.tail)
      }
    })

  val naturals: StreamBag[Int] = Stream.naturals

  // --------------------------------------------------------
  // Miscellaneous.
  // --------------------------------------------------------

  def isEmptyFold[A]: (Boolean, A => Boolean, (Boolean, Boolean) => Boolean) =
    (true, _ => false, (b1, b2) => b1 && b2)

  /**
    * Monad join of Stream[M] where M is a commutative monoid. Used to combine
    * this with Bag monad.
    *
    * @param xss0
    * Doubly-nested stream to flatten.
    * @tparam A
    * Type of stream.
    * @return
    * Flat stream.
    */
  def monoidStreamFlatten[A](xss0: Stream[Stream[DataBag[A]]]): Stream[DataBag[A]] = {

    val curr: Stream[Stream[DataBag[A]]] => DataBag[A] = _.head.head

    def next(xss: Stream[Stream[DataBag[A]]]): Stream[Stream[DataBag[A]]] = {
      val tt: Stream[Stream[DataBag[A]]] = xss.map(_.tail).tail
      val fstRow: Stream[DataBag[A]] = xss.head.tail
      val fstCol: Stream[DataBag[A]] = xss.map(_.head).tail

      val addRow: Stream[Stream[DataBag[A]]] = Stream.unfold[Stream[DataBag[A]], (Boolean, Stream[Stream[DataBag[A]]])](
        (true, tt), {
          case (true, yss) => (yss.head.plus(fstRow), (false, yss.tail))
          case (false, yss) => (yss.head, (false, yss.tail))
        })

      val addCol: Stream[Stream[DataBag[A]]] = fstCol.zipWith[Stream[DataBag[A]], Stream[DataBag[A]]](
        { case (x0, xs0) => Stream.unfold[DataBag[A], (Boolean, Stream[DataBag[A]])](
          (true, xs0), {
            case (true, xs) => (xs.head.plus(x0), (false, xs.tail))
            case (false, xs) => (xs.head, (false, xs.tail))
          })
        })(addRow)

      addCol
    }

    Stream.unfold[DataBag[A], Stream[Stream[DataBag[A]]]](xss0, xss => (curr(xss), next(xss)))
  }
}
