package eu.stratosphere.emma.streaming.api

import eu.stratosphere.emma.streaming.api.Stream._

sealed class Stream[+A](x: => A, xs: => Stream[A]) {

  def head: A = x

  def tail: Stream[A] = xs

  def scan[B](z: B)(op: (B, A) => B): Stream[B] =
    unfold[B, (B, Stream[A])](
      (z, this), {
        case (prev, str) =>
          val curr = op(prev, str.head)
          (curr, (curr, str.tail))
      })

  // todo omit, or define it with unfold
  //  def withFilter(p: (A) => Boolean): Stream[A] = {
  //    if (p(x)) {
  //      Stream(x, xs.withFilter(p))
  //    } else {
  //      xs.withFilter(p)
  //    }
  //  }

  def flatMap[B](f: A => Stream[B]): Stream[B] = flatten(map(f))

  def map[B](f: A => B): Stream[B] =
    unfold[B, Stream[A]](this, xs => (f(xs.head), xs.tail))

  def flatten[B](xss: => Stream[Stream[B]]): Stream[B] = unfold[B, Stream[Stream[B]]](
    xss,
    xss1 => (xss1.head.head, xss1.map(_.tail).tail)
  )

  def take(n: Int): Seq[A] = n match {
    case 0 => Seq()
    case k => head +: tail.take(k - 1)
  }

  def zip[B](other: => Stream[B]): Stream[(A, B)] = for {
    x <- this
    y <- other
  } yield {
    (x, y)
  }

  def zipWith[B, C](f: (A, B) => C)(other: => Stream[B]) =
    zip(other)
      .map[C] { case (x1, y1) => f(x1, y1) }

  /**
    * Retrieves the element from the stream at a certain point of time.
    *
    * This can be thought of as the most simple way of triggering a streaming
    * computation: once at a given time. It models batch processing.
    *
    * @param t
    * The time of the element wished for.
    * @return
    * The element at the given point of time.
    */
  def getAtTime(t: Int): A = t match {
    case 0 => head
    case _ => tail.getAtTime(t - 1)
  }

  def neighbors(): Stream[(A, A)] = zip(tail)

  def accumulate(): Stream[Seq[A]] = unfold[Seq[A], (Stream[A], Seq[A])](
    (this, Seq()), {
      case (rem, sq) =>
        val sq1 = sq :+ rem.head
        (sq1, (rem.tail, sq1))
    })

  // todo solve nested stream printing
  override def toString: String = take(10).mkString("[", ",", "...]")

}

object Stream {

  def apply[A](x: => A, xs: => Stream[A]): Stream[A] = new Stream[A](x, xs)

  def unapply[A](s: Stream[A]): Option[(A, Stream[A])] = Some(s.head, s.tail)

  // Coalgebraic definition of streams
  def unfold[A, B](seed: B, f: B => (A, B)): Stream[A] = {
    val (head, nextSeed) = f(seed)
    Stream(head, unfold(nextSeed, f))
  }

  def cons[A](x: => A, xs: => Stream[A]): Stream[A] = Stream(x, xs)

  def unit[A](a: => A): Stream[A] = unfold[A, A](a, _ => (a, a))

  val naturals: Stream[Int] = unfold[Int, Int](0, x => (x + 1, x + 1))

  // todo define it based on unfold
  def flattenSeqs[A](xs: => Stream[Seq[A]]): Stream[A] =
    if (xs.head.isEmpty) {
      flattenSeqs(xs.tail)
    } else {
      Stream(xs.head.head, flattenSeqs(Stream(xs.head.tail, xs.tail)))
    }

}

