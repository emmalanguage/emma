package eu.stratosphere.emma.streaming.api

sealed class Stream[+A](x: => A, xs: => Stream[A]) {

  def head: A = x

  def tail: Stream[A] = xs

  def withFilter(p: (A) => Boolean): Stream[A] = {
    if (p(x)) {
      Stream(x, xs.withFilter(p))
    } else {
      xs.withFilter(p)
    }
  }

  def flatMap[B](f: A => Stream[B]): Stream[B] = flatten(map(f))

  def map[B](f: A => B): Stream[B] = Stream(f(x), xs.map(f))

  def flatten[B](xss: => Stream[Stream[B]]): Stream[B] = {
    val tt = xss.tail.map { xs: Stream[B] => xs.tail }
    Stream(xss.head.head, flatten(tt))
  }

  def scan[B](z: B)(op: (B, A) => B): Stream[B] = accumulate()
    .map(sq => sq.foldLeft(z)(op))

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

  def accumulate(): Stream[Seq[A]] = Stream(Seq(head), tail.accumulate().map(head +: _))

  // todo solve nested stream printing
  override def toString: String = take(10).mkString("[", ",", "...]")
}

object Stream {

  def apply[A](x: => A, xs: => Stream[A]): Stream[A] = new Stream[A](x, xs)

  def unapply[A](s: Stream[A]): Option[(A, Stream[A])] = Some(s.head, s.tail)

  def cons[A](x: => A, xs: => Stream[A]): Stream[A] = Stream(x, xs)

  def unit[A](a: => A): Stream[A] = Stream(a, unit(a))

  def flattenSeqs[A](xs: => Stream[Seq[A]]): Stream[A] =
    if (xs.head.isEmpty) {
      flattenSeqs(xs.tail)
    } else {
      Stream(xs.head.head, flattenSeqs(Stream(xs.head.tail, xs.tail)))
    }

  val naturals: Stream[Int] = {
    def natsFrom(i: Int): Stream[Int] = Stream(i, natsFrom(i + 1))
    natsFrom(0)
  }

}

