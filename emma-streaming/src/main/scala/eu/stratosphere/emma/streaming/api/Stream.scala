package eu.stratosphere.emma.streaming.api

import eu.stratosphere.emma.streaming.api.Stream._

trait Stream[+A] {

  type S

  val s0: S
  val c: S => (A, S)

  def head: A = c(s0)._1

  def tail: Stream[A] = new CStream(c(s0)._2, c)

  def scan[B](z: B)(op: (B, A) => B): Stream[B] =
    unfold[B, (B, Stream[A])](
      (z, this), {
        case (prev, str) =>
          val curr = op(prev, str.head)
          (curr, (curr, str.tail))
      })

  def flatMap[B](f: A => Stream[B]): Stream[B] = flatten(map(f))

  def map[B](f: A => B): Stream[B] =
    unfold[B, Stream[A]](this, xs => (f(xs.head), xs.tail))

  def flatten[B](xss0: => Stream[Stream[B]]): Stream[B] = unfold[B, Stream[Stream[B]]](
    xss0,
    xss => (xss.head.head, xss.map(_.tail).tail)
  )

  def take(n: Int): Seq[A] = n match {
    case 0 => Seq()
    case k => head +: tail.take(k - 1)
  }

  def zip[B](other: Stream[B]): Stream[(A, B)] = for {
    x <- this
    y <- other
  } yield (x, y)


  def zipWith[B, C](f: (A, B) => C)(other: Stream[B]): Stream[C] = for {
    x <- this
    y <- other
  } yield f(x, y)

  override def toString: String = take(10).mkString("[", ",", "...]")
}

case class CStream[X, +A](seed: X, f: X => (A, X)) extends Stream[A] {

  override type S = X

  override val s0: S = seed
  override val c: S => (A, S) = f

}

object Stream {

  def apply[X, A](s0: X, c: X => (A, X)): Stream[A] = CStream.apply(s0, c)

  // Coalgebraic definition of streams
  def unfold[A, B](seed: B, f: B => (A, B)): Stream[A] = new CStream(seed, f)

  def unit[A](a: => A): Stream[A] = unfold[A, A](a, _ => (a, a))

  val naturals: Stream[Int] = unfold[Int, Int](0, x => (x + 1, x + 1))

}
