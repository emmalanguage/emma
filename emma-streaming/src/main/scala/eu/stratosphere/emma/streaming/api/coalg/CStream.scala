package eu.stratosphere.emma.streaming.api.coalg

trait Stream[+A] {

  type S

  val s0: S
  val c: S => (A, S)

  def head: A = c(s0)._1
  def tail: Stream[A] = new CStream(s0, c)

}

case class CStream[X, +A](seed: X, f: X => (A,X)) extends Stream[A] {

  override type S = X

  override val s0: S = seed
  override val c: S => (A, S) = f

}

object Stream {

  // Coalgebraic definition of streams
  def unfold[A, B](seed: B, f: B => (A, B)): Stream[A] = new CStream(seed, f)

  def unit[A](a: => A): Stream[A] = unfold[A, A](a, _ => (a, a))

  val naturals: Stream[Int] = unfold[Int, Int](0, x => (x + 1, x + 1))

  // todo define it based on unfold
//  def flattenSeqs[A](xs: => Stream[Seq[A]]): Stream[A] =
//    if (xs.head.isEmpty) {
//      flattenSeqs(xs.tail)
//    } else {
//      Stream(xs.head.head, flattenSeqs(Stream(xs.head.tail, xs.tail)))
//    }
}
