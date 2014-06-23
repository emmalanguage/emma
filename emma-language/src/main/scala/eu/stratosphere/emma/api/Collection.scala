package eu.stratosphere.emma.api

trait Collection[+A] {

  // structural recursion
  def fold[B](n: B, s: A => B, p: (B, B) => B): B

  // structural recursion (shorthand)
  def reduce[B](s: A => B, p: (B, B) => B): Option[B] = fold[Option[B]](None,
    (v: A) => Some(s(v)),
    (ox: Option[B], oy: Option[B]) => for (x <- ox; y <- oy) yield p(x, y))

  // structural recursion (shorthand)
  def reduce[B >: A](p: (B, B) => B): Option[B] = fold[Option[B]](None,
    (v: A) => Some(v),
    (ox: Option[B], oy: Option[B]) => for (x <- ox; y <- oy) yield p(x, y))
}
