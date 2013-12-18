package eu.stratosphere.emma.mc

/**
 * An abstract base for all 1 + (E×TE) → TE algebras.
 *
 * @tparam E The element type of this algebra.
 * @tparam TE The Scala type realizing the algebra.
 */
abstract class Algebra[E, TE] {

  def empty: TE

  def cons(x: E, xs: TE): TE
}

/**
 * An algebra of lists.
 *
 * @tparam E The list element type.
 */
class AlgList[E] extends Algebra[E, List[E]] {

  override def empty = List[E]()

  override def cons(x: E, xs: List[E]) = x :: xs
}

/**
 * An algebra of bag.
 *
 * @tparam E The bag element type.
 */
class AlgBag[E] extends Algebra[E, List[E]] {

  override def empty = List[E]()

  override def cons(x: E, xs: List[E]) = x :: xs
}

/**
 * An algebra of sets.
 *
 * @tparam E The set element type.
 */
class AlgSet[E] extends Algebra[E, Set[E]] {

  override def empty = Set[E]()

  override def cons(x: E, xs: Set[E]) = xs + x
}

/**
 * An algebra of universally qualified collections.
 */
class AlgAll extends Algebra[Boolean, Boolean] {

  override def empty = true

  override def cons(x: Boolean, xs: Boolean) = x && xs
}

/**
 * An algebra of existentially qualified collections.
 */
class AlgExists extends Algebra[Boolean, Boolean] {

  override def empty = false

  override def cons(x: Boolean, xs: Boolean) = x || xs
}

/**
 * An algebra of existentially qualified collections.
 */
class AlgCount[E] extends Algebra[E, Int] {

  override def empty = 1

  override def cons(x: E, xs: Int) = 1 + xs
}

/**
 * An algebra of list or bag counts.
 */
class AlgSum[E <: Numeric[E]](implicit n: Numeric[E]) extends Algebra[E, E] {

  override def empty = n.zero

  override def cons(x: E, xs: E) = n.plus(x, xs)
}

/**
 * An algebra of list or bag products.
 */
class AlgProd[E <: Numeric[E]](implicit n: Numeric[E]) extends Algebra[E, E] {

  override def empty = n.one

  override def cons(x: E, xs: E) = n.times(x, xs)
}

/**
 * An algebra of collection minimums.
 */
class AlgMin[E <: Ordered[E]](implicit o: Ordered[E]) extends Algebra[E, Option[E]] {

  override def empty = None

  override def cons(x: E, xs: Option[E]) = xs match {
    case None => Some(x)
    case Some(y) => if (x < y) Some(x) else xs
  }
}

/**
 * An algebra of collection maximums.
 */
class AlgMax[E <: Ordered[E]](implicit o: Ordered[E]) extends Algebra[E, Option[E]] {

  override def empty = None

  override def cons(x: E, xs: Option[E]) = xs match {
    case None => Some(x)
    case Some(y) => if (x > y) Some(x) else xs
  }
}