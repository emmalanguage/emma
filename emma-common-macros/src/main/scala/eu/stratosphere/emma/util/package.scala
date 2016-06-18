package eu.stratosphere
package emma

package object util {

  /** Boolean logic with predicates. */
  implicit class Predicate[A](val self: A => Boolean) extends AnyVal {

    /** Negates this predicate. */
    def unary_! : A => Boolean =
      self.andThen(!_)

    /** Combines this predicate with `that` via conjunction. */
    def &&[B >: A](that: B => Boolean): A => Boolean =
      x => self(x) && that(x)

    /** Combines this predicate with `that` via disjunction. */
    def ||[B >: A](that: B => Boolean): A => Boolean =
      x => self(x) || that(x)
  }
}
