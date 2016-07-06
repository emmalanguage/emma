package eu.stratosphere
package emma

import shapeless.labelled.FieldType

package object util {

  type =?>[-A, +R] = PartialFunction[A, R]
  type ->>[K, +V] = FieldType[K, V]

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
