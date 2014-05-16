package eu.stratosphere.emma.mc

package object monad {

  abstract class Monad[+T](val name: String)(implicit m: scala.reflect.Manifest[T]) {
    def refines(that: Monad[Any]) = that match {
      case _: LeftIdempotentMonad[_] =>
        false
      case _: AssociativeMonad[_] =>
        false
      case _ =>
        true
    }
  }

  private[monad] abstract class AssociativeMonad[+T](override val name: String)(implicit m: scala.reflect.Manifest[T]) extends Monad[T](name)(m) {
    override def refines(that: Monad[Any]) = that match {
      case _: LeftIdempotentMonad[_] =>
        false
      case _: AssociativeMonad[_] =>
        true
      case _ =>
        true
    }
  }

  private[monad] abstract class LeftIdempotentMonad[+T](override val name: String)(implicit m: scala.reflect.Manifest[T]) extends Monad[T](name)(m) {
    override def refines(that: Monad[Any]) = that match {
      case _: LeftIdempotentMonad[_] =>
        true
      case _: AssociativeMonad[_] =>
        true
      case _ =>
        true
    }
  }

  case class List[T]()(implicit m: scala.reflect.Manifest[T]) extends Monad[T]("list")(m)

  case class Bag[T]()(implicit m: scala.reflect.Manifest[T]) extends AssociativeMonad[T]("bag")(m)

  case class Set[T <: Numeric[T]]()(implicit m: scala.reflect.Manifest[T], n: Numeric[T]) extends LeftIdempotentMonad[T]("set")(m)

  case class Sum[T <: Numeric[T]]()(implicit m: scala.reflect.Manifest[T], n: Numeric[T]) extends AssociativeMonad[T]("sum")(m)

  case class Prod[T <: Numeric[T]]()(implicit m: scala.reflect.Manifest[T], n: Numeric[T]) extends AssociativeMonad[T]("prod")(m)

  case class Max[T <: Numeric[T]]()(implicit m: scala.reflect.Manifest[T], n: Numeric[T]) extends LeftIdempotentMonad[T]("max")(m)

  case class Min[T <: Numeric[T]]()(implicit m: scala.reflect.Manifest[T], n: Numeric[T]) extends LeftIdempotentMonad[T]("min")(m)

  object Exists extends LeftIdempotentMonad[Boolean]("exists")(manifest[Boolean])

  object All extends LeftIdempotentMonad[Boolean]("all")(manifest[Boolean])

}
