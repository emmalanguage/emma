package eu.stratosphere.emma.ir

package object monad {

  abstract class Monad[+T](val name: String)(implicit m: scala.reflect.Manifest[T]) {
    def refines(that: Monad[Any]) = that match {
      case _: LeftIdempotentMonad[_] =>
        false
      case _: CommutativeMonad[_] =>
        false
      case _ =>
        true
    }
  }

  private[monad] abstract class CommutativeMonad[+T](override val name: String)(implicit m: scala.reflect.Manifest[T]) extends Monad[T](name)(m) {
    override def refines(that: Monad[Any]) = that match {
      case _: LeftIdempotentMonad[_] =>
        false
      case _: CommutativeMonad[_] =>
        true
      case _ =>
        true
    }
  }

  private[monad] abstract class LeftIdempotentMonad[+T](override val name: String)(implicit m: scala.reflect.Manifest[T]) extends Monad[T](name)(m) {
    override def refines(that: Monad[Any]) = that match {
      case _: LeftIdempotentMonad[_] =>
        true
      case _: CommutativeMonad[_] =>
        true
      case _ =>
        true
    }
  }

  case class List[T]()(implicit m: scala.reflect.Manifest[T]) extends Monad[T]("list")(m)

  case class Bag[T]()(implicit m: scala.reflect.Manifest[T]) extends CommutativeMonad[T]("bag")(m)

  case class Set[T]()(implicit m: scala.reflect.Manifest[T]) extends LeftIdempotentMonad[T]("set")(m)

  case class Sum[T <: Numeric[T]]()(implicit m: scala.reflect.Manifest[T], n: Numeric[T]) extends CommutativeMonad[T]("sum")(m)

  case class Prod[T <: Numeric[T]]()(implicit m: scala.reflect.Manifest[T], n: Numeric[T]) extends CommutativeMonad[T]("prod")(m)

  case class Max[T <: Numeric[T]]()(implicit m: scala.reflect.Manifest[T], n: Numeric[T]) extends LeftIdempotentMonad[T]("max")(m)

  case class Min[T <: Numeric[T]]()(implicit m: scala.reflect.Manifest[T], n: Numeric[T]) extends LeftIdempotentMonad[T]("min")(m)

  object Exists extends LeftIdempotentMonad[Boolean]("exists")(manifest[Boolean])

  object All extends LeftIdempotentMonad[Boolean]("all")(manifest[Boolean])

}
