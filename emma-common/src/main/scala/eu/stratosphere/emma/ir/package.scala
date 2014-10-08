package eu.stratosphere.emma

import java.util.UUID

import eu.stratosphere.emma.api.{DataBag, InputFormat, OutputFormat}

/**
 * Nodes for building an intermediate representation of an Emma programs.
 */
package object ir {

  import scala.language.implicitConversions
  import scala.language.existentials
  import scala.reflect.runtime.universe._

  // ---------------------------------------------------
  // Thunks.
  // ---------------------------------------------------

  trait ValueRef[A] {

    val name: String

    def uuid: UUID = UUID.nameUUIDFromBytes(name.getBytes)

    def value: A
  }

  implicit def ValueRefToValue[A](ref: ValueRef[A]) = ref.value

  // ---------------------------------------------------
  // Combinators.
  // ---------------------------------------------------

  sealed trait Combinator[+A] {
    implicit val tag: TypeTag[_ <: A]
  }

  final case class Read[+A](location: String, format: InputFormat[_ <: A])(implicit val tag: TypeTag[_ <: A]) extends Combinator[A] {
  }

  final case class Write[+A](location: String, format: OutputFormat[_ <: A], xs: Combinator[_ <: A])(implicit val tag: TypeTag[Unit]) extends Combinator[Unit] {
  }

  final case class TempSource[+A](ref: ValueRef[_ <: DataBag[A]])(implicit val tag: TypeTag[_ <: A]) extends Combinator[A] {
  }

  final case class TempSink[+A](name: String, xs: Combinator[_ <: A])(implicit val tag: TypeTag[_ <: A]) extends Combinator[A] {
  }

  final case class FoldSink[+A](name: String, xs: Fold[_ <: A, Any])(implicit val tag: TypeTag[_ <: A]) extends Combinator[A] {
  }

  final case class Map[+A, +B](f: Expr[Any], xs: Combinator[_ <: B])(implicit val tag: TypeTag[_ <: A]) extends Combinator[A] {
  }

  final case class FlatMap[+A, +B](f: Expr[Any], xs: Combinator[_ <: B])(implicit val tag: TypeTag[_ <: A]) extends Combinator[A] {
  }

  final case class Filter[+A](p: Expr[Any], xs: Combinator[_ <: A])(implicit val tag: TypeTag[_ <: A]) extends Combinator[A] {
  }

  final case class EquiJoin[+A, +B, +C](p: Expr[Any], xs: Combinator[_ <: B], ys: Combinator[_ <: C])(implicit val tag: TypeTag[_ <: A]) extends Combinator[A] {
  }

  final case class Cross[+A, +B, +C](xs: Combinator[_ <: B], ys: Combinator[_ <: C])(implicit val tag: TypeTag[_ <: A]) extends Combinator[A] {
  }

  final case class Group[+A, +B](key: Expr[Any], xs: Combinator[_ <: B])(implicit val tag: TypeTag[_ <: A]) extends Combinator[A] {
  }

  final case class Fold[+A, +B](empty: Expr[Any], sng: Expr[Any], union: Expr[Any], xs: Combinator[_ <: B])(implicit val tag: TypeTag[_ <: A]) extends Combinator[A] {
  }

  final case class FoldGroup[+A, +B](key: Expr[Any], empty: Expr[Any], sng: Expr[Any], union: Expr[Any], xs: Combinator[_ <: B])(implicit val tag: TypeTag[_ <: A]) extends Combinator[A] {
  }

  final case class Distinct[+A](xs: Combinator[A])(implicit val tag: TypeTag[_ <: A]) extends Combinator[A] {
  }

  final case class Union[+A, +B, +C](xs: Combinator[_ <: B], ys: Combinator[_ <: C])(implicit val tag: TypeTag[_ <: A]) extends Combinator[A] {
  }

  final case class Diff[+A, +B, +C](xs: Combinator[_ <: B], ys: Combinator[_ <: C])(implicit val tag: TypeTag[_ <: A]) extends Combinator[A] {
  }

}
