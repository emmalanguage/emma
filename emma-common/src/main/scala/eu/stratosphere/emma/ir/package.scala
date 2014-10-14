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

  final case class Read[+A: TypeTag](location: String, format: InputFormat[_ <: A]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class Write[+A: TypeTag](location: String, format: OutputFormat[_ <: A], xs: Combinator[_ <: A]) extends Combinator[Unit] {
    override val tag: TypeTag[Unit] = typeTag[Unit]
  }

  final case class TempSource[+A: TypeTag](ref: ValueRef[_ <: DataBag[A]]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class TempSink[+A: TypeTag](name: String, xs: Combinator[_ <: A]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class FoldSink[+A: TypeTag](name: String, xs: Fold[_ <: A, Any]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class Map[+A: TypeTag, +B](f: Expr[Any], xs: Combinator[_ <: B]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class FlatMap[+A: TypeTag, +B](f: Expr[Any], xs: Combinator[_ <: B]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class Filter[+A: TypeTag](p: Expr[Any], xs: Combinator[_ <: A]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class EquiJoin[+A: TypeTag, +B, +C](keyx: Expr[Any], keyy: Expr[Any], xs: Combinator[_ <: B], ys: Combinator[_ <: C]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class Cross[+A: TypeTag, +B, +C](xs: Combinator[_ <: B], ys: Combinator[_ <: C]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class Group[+A: TypeTag, +B](key: Expr[Any], xs: Combinator[_ <: B]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class Fold[+A: TypeTag, +B](empty: Expr[Any], sng: Expr[Any], union: Expr[Any], xs: Combinator[_ <: B]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class FoldGroup[+A: TypeTag, +B](key: Expr[Any], empty: Expr[Any], sng: Expr[Any], union: Expr[Any], xs: Combinator[_ <: B]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class Distinct[+A: TypeTag](xs: Combinator[A]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class Union[+A: TypeTag, +B, +C](xs: Combinator[_ <: B], ys: Combinator[_ <: C]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class Diff[+A: TypeTag, +B, +C](xs: Combinator[_ <: B], ys: Combinator[_ <: C]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

}
