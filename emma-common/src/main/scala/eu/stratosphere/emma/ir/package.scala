package eu.stratosphere.emma

import eu.stratosphere.emma.api.{DataBag, InputFormat, OutputFormat}

/**
 * Nodes for building an intermediate representation of an Emma programs.
 */
package object ir {

  import scala.language.existentials
  import scala.reflect.runtime.universe._

  // ---------------------------------------------------
  // Thunks.
  // ---------------------------------------------------

  trait Env {
    def get[A](name: String): A

    //    def put[A](value: DataSet[A]): A
    //
    //    def execute
  }

  final case class Temp[A](name: String) {
  }

  // ---------------------------------------------------
  // Combinators.
  // ---------------------------------------------------

  sealed trait Combinator {
  }

  final case class Read[+A](location: String, format: InputFormat[_ <: A])(implicit val tag: TypeTag[_ <: A]) extends Combinator {
  }

  final case class Write[+A](location: String, format: OutputFormat[_ <: A], xs: Combinator)(implicit val tag: TypeTag[_ <: A]) extends Combinator {
  }

  final case class Map[+A](f: Expr[Any], xs: Combinator)(implicit val tag: TypeTag[_ <: A]) extends Combinator {
  }

  final case class FlatMap[+A](f: Expr[Any], xs: Combinator)(implicit val tag: TypeTag[_ <: A]) extends Combinator {
  }

  final case class Filter[+A](p: Expr[Any], xs: Combinator)(implicit val tag: TypeTag[_ <: A]) extends Combinator {
  }

  final case class EquiJoin[+A](p: Expr[Any], xs: Combinator, ys: Combinator)(implicit val tag: TypeTag[_ <: A]) extends Combinator {
  }

  final case class Cross[+A](xs: Combinator, ys: Combinator)(implicit val tag: TypeTag[_ <: A]) extends Combinator {
  }

  final case class Group[+A](key: Expr[Any], xs: Combinator)(implicit val tag: TypeTag[_ <: A]) extends Combinator {
  }

  final case class Fold[+A](empty: Expr[Any], sng: Expr[Any], union: Expr[Any], xs: Combinator)(implicit val tag: TypeTag[_ <: A]) extends Combinator {
  }

  final case class FoldGroup[+A](key: Expr[Any], empty: Expr[Any], sng: Expr[Any], union: Expr[Any], xs: Combinator)(implicit val tag: TypeTag[_ <: A]) extends Combinator {
  }

  final case class Distinct[+A](xs: Combinator)(implicit val tag: TypeTag[_ <: A]) extends Combinator {
  }

  final case class Union[+A](xs: Combinator, ys: Combinator)(implicit val tag: TypeTag[_ <: A]) extends Combinator {
  }

  final case class Diff[+A](xs: Combinator, ys: Combinator)(implicit val tag: TypeTag[_ <: A]) extends Combinator {
  }

  final case class TempSource[+A](temp: Temp[_ <: DataBag[A]])(implicit val tag: TypeTag[_ <: A]) extends Combinator {
  }

  final case class TempSink[+A](xs: Combinator)(implicit val tag: TypeTag[_ <: A]) extends Combinator {
  }

}
