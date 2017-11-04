/*
 * Copyright © 2014 TU Berlin (emma@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.emmalanguage
package api.spark

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions => fun}

import scala.language.implicitConversions

object SparkExp {

  //@formatter:off
  sealed trait Expr {
    implicit val spark: SparkSession
    lazy val col: Column = eval(this)
  }
  // structural expressions
  case class Root private(df: DataFrame)(implicit val spark: SparkSession) extends Expr
  case class Proj private(parent: Expr, name: String)(implicit val spark: SparkSession) extends Expr
  case class Struct private(names: Seq[String], vals: Seq[Expr])(implicit val spark: SparkSession) extends Expr
  // other expressions
  case class Lit private(x: Any)(implicit val spark: SparkSession) extends Expr
  case class IsNull private(x: Expr)(implicit val spark: SparkSession) extends Expr
  case class IsNotNull private(x: Expr)(implicit val spark: SparkSession) extends Expr
  case class Eq private(x: Expr, y: Expr)(implicit val spark: SparkSession) extends Expr
  case class Ne private(x: Expr, y: Expr)(implicit val spark: SparkSession) extends Expr
  case class Gt private(x: Expr, y: Expr)(implicit val spark: SparkSession) extends Expr
  case class Lt private(x: Expr, y: Expr)(implicit val spark: SparkSession) extends Expr
  case class Geq private(x: Expr, y: Expr)(implicit val spark: SparkSession) extends Expr
  case class Leq private(x: Expr, y: Expr)(implicit val spark: SparkSession) extends Expr
  case class Not private(x: Expr)(implicit val spark: SparkSession) extends Expr
  case class Or private(x: Expr, y: Expr)(implicit val spark: SparkSession) extends Expr
  case class And private(x: Expr, y: Expr)(implicit val spark: SparkSession) extends Expr
  case class Plus private(x: Expr, y: Expr)(implicit val spark: SparkSession) extends Expr
  case class Minus private(x: Expr, y: Expr)(implicit val spark: SparkSession) extends Expr
  case class Multiply private(x: Expr, y: Expr)(implicit val spark: SparkSession) extends Expr
  case class Divide private(x: Expr, y: Expr)(implicit val spark: SparkSession) extends Expr
  case class Mod private(x: Expr, y: Expr)(implicit val spark: SparkSession) extends Expr
  case class StartsWith private(x: Expr, y: Expr)(implicit val spark: SparkSession) extends Expr
  case class Contains private(x: Expr, y: Expr)(implicit val spark: SparkSession) extends Expr
  //@formatter:on

  object Chain {
    def unapply(path: Expr): Option[(DataFrame, Seq[String])] = path match {
      case Root(df) => Some(df, Seq.empty)
      case Proj(Chain(df, xs), y) => Some(df, xs :+ y)
      case _ => None
    }
  }

  private def eval(expr: Expr)(implicit spark: SparkSession): Column = {
    expr match {
      // structural expressions
      case Chain(df, chain) if chain.nonEmpty =>
        df(chain.mkString("."))
      case Root(df) if df.schema.fields.length == 1 =>
        df.col(df.schema.fields(0).name)
      case Root(df) =>
        fun.struct(df.schema.fields.map(field => df.col(field.name)): _*)
      case Proj(parent, name) =>
        parent.col.getField(name)
      case Struct(names, vals) =>
        fun.struct((for ((v, n) <- vals zip names) yield v.col.as(n)): _*)
      // other expressions
      case Lit(x) =>
        fun.lit(x)
      case IsNull(x) =>
        x.col.isNull
      case IsNotNull(x) =>
        x.col.isNotNull
      case Eq(x, y) =>
        x.col eqNullSafe y.col
      case Ne(x, y) =>
        !(x.col eqNullSafe y.col)
      case Gt(x, y) =>
        x.col gt y.col
      case Lt(x, y) =>
        x.col lt y.col
      case Geq(x, y) =>
        x.col geq y.col
      case Leq(x, y) =>
        x.col leq y.col
      case Not(x) =>
        !x.col
      case Or(x, y) =>
        x.col or y.col
      case And(x, y) =>
        x.col and y.col
      case Plus(x, y) =>
        x.col plus y.col
      case Minus(x, y) =>
        x.col minus y.col
      case Multiply(x, y) =>
        x.col multiply y.col
      case Divide(x, y) =>
        x.col divide y.col
      case Mod(x, y) =>
        x.col mod y.col
      case StartsWith(x, y) =>
        x.col startsWith y.col
      case Contains(x, y) =>
        x.col contains y.col
    }
  }

  implicit private def anyToExpr(x: Any)(implicit spark: SparkSession): Expr =
    x match {
      case x: Expr => x
      case _ => Lit(x)
    }

  def proj(parent: Expr, name: String)(implicit spark: SparkSession): Expr =
    Proj(parent, name)

  def struct(names: String*)(vals: Any*)(implicit spark: SparkSession): Expr =
    Struct(names, vals.map(anyToExpr))

  def isNull(x: Any)(implicit spark: SparkSession): Expr =
    IsNull(x)

  def isNotNull(x: Any)(implicit spark: SparkSession): Expr =
    IsNotNull(x)

  def eq(x: Any, y: Any)(implicit spark: SparkSession): Expr =
    Eq(x, y)

  def ne(x: Any, y: Any)(implicit spark: SparkSession): Expr =
    Ne(x, y)

  def gt(x: Any, y: Any)(implicit spark: SparkSession): Expr =
    Gt(x, y)

  def lt(x: Any, y: Any)(implicit spark: SparkSession): Expr =
    Lt(x, y)

  def geq(x: Any, y: Any)(implicit spark: SparkSession): Expr =
    Geq(x, y)

  def leq(x: Any, y: Any)(implicit spark: SparkSession): Expr =
    Leq(x, y)

  def not(x: Any)(implicit spark: SparkSession): Expr =
    Not(x)

  def or(x: Any, y: Any)(implicit spark: SparkSession): Expr =
    Or(x, y)

  def and(x: Any, y: Any)(implicit spark: SparkSession): Expr =
    And(x, y)

  def plus(x: Any, y: Any)(implicit spark: SparkSession): Expr =
    Plus(x, y)

  def minus(x: Any, y: Any)(implicit spark: SparkSession): Expr =
    Minus(x, y)

  def multiply(x: Any, y: Any)(implicit spark: SparkSession): Expr =
    Multiply(x, y)

  def divide(x: Any, y: Any)(implicit spark: SparkSession): Expr =
    Divide(x, y)

  def mod(x: Any, y: Any)(implicit spark: SparkSession): Expr =
    Mod(x, y)

  def startsWith(x: Any, y: Any)(implicit spark: SparkSession): Expr =
    StartsWith(x, y)

  def contains(x: Any, y: Any)(implicit spark: SparkSession): Expr =
    Contains(x, y)
}
