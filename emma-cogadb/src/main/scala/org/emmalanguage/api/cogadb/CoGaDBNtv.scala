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
package api.cogadb

import api._
import compiler.lang.cogadb.ast
import runtime.CoGaDB

object CoGaDBNtv {
  import Meta.Projections._



  //----------------------------------------------------------------------------
  // Specialized combinators
  //----------------------------------------------------------------------------

  def select[A: Meta](p: String => Seq[ast.AttrRef])(xs: DataBag[A])(
    implicit cogadb: CoGaDB
  ): DataBag[A] = xs match {
    case table(us) => table(us)
  }

  def project[A: Meta, B: Meta](f: String => Seq[ast.AttrRef])(xs: DataBag[A])(
    implicit cogadb: CoGaDB
  ): DataBag[B] = xs match {
    //TODO: the correct table and projected fields must be chosen
    case table(us) => table(ast.Projection(f(resolveTable(us)), us))
  }

  def equiJoin[A: Meta, B: Meta, K: Meta](
    kx: String => Seq[ast.AttrRef], ky: String => Seq[ast.AttrRef])(xs: DataBag[A], ys: DataBag[B]
  )(implicit s: CoGaDB): DataBag[(A, B)] = (xs, ys) match {
    case (table(us), table(vs)) =>
      table(ast.Join("INNER_JOIN", zeq(kx(resolveTable(us)), ky(resolveTable(vs))), us, vs))
  }

  //----------------------------------------------------------------------------
  // Helper Objects and Methods
  //----------------------------------------------------------------------------

  private def resolveTable(node: ast.Op): String =
    node match {
      case ast.TableScan(tablename, version) =>
        tablename
      case _ =>
        //TODO: handle properly
        "DATAFLOW0000"
    }

  private def and(conjs: Seq[ast.AttrRef]): ast.Predicate = ???

  private def zeq(lhs: Seq[ast.AttrRef], rhs: Seq[ast.AttrRef]): Seq[ast.Predicate] =
    for ((l, r) <- lhs zip rhs) yield ast.ColCol(l,r,ast.Equal)

  private object table {
    def apply[A: Meta](
      rep: ast.Op
    )(implicit cogadb: CoGaDB): DataBag[A] = new CoGaDBTable(rep)

    def unapply[A: Meta](
      bag: DataBag[A]
    )(implicit cogadb: CoGaDB): Option[ast.Op] = bag match {
      case bag: CoGaDBTable[A] => Some(bag.rep)
      case _ => None
    }
  }
}
