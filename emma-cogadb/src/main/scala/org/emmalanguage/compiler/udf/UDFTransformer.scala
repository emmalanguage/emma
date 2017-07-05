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
package compiler.udf

import org.emmalanguage.compiler.lang.cogadb.ast._
import org.emmalanguage.compiler.udf.common._

import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox


class UDFTransformer(udfClosure: UDFClosure) extends TypeHelper {

  val tb = runtimeMirror(getClass.getClassLoader).mkToolBox()

  //TODO: replace Op with Node
  def transform: Op = udfClosure match {
    case MapUDFClosure(ast, symTbl, child) => new MapUDFGenerator(ast, symTbl, child).generate
    case FilterUDFClosure(ast, symTbl, child) => transformFilterUDF(ast, symTbl, child)
    case FoldUDFClosure(zAst, sngAst, uniAst, symTbl, child) =>
      new ReduceUDFGenerator(zAst, sngAst, uniAst, symTbl, child).generate
  }

  private def transformFilterUDF(ast: Tree, symTbl: Map[String, String], child: Op): Op = {
    genericSelectionChecker.traverse(ast)
    if (genericSelectionChecker.isTransformable) {
      //generate GENERIC_SELECTION predicate
      new SelectionGenerator(ast, symTbl, child).generate
    } else {
      //rewrite filter predicate to flatMap UDF and apply map udf transformation
      val flatMapUdf = filterToFlatMapUDFTransformer.transform(ast)
      val unTypeCheckedAST = tb.untypecheck(flatMapUdf)
      val typeCheckedAST = tb.typecheck(unTypeCheckedAST)

      new MapUDFGenerator(typeCheckedAST, symTbl, child).generate
    }
  }

  /**
   * implements heuristic to check if a filter UDF can be transformed to GENERIC_SELECTION
   * filter UDF must not be a Block and should not contain unsupported comparator functions
   */
  private object genericSelectionChecker extends Traverser {

    var isTransformable = true
    private val supportedSelectionOperations = Seq(
      TermName("$eq$eq"), //==
      TermName("$bang$eq"), //!=
      TermName("$less"), //<
      TermName("$less$eq"), //<=
      TermName("$greater"), //>
      TermName("$greater$eq"), //>=
      TermName("$amp$amp"), //&&
      TermName("$bar$bar") //||
    )

    override def traverse(tree: Tree) = tree match {
      case b: Block => isTransformable = false
      case Apply(sel: Select, args: List[Tree]) => if (!isSupportedComparator(sel.name)) isTransformable = false
      else super.traverseTrees(sel +: args)
      case t => super.traverse(t)
    }

    private def isSupportedComparator(name: Name): Boolean =
      if (name.isTermName) supportedSelectionOperations contains name.toTermName else false
  }

  private object filterToFlatMapUDFTransformer extends Transformer {
    override def transform(tree: Tree) = tree match {
      case fun@Function(vparams, body) =>
        treeCopy.Function(fun, vparams, rewrite(tree, body, vparams))
      case t => super.transform(t)
    }

    def rewrite(parent: Tree, t: Tree, params: List[ValDef]): Tree = t match {
      case b: Block => transformBlock(b, params.head)
      case app: Apply => transformApply(app, params.head)
      case _ => super.transform(parent)
    }

    def transformBlock(b: Block, udfParam: ValDef): Block = {
      val varName = TermName("filter_udf_condition")
      val filterCond = q" val $varName: Boolean = ${b.expr}"
      val retStmt = If(Ident(varName), Ident(udfParam.name), Ident(TermName("None")))

      treeCopy.Block(b, b.stats :+ filterCond, retStmt)
    }

    def transformApply(app: Apply, udfParam: ValDef): Tree = {
      val varName = TermName("filter_udf_condition")
      val filterCond = q" val $varName: Boolean = ${app}"
      val retStmt = If(Ident(varName), Ident(udfParam.name), Ident(TermName("None")))
      q"{..${filterCond :: retStmt :: List.empty[Tree]}}"
    }

  }

}
