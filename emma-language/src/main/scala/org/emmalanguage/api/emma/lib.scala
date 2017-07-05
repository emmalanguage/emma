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
package api.emma

import compiler.MacroCompiler

import scala.annotation.{StaticAnnotation, compileTimeOnly}
import scala.language.experimental.macros
import scala.reflect.macros.whitebox

@compileTimeOnly("Enable macro paradise to expand macro annotations.")
class lib extends StaticAnnotation {

  def macroTransform(annottees: Any*): Any = macro libMacro.inlineImpl

}

private class libMacro(val c: whitebox.Context) extends MacroCompiler {

  import c.universe._

  /** The implementation of the @emma.lib macro.
   *
   * The annottee is an object, and all method definitions (`DefDef` nodes) on that object
   * are converted to Emma library functions.
   *
   * @param annottees a list with (currently) exactly one element
   * @return the resulting tree with the replacement
   */
  def inlineImpl(annottees: c.Expr[Any]*): c.Expr[Any] = {
    ensure(annottees.size == 1, "Only an object can be annotated with @emma.lib.")

    val annottee = annottees.head
    c.Expr(transformModule(annottee.tree))
  }

  lazy val transformModule: Tree => Tree = {
    case tree@ModuleDef(mods, name, Template(parents, self, body)) =>
      val res = ModuleDef(mods, name, Template(parents, self, body flatMap transformDefDef))
      //c.warning(tree.pos, c.universe.showCode(res))
      res
    case tree: Tree =>
      // not a module: issue a warning and return
      c.warning(tree.pos, "Unexpected non-module annottee found for `@emma.lib` annotation.")
      tree
  }

  /**
   * Transform a DefDef tree.
   *
   * This consists of the following tasks if the passed argument is a `DefDef` node.
   *
   * 1. Remove annotations associated with the `DefDef` node.
   * 2. Attach the serialized-as-string source of the `DefDef` in a matching value member.
   * 3. Annotate the `DefDef` with the name of the value member from (2).
   *
   * For example
   *
   * {{{
   * def add(x: Int, y: Int): Int = x + y
   * }}}
   *
   * will become
   *
   * {{{
   * val add$Q$m1: String = emma.quote {
   *   def add(x: Int, y: Int) = x + y
   * }
   *
   * @emma.src("add$Q$m1")
   * def add(x: Int, y: Int): Int = x + y
   * }}}
   */
  lazy val transformDefDef: Tree => List[Tree] = {
    case DefDef(mods, name, tparams, vparamss, tpt, rhs)
      if name != api.TermName.init =>
      // clear all existing annotations from the DefDef
      val clrDefDef = DefDef(mods mapAnnotations (_ => List.empty[Tree]), name, tparams, vparamss, tpt, rhs)
      // create a fresh name for the associated `emma.quote { <defdef code> }` ValDef
      val nameQ = api.TermName.fresh(s"${name.encodedName}$$Q")
      // quote the method definition in a `val $nameQx = emma.quote { <defdef code> }
      val quoteDef = q"val $nameQ = ${API.emma.sym}.quote { $clrDefDef }"
      // annotate the DefDef with the name of its associated source
      val annDefDef = DefDef(mods mapAnnotations append(src(nameQ.toString)), name, tparams, vparamss, tpt, rhs)
      // emit `val $nameQx = emma.quote { <defdef code> }; @emma.src("$nameQx") def $name = ...`
      List(quoteDef, annDefDef)
    case tree =>
      List(tree)
  }

  val srcSym = rootMirror.staticClass("org.emmalanguage.api.emma.src")

  /** Construct the AST for `@emma.src("$v")` annotation. */
  def src(v: String): Tree =
    api.Inst(srcSym.toTypeConstructor, argss = Seq(Seq(api.Lit(v))))

  /** Show the code for the given Tree and wrap it as a string literal. */
  def codeLiteralFor(tree: Tree): Literal =
    api.Lit(c.universe.showCode(tree))

  /** Append an annotation `ann` to an `annotations` list. */
  def append(ann: Tree)(annotations: List[Tree]): List[Tree] =
    annotations :+ ann

  /** Ensure a condition applies or exit with the given error message. */
  def ensure(condition: Boolean, message: => String): Unit =
    if (!condition) c.error(c.enclosingPosition, message)
}

