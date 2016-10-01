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
package eu.stratosphere.emma.macros

/**
 * Implements various utility functions that mitigate and/or workaround deficiencies in Scala's
 * macros APIs, e.g. non- idempotent type checking, lack of hygiene, capture-avoiding substitution,
 * fully-qualified names, fresh name generation, identifying closures, etc.
 */
@deprecated("Use `emma.compiler.MacroUtil` instead", "24.02.2016")
trait BlackBoxUtil extends BlackBox with ReflectUtil {
  import universe._
  import c.internal._
  import syntax._

  def parse(string: String) =
    c.parse(string)

  def typeCheck(tree: Tree) =
    if (tree.isType) c.typecheck(tree, c.TYPEmode)
    else c.typecheck(tree)

  def termSym(owner: Symbol, name: TermName, tpe: Type, flags: FlagSet, pos: Position) =
    newTermSymbol(owner, name, pos, flags).withType(tpe).asTerm

  def typeSym(owner: Symbol, name: TypeName, flags: FlagSet, pos: Position) =
    newTypeSymbol(owner, name, pos, flags)

  override def transform(tree: Tree)(pf: Tree ~> Tree): Tree =
    new Transformer {
      override def transform(tree: Tree): Tree =
        if (pf.isDefinedAt(tree)) pf(tree)
        else tree match {
          // NOTE:
          // - TypeTree.original is not transformed by default
          // - setOriginal is only available at compile-time
          case tt: TypeTree if tt.original != null =>
            val original = transform(tt.original)
            if (original eq tt.original) tt else {
              val tpt = if (tt.hasType) TypeTree(tt.preciseType) else TypeTree()
              setOriginal(tpt, transform(tt.original))
            }

          case _ =>
            super.transform(tree)
        }
    }.transform(tree)
}
