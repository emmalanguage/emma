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

import scala.language.implicitConversions
import scala.reflect.runtime
import scala.tools.reflect.ToolBox

/**
 * Implements various utility functions that mitigate and/or workaround deficiencies in Scala's
 * macros APIs, e.g. non- idempotent type checking, lack of hygiene, capture-avoiding substitution,
 * fully-qualified names, fresh name generation, identifying closures, etc.
 */
@deprecated("Use `emma.compiler.RuntimeUtil` instead", "24.02.2016")
trait RuntimeUtil extends ReflectUtil {
  val universe: runtime.universe.type = runtime.universe
  val tb: ToolBox[universe.type]
  
  import universe._
  import internal._
  import syntax._

  def parse(string: String) =
    tb.parse(string)

  def typeCheck(tree: Tree) =
    if (tree.isType) tb.typecheck(tree, tb.TYPEmode)
    else tb.typecheck(tree)

  def termSym(owner: Symbol, name: TermName, tpe: Type, flags: FlagSet, pos: Position) =
    newTermSymbol(owner, name, pos, flags).withType(tpe).asTerm

  def typeSym(owner: Symbol, name: TypeName, flags: FlagSet, pos: Position) =
    newTypeSymbol(owner, name, pos, flags)
}
