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
package ast

/** (method / lambda / class) Parameters. */
trait Parameters { this: AST =>

  /**
   * (method / lambda / class) Parameters.
   *
   * === Examples ===
   * {{{
   *   def method(c: Char) = c
   *   (d: Double) => d
   *   abstract class A(e: Exception) { e.printStackTrace() }
   *   class C(f: Float) { println(f) }
   * }}}
   */
  trait ParameterAPI { this: API =>

    import u.Flag._
    import universe._

    /** (method / lambda / class) Parameter symbols. */
    object ParSym extends Node {

      /**
       * Creates a new parameter symbol.
       * @param owner The method / lambda / class that has this parameter.
       * @param name The name of this parameter (will be encoded).
       * @param tpe The type of this parameter (will be dealiased and widened).
       * @param flags Any additional modifiers (cannot be mutable).
       * @param pos The (optional) source code position where this parameter is defined.
       * @return A new paramter symbol.
       */
      def apply(owner: u.Symbol, name: u.TermName, tpe: u.Type,
        flags: u.FlagSet = u.NoFlags,
        pos: u.Position = u.NoPosition): u.TermSymbol = {

        assert(are.not(MUTABLE)(flags), s"$this `$name` cannot be mutable")
        BindingSym(owner, name, tpe, flags | PARAM, pos)
      }

      def unapply(sym: u.TermSymbol): Option[u.TermSymbol] =
        Option(sym).filter(is.param)
    }

    /** (method / lambda / class) Parameter references. */
    object ParRef extends Node {

      /**
       * Creates a type-checked parameter reference.
       * @param target Must be a parameter symbol.
       * @return `target`.
       */
      def apply(target: u.TermSymbol): u.Ident = {
        assert(is.defined(target), s"$this target `$target` is not defined")
        assert(is.param(target), s"$this target `$target` is not a parameter")
        BindingRef(target)
      }

      def unapply(ref: u.Ident): Option[u.TermSymbol] = ref match {
        case BindingRef(ParSym(target)) => Some(target)
        case _ => None
      }
    }

    /** (method / lambda / class) Parameter definitions. */
    object ParDef extends Node {

      /**
       * Creates a type-checked parameter definition.
       * @param lhs Must be a parameter symbol.
       * @param rhs The default value of this parameter (empty by default).
       * @param flags Any additional modifiers (e.g. `implicit`).
       * @return `(..flags lhs [= rhs])`.
       */
      def apply(lhs: u.TermSymbol,
        rhs: u.Tree = Empty(),
        flags: u.FlagSet = u.NoFlags): u.ValDef = {

        assert(is.defined(lhs), s"$this LHS `$lhs` is not defined")
        assert(is.param(lhs), s"$this LHS `$lhs` is not a parameter")
        assert(are.not(MUTABLE)(flags), s"$this LHS `$lhs` cannot be mutable")
        BindingDef(lhs, rhs, flags)
      }

      def unapply(bind: u.ValDef): Option[(u.TermSymbol, u.Tree, u.FlagSet)] = bind match {
        case BindingDef(ParSym(lhs), rhs, flags) => Some(lhs, rhs, flags)
        case _ => None
      }
    }
  }
}
