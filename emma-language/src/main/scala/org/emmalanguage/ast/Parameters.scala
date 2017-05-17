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

/** (method / lambda) Parameters. */
trait Parameters { this: AST =>

  /**
   * (method / lambda) Parameters.
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

    import u._
    import Flag._

    /** (method / lambda) Parameter symbols. */
    object ParSym extends Node {

      /**
       * Creates a type-checked parameter symbol.
       * @param own The method / lambda / class that has this parameter.
       * @param nme The name of this parameter (will be encoded).
       * @param tpe The type of this parameter (will be dealiased and widened).
       * @param flg Any (optional) modifiers (e.g. implicit, private, protected).
       * @param pos The (optional) source code position where this parameter is defined.
       * @param ans Any (optional) annotations associated with this parameter.
       * @return A new type-checked paramter symbol.
       */
      def apply(own: u.Symbol, nme: u.TermName, tpe: u.Type,
        flg: u.FlagSet         = u.NoFlags,
        pos: u.Position        = u.NoPosition,
        ans: Seq[u.Annotation] = Seq.empty
      ): u.TermSymbol = {
        assert(are.not(MUTABLE, flg), s"$this $nme cannot be mutable")
        BindingSym(own, nme, tpe, flg | PARAM, pos)
      }

      def unapply(sym: u.TermSymbol): Option[u.TermSymbol] =
        Option(sym).filter(_.isParameter)
    }

    /** (method / lambda) Parameter references. */
    object ParRef extends Node {

      /**
       * Creates a type-checked parameter reference.
       * @param target Must be a parameter symbol.
       * @return `target`.
       */
      def apply(target: u.TermSymbol): u.Ident = {
        assert(is.defined(target), s"$this target is not defined")
        assert(target.isParameter, s"$this target $target is not a parameter")
        BindingRef(target)
      }

      def unapply(ref: u.Ident): Option[u.TermSymbol] = ref match {
        case BindingRef(ParSym(target)) => Some(target)
        case _ => None
      }
    }

    /** (method / lambda) Parameter definitions. */
    object ParDef extends Node {

      /**
       * Creates a type-checked parameter definition.
       * @param lhs     Must be a parameter symbol.
       * @param default The default value of this parameter (empty by default).
       * @return `(lhs [= rhs])`.
       */
      def apply(lhs: u.TermSymbol, default: u.Tree = Empty()): u.ValDef = {
        assert(is.defined(lhs), s"$this LHS is not defined")
        assert(lhs.isParameter, s"$this LHS $lhs is not a parameter")
        BindingDef(lhs, default)
      }

      def unapply(bind: u.ValDef): Option[(u.TermSymbol, u.Tree)] = bind match {
        case BindingDef(ParSym(lhs), rhs) => Some(lhs, rhs)
        case _ => None
      }
    }
  }
}
