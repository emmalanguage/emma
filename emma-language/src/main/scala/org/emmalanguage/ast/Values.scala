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

/** Values (vals). */
trait Values { this: AST =>

  /**
   * Values (vals).
   *
   * === Examples ===
   * {{{
   *   val x = 42
   *   println(x)
   *   object Math { val pi = 3.14 }
   *   println(Math.pi)
   *   ("Hello", '!')._1
   *   println(math.e)
   *   import scala.math.e
   *   println(e)
   * }}}
   */
  trait ValueAPI { this: API =>

    import u.Flag._

    /** Value (val) symbols. */
    object ValSym extends Node {

      /**
       * Creates a type-checked value symbol.
       * @param own The enclosing named entity where this value is defined.
       * @param nme The name of this value (will be encoded).
       * @param tpe The type of this value (will be dealiased and widened).
       * @param flg Any (optional) modifiers (e.g. implicit, lazy).
       * @param pos The (optional) source code position where this value is defined.
       * @param ans Any (optional) annotations associated with this value.
       * @return A new type-checked value symbol.
       */
      def apply(own: u.Symbol, nme: u.TermName, tpe: u.Type,
        flg: u.FlagSet         = u.NoFlags,
        pos: u.Position        = u.NoPosition,
        ans: Seq[u.Annotation] = Seq.empty
      ): u.TermSymbol = {
        assert(are.not(MUTABLE, flg), s"$this $nme cannot be mutable")
        assert(are.not(PARAM, flg),   s"$this $nme cannot be a parameter")
        BindingSym(own, nme, tpe, flg, pos, ans)
      }

      def unapply(sym: u.TermSymbol): Option[u.TermSymbol] =
        Option(sym).filter(is.value)
    }

    /** Value (val) references. */
    object ValRef extends Node {

      /**
       * Creates a type-checked value reference.
       * @param target Must be a value symbol.
       * @return `target`.
       */
      def apply(target: u.TermSymbol): u.Ident = {
        assert(is.defined(target), s"$this target is not defined")
        assert(is.value(target),   s"$this target $target is not a value")
        BindingRef(target)
      }

      def unapply(ref: u.Ident): Option[u.TermSymbol] = ref match {
        case BindingRef(ValSym(target)) => Some(target)
        case _ => None
      }
    }

    /** Value (val) definitions. */
    object ValDef extends Node {

      /**
       * Creates a type-checked value definition.
       * @param lhs Must be a value symbol.
       * @param rhs The RHS of this value, owned by `lhs`.
       * @param alwaysSetTpt See BindingDef.apply
       * @return `val lhs = rhs`.
       */
      def apply(lhs: u.TermSymbol, rhs: u.Tree, alwaysSetTpt: Boolean = false): u.ValDef = {
        assert(is.defined(lhs), s"$this LHS is not defined")
        assert(is.value(lhs),   s"$this LHS $lhs is not a value")
        assert(is.defined(rhs), s"$this RHS is not defined")
        BindingDef(lhs, rhs, alwaysSetTpt)
      }

      def unapply(bind: u.ValDef): Option[(u.TermSymbol, u.Tree)] = bind match {
        case BindingDef(ValSym(lhs), rhs) => Some(lhs, rhs)
        case _ => None
      }
    }
  }
}
