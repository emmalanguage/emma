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

/** Variables (vars). */
trait Variables { this: AST =>

  /**
   * Variables (`var`s).
   *
   * === Examples ===
   * {{{
   *   var x = 42
   *   x = x * 2
   *   object Greet { var hi = "Hello" }
   *   println(Greet.hi + ", World!")
   *   // This is a `Def.Call` of the method `hi_=`, not a `Var.Mut`:
   *   Greet.hi = "Hola"
   * }}}
   */
  trait VariableAPI { this: API =>

    import u._
    import internal._
    import reificationSupport._
    import Flag._

    /** Variable (var) symbols. */
    object VarSym extends Node {

      /**
       * Creates a type-checked variable symbol.
       * @param own The enclosing named entity where this variable is defined.
       * @param nme The name of this variable (will be encoded).
       * @param tpe The type of this variable (will be dealiased and widened).
       * @param flg Any (optional) modifiers (e.g. private, protected).
       * @param pos The (optional) source code position where this variable is defined.
       * @param ans Any (optional) annotations associated with this variable.
       * @return A new type-checked variable symbol.
       */
      def apply(own: u.Symbol, nme: u.TermName, tpe: u.Type,
        flg: u.FlagSet         = u.NoFlags,
        pos: u.Position        = u.NoPosition,
        ans: Seq[u.Annotation] = Seq.empty
      ): u.TermSymbol = {
        assert(are.not(PARAM, flg), s"$this $nme cannot be a parameter")
        BindingSym(own, nme, tpe, flg | MUTABLE, pos, ans)
      }

      def unapply(sym: u.TermSymbol): Option[u.TermSymbol] =
        Option(sym).filter(is.variable)
    }

    /** Variable (var) references. */
    object VarRef extends Node {

      /**
       * Creates a type-checked variable reference.
       * @param target Must be a variable symbol.
       * @return `target`.
       */
      def apply(target: u.TermSymbol): u.Ident = {
        assert(is.defined(target),  s"$this target is not defined")
        assert(is.variable(target), s"$this target $target is not a variable")
        BindingRef(target)
      }

      def unapply(ref: u.Ident): Option[u.TermSymbol] = ref match {
        case BindingRef(VarSym(target)) => Some(target)
        case _ => None
      }
    }

    /** Variable (var) definitions. */
    object VarDef extends Node {

      /**
       * Creates a type-checked variable definition.
       * @param lhs Must be a variable symbol.
       * @param rhs The initial value of this variable, owned by `lhs`.
       * @return `var lhs = rhs`.
       */
      def apply(lhs: u.TermSymbol, rhs: u.Tree): u.ValDef = {
        assert(is.defined(lhs),  s"$this LHS is not defined")
        assert(is.variable(lhs), s"$this LHS $lhs is not a variable")
        assert(is.defined(rhs),  s"$this RHS is not defined")
        BindingDef(lhs, rhs)
      }

      def unapply(bind: u.ValDef): Option[(u.TermSymbol, u.Tree)] = bind match {
        case BindingDef(VarSym(lhs), rhs) => Some(lhs, rhs)
        case _ => None
      }
    }

    /** Variable (var) mutations (assignments). */
    object VarMut extends Node {

      /**
       * Creates a type-checked variable assignment.
       * @param lhs Must be a variable symbol.
       * @param rhs The new assigned value (type must be a subtype).
       * @return `lhs = rhs`.
       */
      def apply(lhs: u.TermSymbol, rhs: u.Tree): u.Assign = {
        assert(is.defined(lhs),  s"$this LHS is not defined")
        assert(is.variable(lhs), s"$this LHS $lhs is not a variable")
        assert(is.defined(rhs),  s"$this RHS is not defined")
        assert(is.term(rhs),     s"$this RHS is not a term:\n${Tree.show(rhs)}")
        assert(has.tpe(lhs),     s"$this LHS $lhs has no type")
        assert(has.tpe(rhs),     s"$this RHS has no type:\n${Tree.showTypes(rhs)}")
        assert(rhs.tpe <:< lhs.info,
          s"$this LH type ${lhs.info} is not a supertype of RH type ${rhs.tpe}")
        val mut = u.Assign(VarRef(lhs), rhs)
        setType(mut, Type.none)
      }

      def unapply(mut: u.Assign): Option[(u.TermSymbol, u.Tree)] = mut match {
        case u.Assign(VarRef(lhs), Term(rhs)) => Some(lhs, rhs)
        case _ => None
      }
    }
  }
}
