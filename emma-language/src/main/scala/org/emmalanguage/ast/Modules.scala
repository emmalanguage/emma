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

/** Modules (objects). */
trait Modules { this: AST =>

  /** Modules (objects). */
  trait ModuleAPI { this: API =>

    import universe._
    import internal._
    import reificationSupport._
    import Flag._

    /** Module (object) symbols. */
    object ModuleSym extends Node {

      /**
       * Creates a type-checked module symbol.
       * @param own The enclosing named entity where this module is defined.
       * @param nme The name of this module (will be encoded).
       * @param flg Any (optional) modifiers (e.g. case, final, implicit).
       * @param pos The (optional) source code position where this module is defined.
       * @param ans Any (optional) annotations associated with this module.
       * @return A new type-checked module symbol.
       */
      def apply(own: u.Symbol, nme: u.TermName,
        flg: u.FlagSet         = u.NoFlags,
        pos: u.Position        = u.NoPosition,
        ans: Seq[u.Annotation] = Seq.empty
      ): u.ModuleSymbol = {
        assert(is.defined(nme),       s"$this name is not defined")
        assert(are.not(MUTABLE, flg), s"$this $nme cannot be mutable")
        assert(are.not(PARAM, flg),   s"$this $nme cannot be a parameter")
        val mod = newModuleAndClassSymbol(own, TermName(nme), pos, flg)._1
        setInfo(mod, singleType(u.NoPrefix, mod))
        setAnnotations(mod, ans.toList)
      }

      def unapply(sym: u.ModuleSymbol): Option[u.ModuleSymbol] =
        Option(sym)
    }

    /** Module (object) references. */
    object ModuleRef extends Node {

      /**
       * Creates a type-checked module reference.
       * @param target Must be a module symbol.
       * @return `target`.
       */
      def apply(target: u.ModuleSymbol): u.Ident =
        TermRef(target)

      def unapply(ref: u.Ident): Option[u.ModuleSymbol] = ref match {
        case TermRef(ModuleSym(target)) => Some(target)
        case _ => None
      }
    }

    /** Module (`bject`) accesses. */
    object ModuleAcc extends Node {

      /**
       * Creates a type-checked module access.
       * @param target Must be a term.
       * @param member Must be a dynamic module symbol.
       * @return `target.member`.
       */
      def apply(target: u.Tree, member: u.ModuleSymbol): u.Select =
        TermAcc(target, member)

      def unapply(acc: u.Select): Option[(u.Tree, u.ModuleSymbol)] = acc match {
        case TermAcc(target, ModuleSym(member)) => Some(target, member)
        case _ => None
      }
    }
  }
}
