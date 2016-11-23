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
       * Creates a new module symbol.
       * @param owner The enclosing named entity where this module is defined.
       * @param name The name of this module (will be encoded).
       * @param flags Any additional modifiers (cannot be mutable or parameter).
       * @param pos The (optional) source code position where this module is defined.
       * @return A new module symbol.
       */
      def apply(owner: u.Symbol, name: u.TermName,
        flags: u.FlagSet = u.NoFlags,
        pos: u.Position = u.NoPosition
      ): u.ModuleSymbol = {
        assert(is.defined(name),        s"$this name is not defined")
        assert(are.not(MUTABLE)(flags), s"$this $name cannot be mutable")
        assert(are.not(PARAM)(flags),   s"$this $name cannot be a parameter")
        val mod = newModuleAndClassSymbol(owner, TermName(name), pos, flags)._1
        setInfo(mod, singleType(u.NoPrefix, mod))
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
