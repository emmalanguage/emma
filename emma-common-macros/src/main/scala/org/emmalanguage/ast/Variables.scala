package org.emmalanguage
package ast

/** Variables (`var`s). */
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

    import universe._
    import u.Flag._

    /** Variable (`var`) symbols. */
    object VarSym extends Node {

      /**
       * Creates a new variable symbol.
       * @param owner The enclosing named entity where this variable is defined.
       * @param name The name of this variable (will be encoded).
       * @param tpe The type of this variable (will be dealiased and widened).
       * @param flags Any additional modifiers (cannot be parameter).
       * @param pos The (optional) source code position where this variable is defined.
       * @return A new variable symbol.
       */
      def apply(owner: u.Symbol, name: u.TermName, tpe: u.Type,
        flags: u.FlagSet = u.NoFlags,
        pos: u.Position = u.NoPosition): u.TermSymbol = {

        assert(are.not(PARAM)(flags), s"$this `$name` cannot be a parameter")
        BindingSym(owner, name, tpe, flags | MUTABLE, pos)
      }

      def unapply(sym: u.TermSymbol): Option[u.TermSymbol] =
        Option(sym).filter(is.variable)
    }

    /** Variable (`var`) references. */
    object VarRef extends Node {

      /**
       * Creates a type-checked variable reference.
       * @param target Must be a variable symbol.
       * @return `target`.
       */
      def apply(target: u.TermSymbol): u.Ident = {
        assert(is.defined(target), s"$this target `$target` is not defined")
        assert(is.variable(target), s"$this target `$target` is not a variable")
        BindingRef(target)
      }

      def unapply(ref: u.Ident): Option[u.TermSymbol] = ref match {
        case BindingRef(VarSym(target)) => Some(target)
        case _ => None
      }
    }

    /** Variable (`var`) definitions. */
    object VarDef extends Node {

      /**
       * Creates a type-checked variable definition.
       * @param lhs Must be a variable symbol.
       * @param rhs The initial value of this variable, owned by `lhs`.
       * @param flags Any additional modifiers (cannot be parameter).
       * @return `..flags var lhs = rhs`.
       */
      def apply(lhs: u.TermSymbol, rhs: u.Tree, flags: u.FlagSet = u.NoFlags): u.ValDef = {
        assert(is.defined(lhs), s"$this LHS `$lhs` is not defined")
        assert(is.variable(lhs), s"$this LHS `$lhs` is not a variable")
        assert(are.not(PARAM)(flags), s"$this LHS `$lhs` cannot be a parameter")
        assert(is.defined(rhs), s"$this RHS is not defined: $rhs")
        BindingDef(lhs, rhs, flags)
      }

      def unapply(bind: u.ValDef): Option[(u.TermSymbol, u.Tree, u.FlagSet)] = bind match {
        case BindingDef(VarSym(lhs), rhs, flags) => Some(lhs, rhs, flags)
        case _ => None
      }
    }

    /** Variable (`var`) mutations (assignments). */
    object VarMut extends Node {

      /**
       * Creates a type-checked variable assignment.
       * @param lhs Must be a variable symbol.
       * @param rhs The new assigned value (type must be a subtype).
       * @return `lhs = rhs`.
       */
      def apply(lhs: u.TermSymbol, rhs: u.Tree): u.Assign = {
        assert(is.defined(lhs), s"$this LHS `$lhs` is not defined")
        assert(is.variable(lhs), s"$this LHS `$lhs` is not a variable")
        assert(is.defined(rhs), s"$this RHS is not defined:\n$rhs")
        assert(is.term(rhs), s"$this RHS is not a term:\n${Tree.show(rhs)}")
        assert(has.tpe(lhs), s"$this LHS `$lhs` has no type")
        assert(has.tpe(rhs), s"$this RHS has no type:\n${Tree.showTypes(rhs)}")
        lazy val (lhT, rhT) = (Type.of(lhs), Type.of(rhs))
        assert(rhT weak_<:< lhT, s"$this LH type `$lhT` is not a supertype of RH type `$rhT`")

        val mut = u.Assign(VarRef(lhs), rhs)
        set(mut, tpe = u.NoType)
        mut
      }

      def unapply(mut: u.Assign): Option[(u.TermSymbol, u.Tree)] = mut match {
        case u.Assign(VarRef(lhs), Term(rhs)) => Some(lhs, rhs)
        case _ => None
      }
    }
  }
}
