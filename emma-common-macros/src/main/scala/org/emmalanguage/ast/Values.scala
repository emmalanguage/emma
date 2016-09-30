package org.emmalanguage
package ast

/** Values (`val`s). */
trait Values { this: AST =>

  /**
   * Values (`val`s).
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

    /** Value (`val`) symbols. */
    object ValSym extends Node {

      /**
       * Creates a new value symbol.
       * @param owner The enclosing named entity where this value is defined.
       * @param name The name of this value (will be encoded).
       * @param tpe The type of this value (will be dealiased and widened).
       * @param flags Any additional modifiers (cannot be mutable or parameter).
       * @param pos The (optional) source code position where this value is defined.
       * @return A new value symbol.
       */
      def apply(owner: u.Symbol, name: u.TermName, tpe: u.Type,
        flags: u.FlagSet = u.NoFlags,
        pos: u.Position = u.NoPosition): u.TermSymbol = {

        assert(are.not(MUTABLE)(flags), s"$this `$name` cannot be mutable")
        assert(are.not(PARAM)(flags), s"$this `$name` cannot be a parameter")
        BindingSym(owner, name, tpe, flags, pos)
      }

      def unapply(sym: u.TermSymbol): Option[u.TermSymbol] =
        Option(sym).filter(is.value)
    }

    /** Value (`val`) references. */
    object ValRef extends Node {

      /**
       * Creates a type-checked value reference.
       * @param target Must be a value symbol.
       * @return `target`.
       */
      def apply(target: u.TermSymbol): u.Ident = {
        assert(is.defined(target), s"$this target `$target` is not defined")
        assert(is.value(target), s"$this target `$target` is not a value")
        BindingRef(target)
      }

      def unapply(ref: u.Ident): Option[u.TermSymbol] = ref match {
        case BindingRef(ValSym(target)) => Some(target)
        case _ => None
      }
    }

    /** Value (`val`) definitions. */
    object ValDef extends Node {

      /**
       * Creates a type-checked value definition.
       * @param lhs Must be a value symbol.
       * @param rhs The RHS of this value, owned by `lhs`.
       * @param flags Any additional modifiers (cannot be mutable or parameter).
       * @return `..flags val lhs = rhs`.
       */
      def apply(lhs: u.TermSymbol, rhs: u.Tree, flags: u.FlagSet = u.NoFlags): u.ValDef = {
        assert(is.defined(lhs), s"$this LHS `$lhs` is not defined")
        assert(is.value(lhs), s"$this LHS `$lhs` is not a value")
        assert(are.not(MUTABLE)(flags), s"$this LHS `$lhs` cannot be mutable")
        assert(are.not(PARAM)(flags), s"$this LHS `$lhs` cannot be a parameter")
        assert(is.defined(rhs), s"$this RHS is not defined: $rhs")
        BindingDef(lhs, rhs, flags)
      }

      def unapply(bind: u.ValDef): Option[(u.TermSymbol, u.Tree, u.FlagSet)] = bind match {
        case BindingDef(ValSym(lhs), rhs, flags) => Some(lhs, rhs, flags)
        case _ => None
      }
    }
  }
}
