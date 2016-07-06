package eu.stratosphere
package emma.ast

/** Bindings (values, variables and parameters). */
trait Bindings { this: AST =>

  /**
   * Bindings (values, variables and parameters).
   *
   * === Examples ===
   * {{{
   *   // values
   *   val a = 42
   *
   *   // variables
   *   var b = true
   *
   *   // parameters
   *   def method(c: Char) = c
   *   (d: Double) => d
   *   abstract class A(e: Exception) { e.printStackTrace() }
   *   class C(f: Float) { println(f) }
   * }}}
   */
  trait BindingAPI { this: API =>

    import universe._
    import u.Flag._

    /** Binding symbols (values, variables and parameters). */
    object BindingSym extends Node {

      /**
       * Creates a new binding symbol.
       * @param owner The enclosing named entity where this binding is defined.
       * @param name The name of this binding (will be encoded).
       * @param tpe The type of this binding (will be dealiased and widened).
       * @param flags Any additional modifiers (distinguish between vals, vars and parameters).
       * @param pos The (optional) source code position where this binding is defined.
       * @return A new binding symbol.
       */
      def apply(owner: u.Symbol, name: u.TermName, tpe: u.Type,
        flags: u.FlagSet = u.NoFlags,
        pos: u.Position = u.NoPosition): u.TermSymbol
        = TermSym(owner, name, tpe, flags, pos)

      def unapply(sym: u.TermSymbol): Option[u.TermSymbol] =
        Option(sym).filter(is.binding)
    }

    /** Binding references (values, variables and parameters). */
    object BindingRef extends Node {

      /**
       * Creates a type-checked binding reference.
       * @param target Must be a binding symbol.
       * @return `target`.
       */
      def apply(target: u.TermSymbol): u.Ident = {
        assert(is.defined(target), s"$this target `$target` is not defined")
        assert(is.binding(target), s"$this target `$target` is not a binding")
        TermRef(target)
      }

      def unapply(ref: u.Ident): Option[u.TermSymbol] = ref match {
        case TermRef(BindingSym(target)) => Some(target)
        case _ => None
      }
    }

    /** Binding definitions (values, variables and parameters). */
    object BindingDef extends Node {

      /**
       * Creates a type-checked binding definition.
       * @param lhs Must be a binding symbol.
       * @param rhs The value of this binding (empty by default), owned by `lhs`.
       * @param flags Any additional modifiers (distinguish values, variables and parameters).
       * @return `..flags [val|var] lhs [= rhs]`.
       */
      def apply(lhs: u.TermSymbol,
        rhs: u.Tree = u.EmptyTree,
        flags: u.FlagSet = u.NoFlags): u.ValDef = {

        assert(is.defined(lhs), s"$this LHS `$lhs` is not defined")
        assert(is.binding(lhs), s"$this LHS `$lhs` is not a binding")
        assert(has.name(lhs), s"$this LHS `$lhs` has no name")
        assert(has.tpe(lhs), s"$this LHS `$lhs` has no type")
        assert(is.encoded(lhs), s"$this LHS `$lhs` is not encoded")
        val mods = u.Modifiers(get.flags(lhs) | flags)
        assert(!mods.hasFlag(LAZY), s"$this LHS `$lhs` cannot be lazy")
        val (name, lhT) = (lhs.name, Type.of(lhs))
        val body = if (is.defined(rhs)) {
          assert(is.term(rhs), s"$this RHS is not a term:\n${Tree.show(rhs)}")
          assert(has.tpe(rhs), s"$this RHS has no type:\n${Tree.showTypes(rhs)}")
          lazy val rhT = Type.of(rhs)
          assert(rhT weak_<:< lhT, s"$this LH type `$lhT` is not a supertype of RH type `$rhT`")
          Owner.at(lhs)(rhs)
        } else Tree.empty

        val tpt = TypeQuote(lhT)
        val bind = u.ValDef(mods, name, tpt, body)
        set(bind, sym = lhs)
        bind
      }

      def unapply(bind: u.ValDef): Option[(u.TermSymbol, u.Tree, u.FlagSet)] = bind match {
        case u.ValDef(mods, _, _, Term(rhs)) withSym BindingSym(lhs) => Some(lhs, rhs, mods.flags)
        case _ => None
      }
    }
  }
}
