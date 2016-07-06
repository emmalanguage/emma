package eu.stratosphere
package emma.ast

/** Patterns (for pattern matching). */
trait Patterns { this: AST =>

  /** Patterns (for pattern matching). */
  trait PatternAPI { this: API =>

    import universe._

    /** Patterns. */
    object Pat extends Node {

      /** The `_` pattern. */
      lazy val wildcard: u.Ident = {
        val id = u.Ident(TermName.wildcard)
        set(id, sym = u.NoSymbol)
        id
      }

      def unapply(pat: u.Tree): Option[u.Tree] =
        Option(pat).filter(is.pattern)
    }

    /** Binding in a pattern match. */
    object PatAt extends Node {

      /**
       * Creates a type-checked pattern match binding.
       * @param lhs Must be a value symbol.
       * @param rhs Must be a valid pattern.
       * @return `lhs @ rhs`.
       */
      def apply(lhs: u.TermSymbol, rhs: u.Tree): u.Bind = {
        assert(is.defined(lhs), s"$this LHS `$lhs` is not defined")
        assert(has.name(lhs), s"$this LHS `$lhs` has no name")
        assert(has.tpe(lhs), s"$this LHS `$lhs` has no type")
        assert(is.encoded(lhs), s"$this LHS `$lhs` is not encoded")
        assert(is.value(lhs), s"$this LHS `$lhs` is not a value")
        assert(is.defined(rhs), s"$this RHS is not defined: $rhs")
        assert(is.pattern(rhs), s"$this RHS is not a pattern:\n${Tree.show(rhs)}")

        val at = u.Bind(lhs.name, rhs)
        set(at, sym = lhs, tpe = Type.of(lhs))
        at
      }

      def unapply(at: u.Bind): Option[(u.TermSymbol, u.Tree)] = at match {
        case u.Bind(_, Pat(rhs)) withSym ValSym(lhs) => Some(lhs, rhs)
        case _ => None
      }
    }

    /** Pattern match `case`s. */
    object PatCase extends Node {

      /**
       * Creates a type-checked `case` definition without a guard.
       * @param pat Must be a valid pattern.
       * @param body Must be a term.
       * @return `case pattern => body`.
       */
      def apply(pat: u.Tree, body: u.Tree): u.CaseDef =
        apply(pat, Tree.empty, body)

      /**
       * Creates a type-checked `case` definition with a guard.
       * @param pat Must be a valid pattern.
       * @param guard Must be a boolean expression (has access to bindings in `pattern`).
       * @param body Must be a term.
       * @return `case pattern if guard => body`.
       */
      def apply(pat: u.Tree, guard: u.Tree, body: u.Tree): u.CaseDef = {
        assert(is.defined(pat), s"$this pattern is not defined: $pat")
        assert(is.pattern(pat), s"$this pattern is not valid:\n${Tree.show(pat)}")
        assert(is.defined(body), s"$this body is not defined: $body")
        assert(is.term(body), s"$this body is not a term:\n${Tree.show(body)}")
        assert(has.tpe(body), s"$this body has no type:\n${Tree.showTypes(body)}")
        lazy val bodyT = Type.of(body)
        lazy val guardT = Type.of(guard)
        val grd = if (is.defined(guard)) {
          assert(is.term(guard), s"$this guard is not a term:\n${Tree.show(guard)}")
          assert(has.tpe(guard), s"$this guard has no type:\n${Tree.showTypes(guard)}")
          assert(guardT =:= Type.bool, s"$this guard is not boolean:\n${Tree.showTypes(guard)}")
          guard
        } else Tree.empty

        val cse = u.CaseDef(pat, grd, body)
        set(cse, tpe = bodyT)
        cse
      }

      def unapply(cse: u.CaseDef): Option[(u.Tree, u.Tree, u.Tree)] = cse match {
        case u.CaseDef(Pat(pat), Term(guard), Term(body)) => Some(pat, guard, body)
        case _ => None
      }
    }

    /** Pattern `match`es. */
    object PatMat extends Node {

      /**
       * Creates a type-checked pattern `match`.
       * @param sel The pattern match target (selector) must be a term.
       * @param cases The rest cases of the pattern `match`.
       * @return `sel match { cse; ..cases }`.
       */
      def apply(sel: u.Tree, cases: u.CaseDef*): u.Match = {
        assert(is.defined(sel), s"$this selector is not defined: $sel")
        assert(is.term(sel), s"$this selector is not a term: ${Tree.show(sel)}")
        assert(has.tpe(sel), s"$this selector has no type:\n${Tree.showTypes(sel)}")
        assert(are.defined(cases), s"Not all $this cases are defined")
        assert(have.tpe(cases), s"Not all $this cases have types")

        val mat = u.Match(sel, cases.toList)
        val tpe = Type.weakLub(cases.map(Type.of): _*)
        set(mat, tpe = tpe)
        mat
      }

      def unapplySeq(mat: u.Match): Option[(u.Tree, Seq[u.CaseDef])] = mat match {
        case u.Match(Term(sel), cases) => Some(sel, cases)
        case _ => None
      }
    }
  }
}
