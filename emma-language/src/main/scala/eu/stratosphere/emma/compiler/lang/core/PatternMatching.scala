package eu.stratosphere.emma
package compiler.lang.core

import compiler.Common

/** Pattern matching destructuring for the Core language. */
private[core] trait PatternMatching extends Common {
  self: Core =>

  import universe._
  import Tree._
  import Term.name.fresh

  private[core] object PatternMatching {

    /**
     * Eliminates irrefutable pattern matches by replacing them with value definitions corresponding
     * to bindings and field accesses corresponding to case class extractors.
     *
     * == Assumptions ==
     * - The selector of `mat` is non-null;
     * - `mat` has exactly one irrefutable case;
     * - No guards are used;
     *
     * == Example ==
     * {{{
     *   ("life", 42) match {
     *     case (s: String, i: Int) =>
     *       s + i
     *   } \\ <=>
     *   {
     *     val x$1 = ("life", 42)
     *     val s = x$1._1: String
     *     val i = x$1._2: Int
     *     val x$2 = s + i
     *     x$2
     *   }
     * }}}
     */
    val destructPatternMatches: Tree => Tree = postWalk {
      case Match(sel, cases) =>
        assert(cases.nonEmpty, "No cases for pattern match")
        val CaseDef(pat, guard, body) = cases.head
        assert(guard.isEmpty, "Emma does not support guards for pattern matches")
        val T = Type.of(sel)
        val lhs = Term.sym.free(fresh("x"), T)
        val binds = irrefutable(Term ref lhs, pat)
        assert(binds.isDefined, "Unsupported refutable pattern match case detected")
        block(val_(lhs, sel) :: binds.get, body)
    }

    /**
     * Tests if `pattern` is irrefutable for the given selector, i.e. if it always matches. If it
     * is, returns a sequence of value definitions equivalent to the bindings in `pattern`.
     * Otherwise returns [[scala.None]].
     *
     * A pattern `p` is irrefutable for type `T` when:
     * - `p` is the wildcard pattern (_);
     * - `p` is a variable pattern;
     * - `p` is a typed pattern `x: U` and `T` is a subtype of `U`;
     * - `p` is an alternative pattern `p1 | ... | pn` and at least one `pi` is irrefutable;
     * - `p` is a case class pattern `c(p1, ..., pn)`, `T` is an instance of `c`, the primary
     * constructor of `c` has argument types `T1, ..., Tn` and each `pi` is irrefutable for `Ti`.
     *
     * Caution: Does not consider `null` refutable in contrast to the standard Scala compiler. This
     * might cause [[java.lang.NullPointerException]]s.
     */
    private def irrefutable(sel: Tree, pattern: Tree): Option[List[ValDef]] = {
      def isCaseClass(tree: Tree) = {
        val sym = Type.of(tree).typeSymbol
        sym.isClass &&
          sym.asClass.isCaseClass &&
          sym == Type.of(sel).typeSymbol
      }

      def isVarPattern(id: Ident) =
        !id.isBackquoted && !id.name.toString.head.isUpper

      pattern match {
        case id: Ident if isVarPattern(id) =>
          Some(Nil)

        case Type.ascription(pat, tpe) if Type.of(sel) weak_<:< tpe =>
          irrefutable(Type.ascription(sel, tpe), pat)

        case bind(lhs, pat) =>
          lazy val value = val_(lhs, sel)
          irrefutable(Term ref lhs, pat).map(value :: _)

        // Alternative patterns don't allow binding
        case Alternative(patterns) =>
          patterns.flatMap(irrefutable(sel, _)).headOption

        case extractor@(_: Apply | _: UnApply) if isCaseClass(extractor) =>
          val args = extractor match {
            case app: Apply => app.args
            case un: UnApply => un.args
          }

          val T = Type of sel
          val clazz = Type.of(extractor).typeSymbol.asClass
          val inst = clazz.primaryConstructor
          val params = inst.infoIn(T).paramLists.head
          val selects = params.map { param =>
            val field = T.member(param.name).asTerm
            Term.sel(sel, field, Type of param)
          }

          val patterns = selects zip args map (irrefutable _).tupled
          if (patterns.exists(_.isEmpty)) None
          else Some(patterns.flatMap(_.get))

        case _ =>
          None
      }
    }
  }

}
