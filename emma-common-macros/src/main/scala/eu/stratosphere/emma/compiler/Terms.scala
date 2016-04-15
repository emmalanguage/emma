package eu.stratosphere.emma.compiler

/** Utility for terms. */
trait Terms extends Util { this: Trees with Types with Symbols =>

  import universe._
  import internal.reificationSupport._

  object Term {

    /** Utility for term names. */
    object name {

      // Predefined names
      val anon = apply("anon")
      val init = termNames.CONSTRUCTOR
      val lambda = apply("anonfun")
      val root = termNames.ROOTPKG
      val wildcard = termNames.WILDCARD

      /** Returns a new term name. */
      def apply(name: String): TermName = {
        assert(name.nonEmpty, "Empty term name")
        TermName(name)
      }

      /** Returns the term name of `sym`. */
      def apply(sym: Symbol): TermName = {
        assert(Is defined sym, s"Undefined symbol: `$sym`")
        sym.name.toTermName
      }

      /** Returns an encoded version (i.e. matching `\w+`) of `name`. */
      def encoded(name: String): TermName =
        encoded(apply(name))

      /** Returns an encoded version (i.e. matching `\w+`) of `name`. */
      def encoded(name: Name): TermName =
        name.encodedName.toTermName

      /** Returns a fresh term name starting with `prefix$`. */
      def fresh(prefix: Name): TermName =
        fresh(prefix.toString)

      /** Returns a fresh term name starting with `prefix$`. */
      def fresh(prefix: String): TermName = encoded {
        if (prefix.nonEmpty && prefix.last == '$') freshTermName(prefix)
        else freshTermName(s"$prefix$$")
      }

      def unapply(name: TermName): Option[String] =
        Some(name.toString)

      /** "eta" term name extractor (cf. eta-expansion). */
      object eta {

        val pattern = """eta(\$\d+)+"""

        def unapply(name: TermName): Option[String] = {
          val str = name.toString
          if (str matches pattern) Some(str) else None
        }
      }
    }

    /** Utility for term symbols. */
    object sym {

      /** Returns a new term symbol with specific properties. */
      def apply(owner: Symbol, name: TermName, tpe: Type,
        flags: FlagSet = Flag.SYNTHETIC,
        pos: Position = NoPosition): TermSymbol = {

        assert(name.toString.nonEmpty, "Empty term name")
        assert(Is defined tpe, s"Undefined type: `$tpe`")
        val term = termSymbol(owner, name, flags, pos)
        setInfo(term, Type fix tpe)
      }

      /** Returns the term symbol of `tree`. */
      def apply(tree: Tree): TermSymbol = {
        assert(Has termSym tree, s"No term symbol found for:\n$tree")
        tree.symbol.asTerm
      }

      /** Returns a free term symbol with specific properties. */
      def free(name: TermName, tpe: Type,
        flags: FlagSet = Flag.SYNTHETIC,
        origin: String = null): FreeTermSymbol = {

        val strName = name.toString
        assert(strName.nonEmpty, "Empty term name")
        assert(Is defined tpe, s"Undefined type: `$tpe`")
        val term = newFreeTerm(strName, null, flags, origin)
        setInfo(term, Type fix tpe)
      }

      def unapply(sym: TermSymbol): Option[(TermName, FlagSet)] =
        Some(sym.name, Symbol flags sym)
    }

    /** Term references (Idents). */
    object ref {

      /** Returns a term reference to `sym` (use `quoted=true` for Unicode support). */
      def ref(sym: TermSymbol, quoted: Boolean = false): Ident = {
        assert(Is valid sym, s"Invalid symbol: `$sym`")
        val id = if (quoted) q"`$sym`".asInstanceOf[Ident] else Ident(sym)
        setType(id, Type of sym)
        setSymbol(id, sym)
      }

      def unapply(id: Ident): Option[TermSymbol] =
        if (id.isTerm) Some(Term sym id) else None
    }

    /** Finds field / method `member` accessible in `target` and returns its symbol. */
    def member(target: Symbol, member: TermName): TermSymbol = {
      assert(Is valid target, s"Invalid target: `$target`")
      assert(member.toString.nonEmpty, "Unspecified term member")
      Type.of(target).member(member).asTerm
    }

    /** Finds field / method `member` accessible in `target` and returns its symbol. */
    def member(target: Tree, member: TermName): TermSymbol = {
      assert(Has tpe target, s"Untyped target:\n$target")
      assert(member.toString.nonEmpty, "Unspecified term member")
      Type.of(target).member(member).asTerm
    }

    /** Imports a term from a tree. */
    def imp(from: Tree, sym: TermSymbol): Import =
      imp(from, name(sym))

    /** Imports a term from a tree by name. */
    def imp(from: Tree, name: String): Import =
      imp(from, this.name(name))

    /** Imports a term from a tree by name. */
    def imp(from: Tree, name: TermName): Import = {
      assert(Is valid from, s"Invalid import selector:\n$from")
      assert(name.toString.nonEmpty, "Unspecified import")
      Type.check(q"import $from.$name").asInstanceOf[Import]
    }

    /** Returns a new field access (Select). */
    def sel(target: Tree, member: TermSymbol, tpe: Type = NoType): Select = {
      assert(Has tpe target, s"Untyped target:\n$target")
      assert(member.toString.nonEmpty, "Unspecified term member")
      val sel = Select(target, member)
      val result =
        if (Is defined tpe) tpe
        else member.infoIn(Type of target)

      setSymbol(sel, member)
      setType(sel, result)
    }
  }
}
