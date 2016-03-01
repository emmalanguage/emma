package eu.stratosphere.emma
package compiler

import scala.collection.mutable

/** Utility for symbols (depends on [[Trees]] and [[Types]]). */
trait Symbols extends Util { this: Trees with Types =>

  import universe._
  import internal.reificationSupport._

  /** Utility for [[Symbol]]s. */
  object Symbol {

    /** [[Map]] of all tuple symbols by number of elements. */
    lazy val tuple: Map[Int, ClassSymbol] = {
      for (n <- 1 to Const.maxTupleElems) yield
        n -> rootMirror.staticClass(s"scala.Tuple$n")
    }.toMap

    /** [[Map]] of all [[Function]] symbols by argument count. */
    lazy val fun: Map[Int, ClassSymbol] = {
      for (n <- 0 to Const.maxFunArgs) yield
        n -> rootMirror.staticClass(s"scala.Function$n")
    }.toMap

    /** [[Set]] of all [[Function]] symbols. */
    lazy val funSet: Set[Symbol] =
      fun.values.toSet

    /** Is `sym` the `_root_` package? */
    def isRoot(sym: Symbol): Boolean =
      sym == rootMirror.RootClass ||
        sym == rootMirror.RootPackage

    /** Returns `true` if `sym` is not degenerate. */
    def isDefined(sym: Symbol): Boolean =
      sym != null && sym != NoSymbol

    /** Checks common pre-conditions for type-checked [[Symbol]]s. */
    @inline
    def verify(sym: Symbol): Unit = {
      require(isDefined(sym), "Undefined symbol")
      require(Has.tpe(sym), s"Untyped symbol `$sym`")
    }
  }

  /** Utility for [[Symbol]] owners. */
  object Owner {

    /** Returns the (lazy) owner chain of `target`. */
    def chain(target: Symbol): Stream[Symbol] =
      Stream.iterate(target)(_.owner)
        .takeWhile(Symbol.isDefined)

    /**
     * Fixes the [[Symbol]] owner chain of `tree` at `owner`.
     *
     * WARN:
     *  - Mutates `tree` in place.
     *  - No support for type definitions (including anonymous classes).
     *  - No support for type references (generics).
     *
     * @param owner The [[Symbol]] to set as the new owner of `tree`.
     * @param dict A [[Map]] with terms that have been fixed so far.
     */
    def repair(owner: Symbol,
      dict: mutable.Map[Symbol, Symbol] = mutable.Map.empty)
      (tree: Tree): Unit = topDown(tree) {

      // def method(...)(...)... = { body }
      case dd @ DefDef(mods, name, Nil, argLists, tpt, rhs) =>
        val old = dd.symbol.asMethod
        val tpe = Type.oneOf(old.info, dd.tpe, tpt.tpe)
        val term = Method.sym(owner, name, tpe, mods.flags, dd.pos)
        dict += old -> term
        argLists.flatten.foreach(repair(term, dict))
        repair(term, dict)(rhs)
        // Fix symbol and type
        setSymbol(dd, term)
        setType(dd, tpe)

      // (...args) => { body }
      case fn @ Function(args, body) =>
        val old = fn.symbol
        val tpe = Type.oneOf(old.info, fn.tpe)
        val term = Term.sym(owner, Term.lambda, tpe, pos = fn.pos)
        dict += old -> term
        args.foreach(repair(term, dict))
        repair(term, dict)(body)
        // Fix symbol and type
        setSymbol(fn, term)
        setType(fn, Type.of(fn))

      // ...mods val name: tpt = { rhs }
      case vd @ ValDef(mods, name, tpt, rhs) =>
        val old = vd.symbol
        val tpe = Type.oneOf(old.info, vd.tpe, tpt.tpe)
        val lhs = Term.sym(owner, name, tpe, mods.flags, vd.pos)
        dict += old -> lhs
        repair(lhs, dict)(rhs)
        // Fix symbol and type
        setSymbol(vd, lhs)
        setType(vd, tpe)

      // case x @ pattern => { ... }
      case bd @ Bind(_, pattern) =>
        val old = Term.of(bd)
        val tpe = Type.oneOf(old.info, bd.tpe)
        val lhs = Term.sym(owner, old.name, tpe, pos = bd.pos)
        dict += old -> lhs
        repair(owner, dict)(pattern)
        // Fix symbol and type
        setSymbol(bd, lhs)
        setType(bd, tpe)

      case id: Ident if Has.term(id) &&
        dict.contains(id.symbol) =>
        val term = dict(id.symbol)
        // Fix symbol and type
        setSymbol(id, term)
        setType(id, Type.of(term))
    }
  }

  /** Utility for [[TermSymbol]]s. */
  object Term {

    // Predefined names
    lazy val root = termNames.ROOTPKG
    lazy val init = termNames.CONSTRUCTOR
    lazy val wildcard = termNames.WILDCARD
    lazy val lambda = name("anonfun")
    lazy val anon = name("anon")

    /** Returns the name of `sym`. */
    def name(sym: Symbol): TermName = {
      // Pre-conditions
      Symbol.verify(sym)

      sym.name.toTermName
    }

    /** Returns a new [[TermName]]. */
    def name(name: String): TermName = {
      // Pre-conditions
      verify(name)

      TermName(name)
    }

    /** Returns a fresh [[TermName]] starting with `prefix`. */
    def fresh(prefix: String): TermName =
      freshTerm(prefix)

    /** Returns a free term with the specified properties. */
    def free(name: String, tpe: Type,
      flags: FlagSet = Flag.SYNTHETIC,
      origin: String = null): FreeTermSymbol = {

      // Pre-conditions
      verify(name)
      Type.verify(tpe)

      val term = newFreeTerm(name, null, flags, origin)
      setInfo(term, Type.fix(tpe))
    }

    /** Returns a new term with the specified properties. */
    def sym(owner: Symbol, name: TermName, tpe: Type,
      flags: FlagSet = Flag.SYNTHETIC,
      pos: Position = NoPosition): TermSymbol = {

      // Pre-conditions
      Symbol.verify(owner)
      verify(name)
      Type.verify(tpe)

      val term = termSymbol(owner, name, flags, pos)
      // Fix type
      setInfo(term, Type.fix(tpe))
    }

    /** Returns the term of `tree`. */
    def of(tree: Tree): TermSymbol = {
      // Pre-conditions
      Tree.verify(tree)

      tree.symbol.asTerm
    }

    /** Finds `member` declared in `target` and returns its term. */
    def decl(target: Symbol, member: TermName): TermSymbol = {
      // Pre-conditions
      Symbol.verify(target)
      verify(member)

      Type.of(target).decl(member).asTerm
    }

    /** Finds `member` declared in `target` and returns its term. */
    def decl(target: Tree, member: TermName): TermSymbol = {
      // Pre-conditions
      Tree.verify(target)
      verify(member)

      Type.of(target).decl(member).asTerm
    }

    /** Fixes the [[Type]] of terms in `tree`. */
    def check(tree: Tree): Tree = {
      // Pre-conditions
      Tree.verify(tree)

      val aliases = tree.collect {
        case vd @ ValDef(mods, name, tpt, _)
          if Has.term(vd) && !Has.tpe(vd.symbol) =>
            val old = of(vd)
            val tpe = Type.oneOf(vd.tpe, tpt.tpe)
            old -> sym(old.owner, name, tpe, mods.flags, vd.pos)

        case bd @ Bind(name: TermName, _)
          if Has.term(bd) && !Has.tpe(bd.symbol) =>
            val old = of(bd)
            old -> sym(old.owner, name, bd.tpe, pos = bd.pos)

        case fn: Function
          if Has.term(fn) && !Has.tpe(fn.symbol) =>
            val old = of(fn)
            old -> sym(old.owner, lambda, fn.tpe, pos = fn.pos)
      }

      Tree.rename(tree, aliases: _*)
    }

    /** Imports a term from a [[Tree]]. */
    def imp(from: Tree, sym: TermSymbol): Import =
      imp(from, name(sym))

    /** Imports a term from a [[Tree]] by name. */
    def imp(from: Tree, name: String): Import =
      imp(from, this.name(name))

    /** Imports a term from a [[Tree]] by name. */
    def imp(from: Tree, name: TermName): Import = {
      // Pre-conditions
      Tree.verify(from)
      verify(name)

      Type.check {
        q"import $from.$name"
      }.asInstanceOf[Import]
    }

    /** "eta" [[TermName]] extractor (cf. eta-expansion). */
    object eta {

      val regex = """eta(\$\d+)+"""

      def unapply(name: TermName): Option[String] = {
        val str = name.toString
        if (str.matches(regex)) Some(str)
        else None
      }
    }

    /** Checks common pre-conditions for [[TermName]]s. */
    @inline
    def verify(name: TermName): Unit =
      verify(name.toString)

    /** Checks common pre-conditions for [[TermName]]s. */
    @inline
    def verify(name: String): Unit =
      require(name.nonEmpty, "Empty term name")
  }
}
