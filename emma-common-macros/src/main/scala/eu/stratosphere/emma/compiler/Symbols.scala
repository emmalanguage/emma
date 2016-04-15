package eu.stratosphere.emma
package compiler

import scala.collection.mutable
import scala.reflect.macros.Attachments

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

    /** Returns a mutable metadata container for `sym`. */
    def meta(sym: Symbol): Attachments =
      attachments(sym)

    /** Returns the flags associated with `sym`. */
    def flags(sym: Symbol): FlagSet =
      getFlags(sym)

    /** Wraps `flags` as modifiers. */
    def mods(flags: FlagSet): Modifiers =
      Modifiers(flags)

    /** Returns the modifiers associated with `sym`. */
    def mods(sym: Symbol): Modifiers =
      mods(flags(sym))
  }

  /** Utility for [[Symbol]] owners. */
  object Owner {

    /** Returns the (lazy) owner chain of `target`. */
    def chain(target: Symbol): Stream[Symbol] =
      Stream.iterate(target)(_.owner).takeWhile(Is.defined)

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
      (tree: Tree): Unit = traverse(tree) {

      // def method(...)(...)... = { body }
      case dd @ DefDef(mods, name, Nil, argLists, _, rhs) =>
        val old = dd.symbol.asMethod
        val tpe = Type.of(old)
        val term = Method.sym(owner, name, tpe, mods.flags, dd.pos)
        dict += old -> term
        argLists.flatten.foreach(repair(term, dict))
        repair(term, dict)(rhs)
        setSymbol(dd, term)
        setType(dd, NoType)

      // (...args) => { body }
      case fn @ Function(args, body) =>
        val old = fn.symbol
        val tpe = Type.of(fn)
        val term = Term.sym(owner, Term.lambda, tpe, pos = fn.pos)
        dict += old -> term
        args.foreach(repair(term, dict))
        repair(term, dict)(body)
        setSymbol(fn, term)
        setType(fn, tpe)

      // ...mods val name: tpt = { rhs }
      case vd @ ValDef(mods, name, _, rhs) =>
        val old = vd.symbol
        val tpe = Type.of(old)
        val lhs = Term.sym(owner, name, tpe, mods.flags, vd.pos)
        dict += old -> lhs
        repair(lhs, dict)(rhs)
        setSymbol(vd, lhs)
        setType(vd, NoType)

      // case x @ pattern => { ... }
      case bd @ Bind(_, pattern) =>
        val old = Term.of(bd)
        val tpe = Type.of(old)
        val lhs = Term.sym(owner, old.name, tpe, pos = bd.pos)
        dict += old -> lhs
        repair(owner, dict)(pattern)
        setSymbol(bd, lhs)
        setType(bd, tpe)

      case id: Ident if Has.termSym(id) &&
        dict.contains(id.symbol) =>
        val term = dict(id.symbol)
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
      assert(Is defined sym, s"Undefined symbol: `$sym`")
      sym.name.toTermName
    }

    /** Returns a new [[TermName]]. */
    def name(name: String): TermName = {
      assert(name.nonEmpty, "Empty term name")
      TermName(name)
    }

    /** Returns a fresh term name starting with `prefix$`. */
    def fresh(prefix: Name): TermName =
      fresh(prefix.toString)

    /** Returns a fresh term name starting with `prefix$`. */
    def fresh(prefix: String): TermName = {
      if (prefix.nonEmpty && prefix.last == '$') freshTermName(prefix)
      else freshTermName(s"$prefix$$")
    }.encodedName.toTermName

    /** Returns a free term with the specified properties. */
    def free(name: String, tpe: Type,
      flags: FlagSet = Flag.SYNTHETIC,
      origin: String = null): FreeTermSymbol = {

      assert(name.nonEmpty, "Empty term name")
      assert(Is defined tpe, s"Undefined type: `$tpe`")
      val term = newFreeTerm(name, null, flags, origin)
      setInfo(term, Type.fix(tpe))
    }

    /** Returns a new term with the specified properties. */
    def sym(owner: Symbol, name: TermName, tpe: Type,
      flags: FlagSet = Flag.SYNTHETIC,
      pos: Position = NoPosition): TermSymbol = {

      assert(name.toString.nonEmpty, "Empty term name")
      assert(Is defined tpe, s"Undefined type: `$tpe`")
      val term = termSymbol(owner, name, flags, pos)
      setInfo(term, Type.fix(tpe))
    }

    /** Returns the term of `tree`. */
    def of(tree: Tree): TermSymbol = {
      assert(Has termSym tree, s"No term symbol found for:\n$tree")
      tree.symbol.asTerm
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

    /** Imports a term from a [[Tree]]. */
    def imp(from: Tree, sym: TermSymbol): Import =
      imp(from, name(sym))

    /** Imports a term from a [[Tree]] by name. */
    def imp(from: Tree, name: String): Import =
      imp(from, this.name(name))

    /** Imports a term from a [[Tree]] by name. */
    def imp(from: Tree, name: TermName): Import = {
      assert(Is valid from, s"Invalid import selector:\n$from")
      assert(name.toString.nonEmpty, "Unspecified import")
      Type.check(q"import $from.$name").asInstanceOf[Import]
    }

    /** Returns a new field access ([[Select]]). */
    def sel(target: Tree, member: TermSymbol,
      tpe: Type = NoType): Select = {

      assert(Has tpe target, s"Untyped target:\n$target")
      assert(member.toString.nonEmpty, "Unspecified term member")
      val sel = Select(target, member)
      val result =
        if (Is defined tpe) tpe
        else member.infoIn(Type.of(target))

      setSymbol(sel, member)
      setType(sel, result)
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
  }
}
