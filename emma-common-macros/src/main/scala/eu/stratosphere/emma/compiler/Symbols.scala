package eu.stratosphere.emma
package compiler

import scala.collection.mutable
import scala.reflect.macros.Attachments

/** Utility for symbols. */
trait Symbols extends Util { this: Trees with Terms with Types =>

  import universe._
  import internal.reificationSupport._

  object Symbol {

    /** Map of all tuple symbols by number of elements. */
    lazy val tuple: Map[Int, ClassSymbol] = {
      for (n <- 1 to Const.maxTupleElems) yield
        n -> rootMirror.staticClass(s"scala.Tuple$n")
    }.toMap

    /** Map of all function symbols by argument count. */
    lazy val fun: Map[Int, ClassSymbol] = {
      for (n <- 0 to Const.maxFunArgs) yield
        n -> rootMirror.staticClass(s"scala.Function$n")
    }.toMap

    /** Set of all function symbols. */
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

  /** Utility for symbol owners. */
  object Owner {

    /** Returns the enclosing owner in the current program context. */
    def enclosing: Symbol = enclosingOwner

    /**
     * Duplicates `tree` and repairs its owner chain (see
     * [[eu.stratosphere.emma.compiler.Symbols.Owner.repair()]]).
     */
    def at(owner: Symbol)(tree: Tree) = {
      val copy = Tree copy tree
      repair(owner)(copy)
      copy
    }

    /** Returns the (lazy) owner chain of `target`. */
    def chain(target: Symbol): Stream[Symbol] =
      Stream.iterate(target)(_.owner).takeWhile(Is.defined)

    /**
     * Fixes the symbol owner chain of `tree` at `owner`.
     *
     * Warning:
     *  - Mutates `tree` in place.
     *  - No support for type definitions (including anonymous classes).
     *  - No support for type references (generics).
     *
     * @param owner The symbol to set as the new owner of `tree`.
     * @param dict A map with terms that have been fixed so far.
     */
    private[compiler] def repair(owner: Symbol,
      dict: mutable.Map[Symbol, Symbol] = mutable.Map.empty)
      (tree: Tree): Unit = traverse(tree) {

      // def method(...)(...)... = { body }
      case method @ DefDef(mods, name, Nil, argLists, _, rhs) =>
        val old = method.symbol.asMethod
        val tpe = Type.of(old)
        val sym = Method.sym(owner, name, tpe, mods.flags, method.pos)
        dict += old -> sym
        argLists.flatten foreach repair(sym, dict)
        repair(sym, dict)(rhs)
        setSymbol(method, sym)
        setType(method, NoType)

      // (...args) => { body }
      case lambda @ Function(args, body) =>
        val old = lambda.symbol
        val tpe = Type.of(lambda)
        val sym = Term.sym(owner, Term.name.lambda, tpe, pos = lambda.pos)
        dict += old -> sym
        args foreach repair(sym, dict)
        repair(sym, dict)(body)
        setSymbol(lambda, sym)
        setType(lambda, tpe)

      // ...mods val name: tpt = { rhs }
      case value @ ValDef(mods, name, _, rhs) =>
        val old = value.symbol
        val tpe = Type.of(old)
        val lhs = Term.sym(owner, name, tpe, mods.flags, value.pos)
        dict += old -> lhs
        repair(lhs, dict)(rhs)
        setSymbol(value, lhs)
        setType(value, NoType)

      // case x @ pattern => { ... }
      case bind @ Bind(_, pattern) =>
        val old = Term.sym(bind)
        val tpe = Type.of(old)
        val lhs = Term.sym(owner, old.name, tpe, pos = bind.pos)
        dict += old -> lhs
        repair(owner, dict)(pattern)
        setSymbol(bind, lhs)
        setType(bind, tpe)

      case id @ Term.ref(old) if dict.contains(old) =>
        val sym = dict(old)
        setSymbol(id, sym)
        setType(id, Type of sym)
    }
  }
}
