package eu.stratosphere.emma
package compiler

import scala.reflect.macros.Attachments

/** Utility for trees. */
trait Trees extends Util { this: Terms with Types with Symbols =>

  import universe._
  import internal._
  import reificationSupport._
  import Term._

  object Has {

    /** Does `tree` have a type? */
    def tpe(tree: Tree): Boolean =
      Is defined tree.tpe

    /** Does `sym` have a type? */
    def tpe(sym: Symbol): Boolean =
      Is defined sym.info

    /** Does `tree` have a symbol? */
    def sym(tree: Tree): Boolean =
      Is defined tree.symbol

    /** Does `tree` have a term symbol? */
    def termSym(tree: Tree): Boolean =
      Has.sym(tree) && tree.symbol.isTerm

    /** Does `tpe` have a term symbol? */
    def termSym(tpe: Type): Boolean =
      Is defined tpe.termSymbol

    /** Does `tree` have a type symbol? */
    def typeSym(tree: Tree): Boolean =
      Has.sym(tree) && tree.symbol.isType

    /** Does `tpe` have a type symbol? */
    def typeSym(tpe: Type): Boolean =
      Is defined tpe.typeSymbol

    /** Does `sym` have an owner? */
    def owner(sym: Symbol): Boolean = {
      val owner = sym.owner
      Is.defined(owner) && !Is.root(owner)
    }

    /** Does `tree` have a position? */
    def pos(tree: Tree): Boolean =
      Is defined tree.pos

    /** Does `sym` have a position? */
    def pos(sym: Symbol): Boolean =
      Is defined sym.pos
  }

  object Is {
    import Flag._

    /** Does `sym` satisfy the specified property? */
    def apply(property: FlagSet, sym: Symbol): Boolean =
      Symbol mods sym hasFlag property

    /** Is `sym` non-degenerate? */
    def defined(sym: Symbol): Boolean =
      sym != null && sym != NoSymbol

    /** Is `tpe` non-degenerate? */
    def defined(tpe: Type): Boolean =
      tpe != null && tpe != NoType

    /** Is `pos` non-degenerate? */
    def defined(pos: Position): Boolean =
      pos != null && pos != NoPosition

    /** Would `sym` be accepted by the compiler? */
    def valid(sym: Symbol): Boolean =
      Is.defined(sym) && Has.tpe(sym)

    /** Would `tree` be accepted by the compiler? */
    def valid(tree: Tree): Boolean =
      tree.tpe != null

    /** Is `sym` the `_root_` package? */
    def root(sym: Symbol): Boolean =
      sym == rootMirror.RootClass || sym == rootMirror.RootPackage

    /** Is `value` a method or lambda parameter? */
    def param(value: ValDef): Boolean =
      value.mods.hasFlag(PARAM) || Term.sym(value).isParameter

    /** Is `value` a lazy val? */
    def lzy(value: ValDef): Boolean =
      value.mods.hasFlag(LAZY) || Term.sym(value).isLazy

    /** Is `tpe` a method type (e.g. `+ : (Int, Int)Int`)? */
    def method(tpe: Type): Boolean = tpe match {
      case _: NullaryMethodType => true
      case _: MethodType => true
      case _: PolyType => true
      case _ => false
    }

    /** Is `tree` type-checked? */
    def typeChecked(tree: Tree): Boolean =
      tree forAll Is.valid
  }

  object Tree {

    // Predefined trees
    val Root = q"${Term.name.root}"
    val Java = q"$Root.java"
    val Scala = q"$Root.scala"

    /** Imports everything from a tree. */
    def impAll(from: Tree): Import = {
      val sel = ImportSelector(Term.name.wildcard, -1, null, -1)
      val imp = Import(from, sel :: Nil)
      setType(imp, NoType)
    }

    /** Returns a parseable [[String]] representation of `tree`. */
    def show(tree: Tree): String =
      showCode(tree, printRootPkg = true)

    /** Same as `Tree.show(tree)` with extra info (symbols, types and owners). */
    def debug(tree: Tree): String =
      showCode(tree,
        printIds = true,
        printOwners = true,
        printTypes = true)

    /** Parses a snippet of source code and returns the (type-checked) AST. */
    def parse(code: String): Tree =
      Type.check(Trees.this.parse(code))

    /** Returns the set of values (mutable or immutable) defined in `tree`. */
    def vals(tree: Tree): Set[TermSymbol] = tree.collect {
      case value: ValDef => Term sym value
    }.toSet

    /** Returns the set of variables (i.e. mutable) defined in `tree`. */
    def vars(tree: Tree): Set[TermSymbol] = {
      import Flag.MUTABLE
      tree.collect {
        case value: ValDef if Is(MUTABLE, value.symbol) =>
          Term sym value
      }.toSet
    }

    /** Returns the set of lambdas (`Function`s) defined in `tree`. */
    def lambdas(tree: Tree): Set[TermSymbol] = tree.collect {
      case fun: Function => Term sym fun
    }.toSet

    /** Returns the set of methods (`DefDef`s) defined in `tree`. */
    def methods(tree: Tree): Set[TermSymbol] = tree.collect {
      case method: DefDef => Term sym method
    }.toSet

    /** Returns the set of terms defined in `tree`. */
    def defs(tree: Tree): Set[TermSymbol] = tree.collect {
      case definition @ (_: ValDef | _: Bind | _: DefDef) => definition
    }.map(Term sym _).toSet

    /** Returns the set of terms referenced in `tree`. */
    def refs(tree: Tree): Set[TermSymbol] =
      tree.collect { case Term.ref(sym)
        if sym.isVal || sym.isVar || sym.isMethod => sym
      }.toSet

    /** Returns the set of references to the outer scope of `tree`. */
    def closure(tree: Tree): Set[TermSymbol] =
      refs(tree) diff defs(tree)

    /**
     * Reverses eta expansion.
     *
     * @param tree The tree to normalize.
     * @return The tree with all eta expansions inlined.
     */
    def etaCompact(tree: Tree): Tree =
      inline(tree, tree collect {
        case value @ ValDef(_, Term.name.eta(_), _, _) => value
      }: _*)

    /** Binding constructors and extractors. */
    object bind {

      /** Returns a binding of `lhs` to use when pattern matching. */
      def apply(lhs: TermSymbol, rhs: Tree = Ident(Term.name.wildcard)): Bind = {
        assert(Is valid lhs, s"Invalid LHS: `$lhs`")
        val pattern = Owner.at(lhs)(rhs)
        val x = Bind(lhs.name, pattern)
        setSymbol(x, lhs)
        setType(x, Type of lhs)
      }

      def unapply(tree: Tree): Option[(TermSymbol, Tree)] = tree match {
        case bind @ Bind(_, pattern) => Some(Term sym bind, pattern)
        case _ => None
      }
    }

    /** `val` constructors and extractors. No support for lazy vals. */
    object val_ {

      /** Returns a new value / variable definition. */
      def apply(lhs: TermSymbol,
        rhs: Tree = EmptyTree,
        flags: FlagSet = Flag.SYNTHETIC): ValDef = {

        assert(Is valid lhs, s"Invalid LHS: `$lhs`")
        assert(rhs.isEmpty || (Has.tpe(rhs) && Type.of(rhs).weak_<:<(Type of lhs)),
          "LHS and RHS types don't match")

        val mods = Modifiers(flags)
        val T = Type quote Type.of(lhs)
        val body = if (rhs.isEmpty) rhs else Owner.at(lhs)(rhs)
        val value = ValDef(mods, lhs.name, T, body)
        setSymbol(value, lhs)
        setType(value, NoType)
      }

      def unapply(value: ValDef): Option[(TermSymbol, Tree, FlagSet)] =
        Some(Term sym value, value.rhs, value.mods.flags)
    }

    /** Blocks. */
    object block {

      /** Returns a new block with the supplied body. */
      def apply(body: Tree*): Block = {
        assert(body forall Is.valid, "Invalid block body")
        assert(Has tpe body.last, s"Invalid expression:\n${body.last}")
        // Implicitly remove no-ops
        val (stats, expr) =
          if (body.isEmpty) (Nil, unit)
          else (body.init.filter {
            case q"()" => false
            case _ => true
          }.toList, body.last)

        val block = Block(stats, expr)
        setType(block, Type of expr)
      }

      /** Returns a new block with the supplied body. */
      def apply(init: Seq[Tree], rest: Tree*): Block =
        apply(init ++ rest: _*)

      def unapplySeq(block: Block): Option[Seq[Tree]] =
        Some(block.stats :+ block.expr)
    }

    /** Object creation / class instantiation. */
    object inst {

      /** Returns a new class instantiation. */
      def apply(target: TypeSymbol, types: Type*)(args: Tree*): Tree = {
        assert(Is valid target, s"Invalid target: `$target`")
        assert(types forall Is.defined, "Unspecified type arguments")
        assert(args forall Has.tpe, "Untyped arguments")
        // TODO: Handle alternatives properly
        val clazz = Type of target
        val constructor = clazz decl Term.name.init
        val T = if (types.isEmpty) clazz else Type(clazz, types: _*)
        val inst = q"new ${resolve(target)}[..$types](..$args)"
        setSymbol(inst, constructor)
        setType(inst, T)
      }

      /** Returns a new class instantiation. */
      def apply(tpe: Type, types: Type*)(args: Tree*): Tree =
        apply(tpe.typeSymbol.asType, types: _*)(args: _*)

      def unapplySeq(tree: Tree): Option[(TypeSymbol, Seq[Type], Seq[Tree])] = tree match {
        case q"new ${clazz: Tree}[..${types: Seq[Tree]}](..${args: Seq[Tree]})" =>
          Some(Type sym clazz, types map Type.of, args)
        case _ => None
      }
    }

    /** Returns a fully-qualified reference to `target` (must be static). */
    def resolve(target: Symbol): Tree =
      if (target.isStatic) {
        val owner =
          if (Has owner target) resolve(target.owner)
          else Term ref rootMirror.RootPackage

        if (target.isModule) {
          Term.sel(owner, rootMirror.staticModule(target.fullName))
        } else if (target.isPackage) {
          Term.sel(owner, rootMirror.staticPackage(target.fullName))
        } else if (target.isClass) {
          Type.sel(owner, rootMirror.staticClass(target.fullName))
        } else if (target.isType) {
          Type.sel(owner, target.asType)
        } else {
          Term.sel(owner, target.asTerm)
        }
      } else if (target.isType) {
        Type ref target.asType
      } else {
        Term ref target.asTerm
      }

    /** Anonymous Functions. */
    object lambda {

      /** Returns a new anonymous function. */
      def apply(args: TermSymbol*)(body: Tree*): Function = {
        assert(args forall Is.valid, "Invalid lambda parameters")
        assert(body forall Is.valid, "Invalid lambda body")
        assert(Has tpe body.last, s"Invalid expression:\n${body.last}")
        val bodyBlock = if (body.size == 1) body.head else block(body: _*)
        val types = args map Type.of
        val T = Type.fun(types: _*)(Type of bodyBlock)
        val anon = Term.sym.free(Term.name.lambda, T)
        val argFlags = Flag.SYNTHETIC | Flag.PARAM
        val params = for ((arg, tpe) <- args zip types) yield
          Term.sym(anon, arg.name, tpe, argFlags)

        val paramList = params.map(val_(_, flags = argFlags)).toList
        val rhs = Owner.at(anon)(rename(bodyBlock, args zip params: _*))
        val fun = Function(paramList, rhs)
        setSymbol(fun, anon)
        setType(fun, T)
      }

      def unapply(fun: Function): Option[(TermSymbol, Seq[TermSymbol], Tree)] =
        Some(Term sym fun, fun.vparams.map(Term sym _), fun.body)
    }

    /**
     * Bind a dictionary of symbol-value pairs in a tree.
     *
     * @param in The tree to substitute in.
     * @param dict A map of symbol-value pairs to bind.
     * @return This tree with all symbols in `dict` bound to their respective values.
     */
    def subst(in: Tree, dict: Map[Symbol, Tree]): Tree =
      if (dict.isEmpty) in else {
        val closure = dict.valuesIterator
          .flatMap(this.closure)
          .filterNot(dict.keySet)
          .map(_.name).toSet

        val capture = defs(in).filter(sym => closure(sym.name))
        transform(refresh(in, capture.toSeq: _*)) {
          case id: Ident if dict contains id.symbol => dict(id.symbol)
        }
      }

    /**
     * Replace occurrences of the `find` tree with the `repl` tree.
     *
     * @param in The tree to substitute in.
     * @param find The tree to look for.
     * @param repl The tree that should replace `find`.
     * @return A substituted version of the enclosing tree.
     */
    def replace(in: Tree, find: Tree, repl: Tree): Tree =
      transform(in) {
        case tree if tree equalsStructure find =>
          repl
      }

    /**
     * Replace a sequence of symbols with references to their aliases.
     *
     * @param in The tree to rename in.
     * @param aliases A sequence of aliases to replace.
     * @return The tree with the specified symbols replaced.
     */
    def rename(in: Tree, aliases: (TermSymbol, TermSymbol)*): Tree =
      if (aliases.isEmpty) in else rename(in, aliases.toMap)

    /**
     * Replace term symbols with references to their aliases.
     *
     * @param in The tree to rename in.
     * @param pf A partial function mapping term symbols to their aliases.
     * @return The tree with the matched symbols replaced.
     */
    def rename(in: Tree, pf: TermSymbol ~> TermSymbol): Tree =
      preWalk(in) {
        case Term.ref(sym) if pf isDefinedAt sym =>
          Term ref pf(sym)
        case val_(lhs, rhs, flags) if pf isDefinedAt lhs =>
          val_(pf(lhs), rhs, flags)
        case bind(lhs, pattern) if pf isDefinedAt lhs =>
          bind(pf(lhs), pattern)
      }

    /**
     * Replace a sequence of symbols in a tree with fresh ones.
     *
     * @param in The tree to refresh.
     * @param symbols The sequence of symbols to rename.
     * @return The tree with the specified symbols replaced.
     */
    def refresh(in: Tree, symbols: TermSymbol*): Tree =
      rename(in, (for (sym <- symbols) yield {
        val x = Term.name.fresh(sym.name)
        sym -> Term.sym(sym.owner, x, sym.info, pos = sym.pos)
      }): _*)

    /**
     * Inline a sequence of value definitions in a tree.
     * It's assumed that they are part of the tree.
     *
     * @param in The tree to inline in.
     * @param defs A sequence of value definitions to inline.
     * @return The tree with the specified value definitions inlined.
     */
    def inline(in: Tree, defs: ValDef*): Tree =
      if (defs.isEmpty) in else {
        val dict = defs.map(v => v.symbol -> v.rhs).toMap
        transform(in) {
          case value: ValDef if dict contains value.symbol => unit
          case id: Ident if dict contains id.symbol => dict(id.symbol)
        }
      }

    /** Returns a mutable metadata container for `tree`. */
    def meta(tree: Tree): Attachments =
      attachments(tree)

    /** Returns a new `if` branch. */
    def branch(cond: Tree, thn: Tree, els: Tree): Tree = {
      assert(Has.tpe(cond) && Type.of(cond) =:= Type.bool, s"Non-boolean condition:\n$cond")
      assert(Has tpe thn, s"Untyped then branch:\n$thn")
      assert(Has tpe els, s"Untyped else branch:\n$els")
      val branch = If(cond, thn, els)
      setType(branch, Type.weakLub(thn, els))
    }

    /** Returns a set of all var mutations in `tree`. */
    def assignments(tree: Tree): Set[TermSymbol] = tree.collect {
      case assign: Assign => Term sym assign
    }.toSet

    /** Returns a copy of `tree`. */
    def copy(tree: Tree): Tree =
      tree.duplicate.asInstanceOf[Tree]
  }

  /** Some useful constants. */
  object Const {

    val maxFunArgs = 22
    val maxTupleElems = 22
  }

  /** Utility for methods (DefDefs). */
  object Method {
    import Tree._

    /** Returns a free method symbol (i.e. without owner). */
    def free(name: TermName, tpe: Type,
      flags: FlagSet = Flag.SYNTHETIC,
      pos: Position = NoPosition): MethodSymbol = {

      assert(name.toString.nonEmpty, "Empty method name")
      assert(Is defined tpe, s"Undefined method type: `$tpe`")
      val method = newMethodSymbol(NoSymbol, name, pos, flags)
      setInfo(method, Type fix tpe)
    }

    /** Returns a new method symbol with provided specification. */
    def sym(owner: Symbol, name: TermName, tpe: Type,
      flags: FlagSet = Flag.SYNTHETIC,
      pos: Position = NoPosition): MethodSymbol = {

      assert(name.toString.nonEmpty, "Empty method name")
      assert(Is defined tpe, s"Undefined method type: `$tpe`")
      val method = newMethodSymbol(owner, name, pos, flags)
      setInfo(method, Type fix tpe)
    }

    /** Returns a new method type (possibly generic with multiple arg lists). */
    def tpe(typeArgs: TypeSymbol*)
      (argLists: Seq[TermSymbol]*)
      (body: Type): Type = {

      assert(typeArgs forall Is.valid, "Unspecified method type parameters")
      assert(argLists.flatten forall Is.valid, "Unspecified method parameters")
      assert(Is defined body, s"Undefined method return type: `$body`")
      val result = Type fix body
      val T = if (argLists.isEmpty) {
        nullaryMethodType(result)
      } else argLists.foldRight(result) { (args, result) =>
        methodType(args.toList, result)
      }

      if (typeArgs.isEmpty) T
      else polyType(typeArgs.toList, T)
    }

    /** Returns a new simple method (i.e. with single arg list and no generics). */
    def simple(sym: MethodSymbol,
      flags: FlagSet = Flag.SYNTHETIC)
      (args: TermSymbol*)
      (body: Tree*): DefDef = {

      assert(Is valid sym, s"Invalid method symbol: `$sym`")
      assert(args forall Is.valid, "Unspecified method parameters")
      assert(body forall Is.valid, "Invalid method body")
      assert(Has tpe body.last, s"Invalid expression:\n${body.last}")
      val bodyBlock = if (body.size == 1) body.head else block(body: _*)
      val argFlags = Flag.SYNTHETIC | Flag.PARAM
      val params = for (arg <- args) yield
        Term.sym(sym, arg.name, Type of arg, argFlags)

      val paramLists = params.map(val_(_)).toList :: Nil
      val rhs = Owner.at(sym)(rename(bodyBlock, args zip params: _*))
      val method = defDef(sym, Modifiers(flags), paramLists, rhs)
      setSymbol(method, sym)
      setType(method, NoType)
    }

    /** Returns a new lambda function wrapping `method`. */
    def lambdafy(method: MethodSymbol): Function = {
      assert(Is valid method, s"Invalid method symbol: `$method`")
      val T = Type.of(method)
      val args = T match {
        case _: NullaryMethodType => Nil
        case mt: MethodType if mt.typeArgs.isEmpty =>
          mt.paramLists.flatten.map(_.asTerm)
        case _ => abort(method.pos,
          s"Cannot lambdafy method with generic type `$T`")
      }

      lambda(args: _*) {
        app(Term ref method)(args.map(Term ref _))
      }
    }

    /** Method calls (application). */
    object call {

      /** Returns a new method invocation. */
      def apply(target: Tree, method: TermSymbol, types: Type*)(argss: Seq[Tree]*): Tree =
        app(Term.sel(target, method), types: _*)(argss: _*)

      def unapplySeq(tree: Tree): Option[(Tree, TermSymbol, Seq[Type], Seq[Seq[Tree]])] =
        tree match {
          case app(sel(target, method), types, argss @ _*) =>
            Some(target, method, types, argss)
          case _ => None
        }
    }
  }
}
