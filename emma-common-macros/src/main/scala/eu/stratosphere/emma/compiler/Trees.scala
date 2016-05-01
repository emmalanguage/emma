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

      def unapply(tree: Bind): Option[(TermSymbol, Tree)] =
        Some(Term sym tree, tree.body)
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

      def unapply(block: Block): Option[(List[Tree], Tree)] =
        Some(block.stats, block.expr)
    }

    /** Returns a fully-qualified reference to `target` (must be static). */
    def resolve(target: Symbol): Tree =
      if (target.isStatic) {
        val ancestors = Owner.chain(target).tail.reverse

        val owner = ancestors.tail.foldLeft[Tree](Term ref rootMirror.RootPackage)((owner, sym) => {
          assert(sym.isClass, s"Ancestor symbol '${sym.name}' is not a class")
          if (sym.isPackage) {
            Term.sel(owner, rootMirror.staticPackage(sym.fullName))
          } else {
            val cls = sym.asClass
            if (cls.isModuleClass) Term.sel(owner, rootMirror.staticModule(cls.module.fullName))
            else Type.sel(owner, rootMirror.staticClass(cls.fullName))
          }
        })

        if (target.isPackage) {
          Term.sel(owner, rootMirror.staticPackage(target.fullName))
        } else if (target.isModule) {
          Term.sel(owner, rootMirror.staticModule(target.fullName))
        } else if (target.isMethod) {
          Term.sel(owner, target.asMethod)
        } else {
          assert(target.isClass, s"Ancestor symbol '${target.name}' is not a class")
          Type.sel(owner, rootMirror.staticClass(target.fullName))
        }
      } else if (target.isType) {
        Type ref target.asType
      } else {
        Term ref target.asTerm
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
    def replace(in: Tree, find: Tree, repl: Tree): Tree = transform(in) {
      case tree if tree equalsStructure find => repl
    }

    /**
     * Replace a sequence of symbols with references to their aliases.
     *
     * @param in The tree to rename in.
     * @param aliases A sequence of aliases to replace.
     * @return The tree with the specified symbols replaced.
     */
    def rename(in: Tree, aliases: (TermSymbol, TermSymbol)*): Tree =
      if (aliases.isEmpty) in else rename(in)(aliases.toMap)

    /**
     * Replace term symbols with references to their aliases.
     *
     * @param in The tree to rename in.
     * @param pf A partial function mapping term symbols to their aliases.
     * @return The tree with the matched symbols replaced.
     */
    def rename(in: Tree)(pf: TermSymbol =?> TermSymbol): Tree = preWalk(in) {
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

    object branch {
      /** Returns a new `if` branch. */
      def apply(cond: Tree, thn: Tree, els: Tree): If = {
        assert(Has.tpe(cond) && Type.of(cond) =:= Type.bool, s"Non-boolean condition:\n$cond")
        assert(Has tpe thn, s"Untyped then branch:\n$thn")
        assert(Has tpe els, s"Untyped else branch:\n$els")
        val branch = If(cond, thn, els)
        setType(branch, Type.weakLub(thn, els))
      }

      def unapply(branch: If): Option[(Tree, Tree, Tree)] = branch match {
        case If(cond, thn, els) => Some(cond, thn, els)
      }
    }

    /** Returns a set of all var mutations in `tree`. */
    def assignments(tree: Tree): Set[TermSymbol] = tree.collect {
      case Assign(ref(variable), _) => variable
    }.toSet

    /** Returns the closure of `tree` that is modified within `tree`. */
    def closureMutations(tree: Tree): Set[TermSymbol] =
      closure(tree) & assignments(tree)

    /** Returns a copy of `tree`. */
    def copy(tree: Tree): Tree =
      tree.duplicate.asInstanceOf[Tree]

    object assign {
      /** Returns a new assignment `lhs = rhs`. */
      def apply(lhs: Tree, rhs: Tree): Assign = {
        val assign = Assign(lhs, rhs)
        setType(assign, NoType)
      }

      def unapply(assign: Assign): Option[(Tree, Tree)] = assign match {
        case Assign(lhs, rhs) => Some(lhs, rhs)
      }
    }

    /** While loops. */
    object while_ {

      /** Returns a new while loop. */
      def apply(cond: Tree, stat: Tree, stats: Tree*): LabelDef = {
        assert(Has tpe cond, s"Untyped loop condition:\n${debug(cond)}")
        assert(Type.of(cond) =:= Type.bool, s"Non-boolean loop condition: `${Type of cond}`")
        assert(Is.valid(stat) && stats.forall(Is.valid),
          s"Invalid loop body:\n${debug(block(stat +: stats))}")
        val name = Term.name.fresh("while")
        val T = Method.tpe()()(Type.unit)
        val sym = Method.free(name, T)
        val body = branch(cond,
          block(stat +: stats, app(ref(sym))()),
          Term.unit)
        val label = LabelDef(name, Nil, body)
        setSymbol(label, sym)
        setType(label, Type.unit)
      }

      def unapply(label: LabelDef): Option[(Tree, Tree)] = label match {
        case q"while (${cond: Tree}) ${body: Tree}" => Some(cond, body)
        case _ => None
      }
    }

    /** Do while loops. */
    object doWhile {

      /** Returns a new do while loop. */
      def apply(cond: Tree, stat: Tree, stats: Tree*): LabelDef = {
        assert(Has tpe cond, s"Untyped loop condition:\n${debug(cond)}")
        assert(Type.of(cond) =:= Type.bool, s"Non-boolean loop condition: `${Type of cond}`")
        assert(Is.valid(stat) && stats.forall(Is.valid),
          s"Invalid loop body:\n${debug(block(stat +: stats))}")
        val name = Term.name.fresh("doWhile")
        val T = Method.tpe()()(Type.unit)
        val sym = Method.free(name, T)
        val body = block(stat +: stats,
          branch(cond, app(ref(sym))(), Term.unit))
        val label = LabelDef(name, Nil, body)
        setSymbol(label, sym)
        setType(label, Type.unit)
      }

      def unapply(label: LabelDef): Option[(Tree, Tree)] = label match {
        case q"do ${body: Tree} while (${cond: Tree})" => Some(cond, body)
        case _ => None
      }
    }
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
        if (types.isEmpty && argss.isEmpty) {
          val T = method.infoIn(Type of target).finalResultType
          sel(target, method, T)
        } else {
          app(Term.sel(target, method), types: _*)(argss: _*)
        }

      def unapplySeq(tree: Tree): Option[(Tree, TermSymbol, Seq[Type], Seq[Seq[Tree]])] =
        tree match {
          case app(sel(target, method), types, argss @ _*) =>
            Some(target, method, types, argss)
          case _ => None
        }
    }
  }
}
