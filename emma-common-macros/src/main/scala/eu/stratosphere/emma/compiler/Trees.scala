package eu.stratosphere.emma
package compiler

import scala.annotation.tailrec
import scala.reflect.ClassTag

/** Utility for trees (depends on [[Types]] and [[Symbols]]). */
trait Trees extends Util { this: Types with Symbols =>

  import universe._
  import internal._
  import reificationSupport._

  object Tree {

    // Predefined trees
    lazy val unit = lit(())
    lazy val Root = q"${Term.root}"
    lazy val Java = q"$Root.java"
    lazy val Scala = q"$Root.scala"
    lazy val predef = Type.check(q"$Scala.Predef")

    /** Is `tree` of [[Type]] `T`? */
    def is[T: ClassTag](tree: Tree): Boolean =
      tree match {
        case _: T => true
        case _ => false
      }

    /** Returns `null` of [[Type]] `T`. */
    def nil[T: TypeTag]: Tree =
      nil(Type[T])

    /** Returns `null` of [[Type]] `tpe`. */
    def nil(tpe: Type): Tree =
      Type.check(q"null.asInstanceOf[$tpe]")

    /** Returns a new [[Literal]] containing `const`. */
    def lit[A](const: A): Tree =
      Type.check(Literal(Constant(const)))

    /** Imports everything from a [[Tree]]. */
    def impAll(from: Tree): Import =
      Import(from, ImportSelector(Term.wildcard, -1, null, -1) :: Nil)

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

    /** Returns the [[Set]] of terms defined in `tree`. */
    def defs(tree: Tree): Set[TermSymbol] = tree.collect {
      case dt @ (_: ValDef | _: Bind | _: Function | _: DefDef) => dt
    }.map(Term.of).toSet

    /** Returns the [[Set]] of terms referenced in `tree`. */
    def refs(tree: Tree): Set[TermSymbol] =
      tree.collect {
        case id: Ident if Has.term(id) && {
          val term = Term.of(id)
          term.isVal || term.isVar || term.isMethod
        } => Term.of(id)
      }.toSet

    /** Returns the [[Set]] of references to the outer scope of `tree`. */
    def closure(tree: Tree): Set[TermSymbol] =
      refs(tree) diff defs(tree)

    /** Returns `tree` without [[Type]] ascriptions (ie. `x: Ascription`). */
    @tailrec
    def unAscribe(tree: Tree): Tree = tree match {
      case Typed(inner, _) => unAscribe(inner)
      case _ => tree
    }

    /**
     * Reverses eta expansion.
     *
     * @param tree The [[Tree]] to normalize.
     * @return The [[Tree]] with all eta expansions inlined.
     */
    def etaCompact(tree: Tree): Tree =
      inline(tree, tree.collect {
        case vd @ ValDef(_, Term.eta(_), _, _) => vd
      }: _*)

    /** Returns a reference to `term`. */
    def ref(term: TermSymbol): Ident =
      With(term)(Ident(term.name)).asInstanceOf[Ident]

    /** Returns a binding of `lhs` to use when pattern matching. */
    def bind(lhs: TermSymbol,
      pattern: Tree = Ident(Term.wildcard)): Bind = {

      With(lhs)(Bind(lhs.name, pattern)).asInstanceOf[Bind]
    }

    /** Returns a new value / variable definition. */
    def val_(lhs: TermSymbol, rhs: Tree = EmptyTree): ValDef =
      With(lhs)(internal.valDef(lhs, rhs)).asInstanceOf[ValDef]

    /** Returns a new [[Block]] with the supplied content. */
    def block(body: Tree*): Block = {
      val (statements, expr) =
        if (body.isEmpty) (Nil, unit)
        else (body.init.toList, body.last)

      With.tpe(expr.tpe) {
        Block(statements, expr)
      }.asInstanceOf[Block]
    }

    /** Returns a new field access ([[Select]]). */
    def sel(target: Tree, member: TermName): Select =
      With(Term.decl(target, member)) {
        Select(target, member)
      }.asInstanceOf[Select]

    /** Returns a new type `member` access ([[Select]]). */
    def sel(target: Tree, member: TypeName): Select =
      With(Type.decl(target, member)) {
        Select(target, member)
      }.asInstanceOf[Select]

    /** Returns `target` applied to the ([[Type]]) arguments. */
    @tailrec
    def app(target: Tree, types: Type*)(args: Tree*): Tree =
      if (types.isEmpty) With.tpe(Type.result(target)) {
        Apply(target, args.toList)
      } else app(With.tpe(Type(target.tpe, types: _*)) {
        TypeApply(target, types.map(Type.quote(_)).toList)
      })(args: _*)

    /** Returns a new method invocation. */
    def call(target: Tree, method: TermSymbol, types: Type*)(args: Tree*): Tree =
      call(target, Term.name(method), types: _*)(args: _*)

    /** Returns a new method invocation. */
    def call(target: Tree, method: TermName, types: Type*)(args: Tree*): Tree =
      app(sel(target, method), types: _*)(args: _*)

    /** Returns a new method invocation. */
    def call(target: Tree, method: String, types: Type*)(args: Tree*): Tree =
      call(target, Term.name(method), types: _*)(args: _*)

    /** Returns a new class instantiation. */
    def inst(target: TypeSymbol, types: Type*)(args: Tree*): Tree = {
      val clazz = Type.of(target)
      // TODO: Handle alternatives properly
      val tpe =
        if (types.isEmpty) clazz
        else Type(clazz, types: _*)

      With(clazz.decl(Term.init), tpe) {
        q"new ${resolve(target)}[..$types](..$args)"
      }
    }

    /** Returns a new class instantiation. */
    def inst(tpe: Type, types: Type*)(args: Tree*): Tree =
      inst(tpe.typeSymbol.asType, types: _*)(args: _*)

    /** Returns a fully-qualified reference to `target` (must be static). */
    def resolve(target: Symbol): Tree =
      if (target.isStatic) {
        if (target.isPackage || target.isModule) {
          val owner =
            if (Has.owner(target)) resolve(target.owner)
            else ref(rootMirror.RootPackage)

          sel(owner, Term.name(target))
        } else if (target.isClass) {
          val owner =
            if (Has.owner(target)) resolve(target.owner)
            else ref(rootMirror.RootPackage)

          sel(owner, Type.name(target))
        } else if (target.isType) {
          sel(resolve(target.owner), Type.name(target))
        } else {
          sel(resolve(target.owner), Term.name(target))
        }
      } else if (target.isType) {
        Type.quote(target)
      } else {
        ref(target.asTerm)
      }

    /** Returns a new anonymous [[Function]]. */
    def lambda(args: TermSymbol*)(body: Tree*): Function = {
      val content = if (body.size == 1) body.head else block(body: _*)
      val types = args.map(Type.of).toList
      val tpe = Type.fun(types: _*)(Type.of(content))
      val sym = Term.free(Term.lambda.toString, tpe)
      val params = for ((arg, tpe) <- args zip types)
        yield val_(Term.sym(sym, arg.name, tpe, Flag.SYNTHETIC | Flag.PARAM))

      With(sym) {
        val aliases = args zip params.map(Term.of)
        Function(params.toList, rename(content, aliases: _*))
      }.asInstanceOf[Function]
    }

    /**
     * Bind a dictionary of [[Symbol]]-value pairs in a [[Tree]].
     *
     * @param in The [[Tree]] to substitute in.
     * @param dict A [[Map]] of [[Symbol]]-value pairs to bind.
     * @return This [[Tree]] with all [[Symbol]]s in `dict` bound to their respective values.
     */
    def subst(in: Tree, dict: Map[Symbol, Tree]): Tree =
      if (dict.isEmpty) in else {
        val closure = dict.values
          .flatMap(this.closure)
          .filterNot(dict.keySet)
          .map(_.name).toSet

        val capture = defs(in).filter(term => closure(term.name))
        preWalk(refresh(in, capture.toSeq: _*)) {
          case id: Ident if dict.contains(id.symbol) => dict(id.symbol)
        }
      }

    /**
     * Replace occurrences of the `find` [[Tree]] with the `repl` [[Tree]].
     *
     * @param in The [[Tree]] to substitute in.
     * @param find The [[Tree]] to look for.
     * @param repl The [[Tree]] that should replace `find`.
     * @return A substituted version of the enclosing [[Tree]].
     */
    def replace(in: Tree, find: Tree, repl: Tree): Tree =
      preWalk(in) { case `find` => repl }

    /**
     * Replace a sequence of [[Symbol]]s with references to their aliases.
     *
     * @param in The [[Tree]] to rename in.
     * @param aliases A sequence of aliases to replace.
     * @return The [[Tree]] with the specified [[Symbol]]s replaced.
     */
    def rename(in: Tree, aliases: (TermSymbol, TermSymbol)*): Tree =
      if (aliases.isEmpty) in else {
        val dict = aliases.toMap
        postWalk(in) {
          case vd: ValDef if dict.contains(Term.of(vd)) =>
            val_(dict(Term.of(vd)), vd.rhs)
          case id: Ident if Has.term(id) // could be a type ref
            && dict.contains(Term.of(id)) =>
              ref(dict(Term.of(id)))
          case bd: Bind if dict.contains(Term.of(bd)) =>
            bind(dict(Term.of(bd)), bd.body)
        }
      }

    /**
     * Replace a sequence of [[Symbol]]s in a [[Tree]] with fresh ones.
     *
     * @param in The [[Tree]] to refresh.
     * @param terms The sequence of [[Symbol]]s to rename.
     * @return The [[Tree]] with the specified [[Symbol]]s replaced.
     */
    def refresh(in: Tree, terms: TermSymbol*): Tree =
      rename(in, (for (term <- terms) yield {
        val name = Term.fresh(term.name.toString)
        term -> Term.sym(term.owner, name, term.info, pos = term.pos)
      }): _*)

    /**
     * Inline a sequence of value definitions in a [[Tree]].
     * It's assumed that they are part of the [[Tree]].
     *
     * @param in The [[Tree]] to inline in.
     * @param defs A sequence of [[ValDef]]s to inline.
     * @return The [[Tree]] with the specified value definitions inlined.
     */
    def inline(in: Tree, defs: ValDef*): Tree =
      if (defs.isEmpty) in else {
        val dict = defs.map(vd => vd.symbol -> vd.rhs).toMap
        preWalk(in) {
          case vd: ValDef if dict.contains(vd.symbol) => unit
          case id: Ident if dict.contains(id.symbol) => dict(id.symbol)
        }
      }

    /** Checks common pre-conditions for type-checked [[Tree]]s. */
    @inline
    def verify(tree: Tree): Unit =
      require(Has.tpe(tree), s"Untyped tree:\n${debug(tree)}")
  }

  /** Some useful constants. */
  object Const {

    val maxFunArgs = 22
    val maxTupleElems = 22
  }

  /** Utility for methods ([[DefDef]]s). */
  object Method {

    import Tree._

    /** Returns a free [[MethodSymbol]] (i.e. without owner). */
    def free(name: String, tpe: Type,
      flags: FlagSet = Flag.SYNTHETIC,
      pos: Position = NoPosition): MethodSymbol = {

      // Pre-conditions
      Term.verify(name)
      Type.verify(tpe)

      val sym = newMethodSymbol(NoSymbol, Term.name(name), pos, flags)
      setInfo(sym, Type.fix(tpe))
    }

    /** Returns a new [[MethodSymbol]] with provided specification. */
    def sym(owner: Symbol, name: TermName, tpe: Type,
      flags: FlagSet = Flag.SYNTHETIC,
      pos: Position = NoPosition): MethodSymbol = {

      // Pre-conditions
      Symbol.verify(owner)
      Term.verify(name)
      Type.verify(tpe)

      val sym = newMethodSymbol(owner, name, pos, flags)
      setInfo(sym, Type.fix(tpe))
    }

    /** Returns a new [[MethodType]] (possibly generic with multiple arg lists). */
    def tpe(typeArgs: TypeSymbol*)
      (argLists: Seq[TermSymbol]*)
      (body: Type): Type = {

      // Pre-conditions
      typeArgs.foreach(Symbol.verify)
      argLists.flatten.foreach(Symbol.verify)
      Type.verify(body)

      val result = Type.fix(body)
      val tpe = if (argLists.isEmpty) {
        nullaryMethodType(result)
      } else argLists.foldRight(result) { (args, result) =>
        methodType(args.toList, result)
      }

      if (typeArgs.isEmpty) tpe
      else polyType(typeArgs.toList, tpe)
    }

    /** Returns a new simple method (i.e. with single arg list and no generics). */
    def simple(sym: MethodSymbol,
      flags: FlagSet = Flag.SYNTHETIC)
      (args: TermSymbol*)
      (body: Tree*): DefDef = {

      // Pre-conditions
      Symbol.verify(sym)
      args.foreach(Symbol.verify)
      body.foreach(Tree.verify)

      val bodyBlock =
        if (body.size == 1) body.head
        else block(body: _*)

      val tpe = this.tpe()(args)(bodyBlock.tpe)
      val params = for (arg <- args) yield
        Term.sym(sym, arg.name, Type.of(arg), Flag.SYNTHETIC | Flag.PARAM)

      val paramLists = params.map(val_(_)).toList :: Nil
      val rhs = rename(bodyBlock, args zip params: _*)
      val dd = defDef(sym, Modifiers(flags), paramLists, rhs)
      // Fix symbol and type
      setSymbol(dd, sym)
      setType(dd, tpe)
    }

    /** Returns a new lambda [[Function]] wrapping `method`. */
    def curry(method: MethodSymbol): Function = {
      // Pre-conditions
      Symbol.verify(method)

      val tpe = Type.of(method)
      val args = tpe match {
        case _: NullaryMethodType => Nil
        case mt: MethodType if mt.typeArgs.isEmpty =>
          mt.paramLists.flatten.map(_.asTerm)
        case _ => abort(method.pos,
          s"Cannot curry method with generic type `$tpe`")
      }

      lambda(args: _*) {
        app(ref(method))(args.map(ref): _*)
      }
    }
  }
}
