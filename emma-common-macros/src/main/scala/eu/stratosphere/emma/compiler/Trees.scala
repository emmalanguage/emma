package eu.stratosphere.emma
package compiler

import scala.annotation.tailrec
import scala.reflect.macros.Attachments

/** Utility for trees (depends on [[Types]] and [[Symbols]]). */
trait Trees extends Util { this: Terms with Types with Symbols =>

  import universe._
  import internal._
  import reificationSupport._

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
    lazy val unit = lit(())
    lazy val Root = q"${Term.name.root}"
    lazy val Java = q"$Root.java"
    lazy val Scala = q"$Root.scala"
    lazy val predef = Type.check(q"$Scala.Predef")

    /** Returns `null` of [[Type]] `T`. */
    def nil[T: TypeTag]: Tree =
      nil(Type[T])

    /** Returns `null` of [[Type]] `tpe`. */
    def nil(tpe: Type): Tree = {
      assert(Is defined tpe, s"Undefined type: `$tpe`")
      Type.check(q"null.asInstanceOf[$tpe]")
    }

    /** Returns a new [[Literal]] containing `const`. */
    def lit[A](const: A): Tree =
      Type.check(Literal(Constant(const)))

    /** Imports everything from a [[Tree]]. */
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

    /** Returns the [[Set]] of terms defined in `tree`. */
    def defs(tree: Tree): Set[TermSymbol] = tree.collect {
      case dt @ (_: ValDef | _: Bind | _: DefDef) => dt
    }.map(Term.sym(_)).toSet

    /** Returns the [[Set]] of terms referenced in `tree`. */
    def refs(tree: Tree): Set[TermSymbol] =
      tree.collect {
        // Could be a type ref
        case id: Ident if Has.termSym(id) && {
          val term = Term.sym(id)
          term.isVal || term.isVar || term.isMethod
        } => Term.sym(id)
      }.toSet

    /** Returns the [[Set]] of references to the outer scope of `tree`. */
    def closure(tree: Tree): Set[TermSymbol] =
      refs(tree) diff defs(tree)

    /** Casts `tree` up to `tpe` (i.e. `tree: tpe`). */
    def ascribe(tree: Tree, tpe: Type): Typed = {
      assert(Has tpe tree, s"Untyped tree:\n$tree")
      assert(Is defined tpe, s"Undefined type ascription: `$tpe`")
      assert(Type.of(tree) weak_<:< tpe, "Type ascription does not match")
      val typed = Typed(tree, Type.quote(tpe))
      setType(typed, tpe)
    }

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
        case vd @ ValDef(_, Term.name.eta(_), _, _) => vd
      }: _*)

    /** Returns a reference to `sym`. */
    def ref(sym: TermSymbol, quoted: Boolean = false): Ident = {
      assert(Is valid sym, s"Invalid symbol: `$sym`")
      val id = if (quoted) q"`$sym`".asInstanceOf[Ident] else Ident(sym)
      setType(id, Type.of(sym))
    }

    /** Binding constructors and extractors. */
    object bind {

      /** Returns a binding of `lhs` to use when pattern matching. */
      def apply(lhs: TermSymbol, pat: Tree = Ident(Term.name.wildcard)): Bind = {
        assert(Is valid lhs, s"Invalid LHS: `$lhs`")
        val x = Bind(lhs.name, pat)
        setSymbol(x, lhs)
        setType(x, Type.of(lhs))
      }

      def unapply(tree: Tree): Option[(TermSymbol, Tree)] = tree match {
        case x@Bind(_, pat) => Some(Term.sym(x), pat)
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
        val tpt = Type.quote(Type.of(lhs))
        val vd = ValDef(mods, lhs.name, tpt, rhs)
        setSymbol(vd, lhs)
        setType(vd, NoType)
      }

      def unapply(tree: Tree): Option[(TermSymbol, Tree, FlagSet)] = tree match {
        case x@ValDef(mods, _, _, rhs) => Some(Term.sym(x), rhs, mods.flags)
        case _ => None
      }
    }

    /** Returns a new [[Block]] with the supplied content. */
    def block(body: Tree*): Block = {
      assert(body forall Is.valid, "Invalid block body")
      assert(Has tpe body.last, s"Invalid expression:\n${body.last}")
      val (stats, expr) =
        if (body.isEmpty) (Nil, unit)
        else (body.init.filter {
          case q"()" => false
          case _ => true
        }.toList, body.last)

      val block = Block(stats, expr)
      setType(block, Type.of(expr))
    }

    /** Returns a new [[Block]] with the supplied content. */
    def block(init: Seq[Tree], rest: Tree*): Block =
      block(init ++ rest: _*)

    /** Returns `target` applied to the ([[Type]]) arguments. */
    @tailrec
    def app(target: Tree, types: Type*)(args: Tree*): Tree = {
      assert(Has tpe target, s"Untyped target:\n$target")
      assert(types forall Is.defined, "Unspecified type arguments")
      assert(args forall Has.tpe, "Untyped arguments")
      if (types.isEmpty) {
        val app = Apply(target, args.toList)
        setType(app, Type.result(target))
      } else {
        val typeApp = this.typeApp(target, types: _*)
        app(typeApp)(args: _*)
      }
    }

    /** Returns `target` instantiated with the [[Type]] arguments. */
    def typeApp(target: Tree, types: Type*): Tree = {
      assert(Has tpe target, s"Untyped target:\n$target")
      assert(types forall Is.defined, "Unspecified type arguments")
      if (types.isEmpty) target else {
        val typeArgs =  types.map(Type.quote(_)).toList
        val typeApp = TypeApply(target, typeArgs)
        setType(typeApp, Type(target.tpe, types: _*))
      }
    }

    /** Returns a new method invocation. */
    def call(target: Tree, method: TermSymbol, types: Type*)(args: Tree*): Tree =
      app(Term.sel(target, method), types: _*)(args: _*)

    /** Returns a new class instantiation. */
    def inst(target: TypeSymbol, types: Type*)(args: Tree*): Tree = {
      assert(Is valid target, s"Invalid target: `$target`")
      assert(types forall Is.defined, "Unspecified type arguments")
      assert(args forall Has.tpe, "Untyped arguments")
      // TODO: Handle alternatives properly
      val clazz = Type.of(target)
      val sym = clazz.decl(Term.name.init)
      val tpe =
        if (types.isEmpty) clazz
        else Type(clazz, types: _*)

      val inst = q"new ${resolve(target)}[..$types](..$args)"
      setSymbol(inst, sym)
      setType(inst, tpe)
    }

    /** Returns a new class instantiation. */
    def inst(tpe: Type, types: Type*)(args: Tree*): Tree =
      inst(tpe.typeSymbol.asType, types: _*)(args: _*)

    /** Returns a fully-qualified reference to `target` (must be static). */
    def resolve(target: Symbol): Tree =
      if (target.isStatic) {
        val owner =
          if (Has.owner(target)) resolve(target.owner)
          else ref(rootMirror.RootPackage)

        if (target.isModule) {
          Term.sel(owner, rootMirror.staticModule(target.fullName))
        } else if (target.isPackage) {
          Term.sel(owner, rootMirror.staticPackage(target.fullName))
        } else if (target.isClass) {
          Type.sel(owner, rootMirror.staticClass(target.fullName))
        } else if (target.isType) {
          Type.sel(resolve(target.owner), target.asType)
        } else {
          Term.sel(resolve(target.owner), target.asTerm)
        }
      } else if (target.isType) {
        Type.quote(target)
      } else {
        ref(target.asTerm)
      }

    /** Returns a new anonymous [[Function]]. */
    def lambda(args: TermSymbol*)(body: Tree*): Function = {
      assert(args forall Is.valid, "Invalid lambda parameters")
      assert(body forall Is.valid, "Invalid lambda body")
      assert(Has tpe body.last, s"Invalid expression:\n${body.last}")
      val bodyBlock =
        if (body.size == 1) body.head
        else block(body: _*)

      val types = args.map(Type.of)
      val tpe = Type.fun(types: _*)(Type.of(bodyBlock))
      val term = Term.sym.free(Term.name.lambda, tpe)
      val argFlags = Flag.SYNTHETIC | Flag.PARAM
      val params = for ((arg, tpe) <- args zip types) yield
        Term.sym(term, arg.name, tpe, argFlags)

      val paramList = params.map(val_(_, flags = argFlags)).toList
      val rhs = rename(bodyBlock, args zip params: _*)
      val fn = Function(paramList, rhs)
      setSymbol(fn, term)
      setType(fn, tpe)
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
        transform(refresh(in, capture.toSeq: _*)) {
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
      transform(in) {
        case tree if tree.equalsStructure(find) =>
          repl
      }

    /**
     * Replace a sequence of [[Symbol]]s with references to their aliases.
     *
     * @param in The [[Tree]] to rename in.
     * @param aliases A sequence of aliases to replace.
     * @return The [[Tree]] with the specified [[Symbol]]s replaced.
     */
    def rename(in: Tree, aliases: (TermSymbol, TermSymbol)*): Tree =
      rename(in, aliases.toMap)

    /**
     * Replace a sequence of [[Symbol]]s with references to their aliases.
     *
     * @param in The [[Tree]] to rename in.
     * @param dict A dictionary of aliases to replace.
     * @return The [[Tree]] with the specified [[Symbol]]s replaced.
     */
    def rename(in: Tree, dict: Map[TermSymbol, TermSymbol]): Tree =
      if (dict.isEmpty) in else postWalk(in) {
        case vd: ValDef if dict.contains(Term.sym(vd)) =>
          val_(dict(Term.sym(vd)), vd.rhs)
        // Could be a type ref
        case id: Ident if Has.termSym(id) && dict.contains(Term.sym(id)) =>
          ref(dict(Term.sym(id)))
        case bd: Bind if dict.contains(Term.sym(bd)) =>
          bind(dict(Term.sym(bd)), bd.body)
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
        val name = Term.name.fresh(term.name)
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
        transform(in) {
          case vd: ValDef if dict.contains(vd.symbol) => unit
          case id: Ident if dict.contains(id.symbol) => dict(id.symbol)
        }
      }

    /** Checks common pre-conditions for type-checked [[Tree]]s. */
    def verify(tree: Tree): Boolean =
      tree.tpe != null

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

    /** Returns a [[scala.collection.Set]] of all var mutations in `tree`. */
    def assignments(tree: Tree): Set[TermSymbol] = tree.collect {
      case assign: Assign => Term.sym(assign)
    }.toSet
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

      assert(name.nonEmpty, "Empty method name")
      assert(Is defined tpe, s"Undefined method type: `$tpe`")
      val sym = newMethodSymbol(NoSymbol, Term.name(name), pos, flags)
      setInfo(sym, Type.fix(tpe))
    }

    /** Returns a new [[MethodSymbol]] with provided specification. */
    def sym(owner: Symbol, name: TermName, tpe: Type,
      flags: FlagSet = Flag.SYNTHETIC,
      pos: Position = NoPosition): MethodSymbol = {

      assert(name.toString.nonEmpty, "Empty method name")
      assert(Is defined tpe, s"Undefined method type: `$tpe`")
      val sym = newMethodSymbol(owner, name, pos, flags)
      setInfo(sym, Type.fix(tpe))
    }

    /** Returns a new [[MethodType]] (possibly generic with multiple arg lists). */
    def tpe(typeArgs: TypeSymbol*)
      (argLists: Seq[TermSymbol]*)
      (body: Type): Type = {

      assert(typeArgs forall Is.valid, "Unspecified method type parameters")
      assert(argLists.flatten forall Is.valid, "Unspecified method parameters")
      assert(Is defined body, s"Undefined method return type: `$body`")
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

      assert(Is valid sym, s"Invalid method symbol: `$sym`")
      assert(args forall Is.valid, "Unspecified method parameters")
      assert(body forall Is.valid, "Invalid method body")
      assert(Has.tpe(body.last))
      val bodyBlock =
        if (body.size == 1) body.head
        else block(body: _*)

      val argFlags = Flag.SYNTHETIC | Flag.PARAM
      val params = for (arg <- args) yield
        Term.sym(sym, arg.name, Type.of(arg), argFlags)

      val paramLists = params.map(val_(_)).toList :: Nil
      val rhs = rename(bodyBlock, args zip params: _*)
      val dd = defDef(sym, Modifiers(flags), paramLists, rhs)
      setSymbol(dd, sym)
      setType(dd, NoType)
    }

    /** Returns a new lambda [[Function]] wrapping `method`. */
    def curry(method: MethodSymbol): Function = {
      assert(Is valid method, s"Invalid method symbol: `$method`")
      val tpe = Type.of(method)
      val args = tpe match {
        case _: NullaryMethodType => Nil
        case mt: MethodType if mt.typeArgs.isEmpty =>
          mt.paramLists.flatten.map(_.asTerm)
        case _ => abort(method.pos,
          s"Cannot curry method with generic type `$tpe`")
      }

      lambda(args: _*) {
        app(ref(method))(args.map(ref(_)): _*)
      }
    }
  }
}
