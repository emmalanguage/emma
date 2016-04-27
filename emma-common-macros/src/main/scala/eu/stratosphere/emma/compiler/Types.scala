package eu.stratosphere.emma
package compiler

import scala.annotation.tailrec
import scala.language.higherKinds

/** Utility for types. */
trait Types extends Util { this: Trees with Symbols =>

  import universe._
  import internal.reificationSupport._
  import Const._
  import Flag._

  object Type {

    // Predefined types
    lazy val nothing = definitions.NothingTpe
    lazy val unit = definitions.UnitTpe
    lazy val bool = definitions.BooleanTpe
    lazy val char = definitions.CharTpe
    lazy val byte = definitions.ByteTpe
    lazy val short = definitions.ShortTpe
    lazy val int = definitions.IntTpe
    lazy val long = definitions.LongTpe
    lazy val float = definitions.FloatTpe
    lazy val double = definitions.DoubleTpe

    lazy val string = Type[String]
    lazy val bigInt = Type[BigInt]
    lazy val bigDec = Type[BigDecimal]

    lazy val void = Type[java.lang.Void]
    lazy val jBool = Type[java.lang.Boolean]
    lazy val jChar = Type[java.lang.Character]
    lazy val jByte = Type[java.lang.Byte]
    lazy val jShort = Type[java.lang.Short]
    lazy val jInt = Type[java.lang.Integer]
    lazy val jLong = Type[java.lang.Long]
    lazy val jFloat = Type[java.lang.Float]
    lazy val jDouble = Type[java.lang.Double]
    lazy val jBigInt = Type[java.math.BigInteger]
    lazy val jBigDec = Type[java.math.BigDecimal]

    /** Utility for type names. */
    object name {

      /** Returns a new type name. */
      def apply(name: String): TypeName = {
        assert(name.nonEmpty, "Empty type name")
        TypeName(name)
      }

      /** Returns the type name of `sym`. */
      def apply(sym: Symbol): TypeName = {
        assert(Is defined sym, s"Undefined symbol: `$sym`")
        sym.name.toTypeName
      }

      /** Returns an encoded version (i.e. matching `\w+`) of `name`. */
      def encoded(name: String): TypeName =
        encoded(apply(name))

      /** Returns an encoded version (i.e. matching `\w+`) of `name`. */
      def encoded(name: Name): TypeName =
        name.encodedName.toTypeName

      /** Returns a fresh type name starting with `prefix$`. */
      def fresh(prefix: String): TypeName = encoded {
        if (prefix.nonEmpty && prefix.last == '$') freshTypeName(prefix)
        else freshTypeName(s"$prefix$$")
      }

      /** Returns a fresh type name starting with `prefix$`. */
      def fresh(prefix: Name): TypeName =
        fresh(prefix.toString)

      /** Returns a fresh type name starting with `prefix$`. */
      def fresh(prefix: Symbol): TypeName =
        fresh(prefix.name)

      def unapply(name: TypeName): Option[String] =
        Some(name.toString)
    }

    /** Utility for type symbols. */
    object sym {

      /** Returns a new type symbol with specific attributes. */
      def apply(owner: Symbol, name: TypeName,
        flags: FlagSet = NoFlags,
        pos: Position = NoPosition): TypeSymbol = {

        assert(name.toString.nonEmpty, "Empty type name")
        typeSymbol(owner, name, flags | SYNTHETIC, pos)
      }

      /** Returns the type symbol of `tree` (`Has.typeSym` must be true). */
      def apply(tree: Tree): TypeSymbol = {
        assert(Has typeSym tree, s"No type symbol found for:\n$tree")
        tree.symbol.asType
      }

      /** Returns a free type symbol with specific attributes. */
      def free(name: TypeName,
        flags: FlagSet = NoFlags,
        origin: String = null): FreeTypeSymbol = {

        val strName = name.toString
        assert(strName.nonEmpty, "Empty type name")
        newFreeType(strName, flags | SYNTHETIC, origin)
      }

      /** Returns a new free type symbol equivalent to `original` but with new flags. */
      def free(original: TypeSymbol, flags: FlagSet): FreeTypeSymbol =
        free(name(original), flags)

      /** Returns a new type symbol equivalent to `original` but with a fresh name. */
      def fresh(original: TermSymbol, flags: FlagSet = NoFlags): FreeTypeSymbol =
        free(name fresh original, flags)

      def unapply(sym: TypeSymbol): Option[(TypeName, FlagSet)] =
        Some(sym.name, Symbol flags sym)
    }

    /** Type references (Idents). */
    object ref {

      /** Returns a new type reference (use `quoted=true` for Unicode support). */
      def apply(sym: TypeSymbol, quoted: Boolean = false): Ident = {
        val id = if (quoted) q"`$sym`".asInstanceOf[Ident] else Ident(sym)
        setType(id, Type of sym)
        setSymbol(id, sym)
      }

      def unapply(id: Ident): Option[TypeSymbol] =
        if (id.isType) Some(Type sym id) else None
    }

    /** Type member selection. */
    object sel {

      /** Returns a new type `member` access (Select). */
      def apply(target: Tree, member: TypeSymbol, tpe: Type = NoType): Select = {
        assert(Has tpe target, s"Untyped target:\n$target")
        assert(member.toString.nonEmpty, "Unspecified type member")
        val sel = Select(target, member)
        val result =
          if (Is defined tpe) tpe
          else member.infoIn(of(target))

        setSymbol(sel, member)
        setType(sel, result)
      }

      def unapply(sel: Select): Option[(Tree, TypeSymbol)] =
        if (Has typeSym sel) Some(sel.qualifier, Type sym sel) else None
    }

    /** Returns the `this` type of a class or singleton object. */
    def this_(sym: Symbol): ThisType = {
      assert(sym.isClass || sym.isModule)
      internal.thisType(sym).asInstanceOf[ThisType]
    }

    /** Returns a new tuple type of specific elements. */
    def tuple(first: Type, rest: Type*): Type = {
      val n = rest.size + 1
      assert(n <= maxTupleElems, s"Cannot have $n > $maxTupleElems tuple elements")
      val constructor = Symbol.tuple(n).toTypeConstructor
      apply(constructor, first +: rest: _*)
    }

    /** Returns a new [[scala.Array]] type of specific elements. */
    def array(elements: Type): Type =
      _1[Array](elements)

    /** Returns a new [[scala.Function]] type with specific arguments and result. */
    def fun(args: Type*)(result: Type): Type = {
      val n = args.size
      assert(n <= maxFunArgs, s"Cannot have $n > $maxFunArgs lambda parameters")
      val constructor = Symbol.fun(n).toTypeConstructor
      apply(constructor, args :+ result: _*)
    }

    /** Applies a nullary type constructor (i.e. a concrete type). */
    def _0[T: TypeTag]: Type =
      fix(typeOf[T])

    /** Applies a unary type constructor. */
    def _1[F[_]](arg: Type)
      (implicit tag: TypeTag[F[Nothing]]): Type = {

      apply(_0[F[Nothing]].typeConstructor, arg)
    }

    /** Applies a binary type constructor. */
    def _2[F[_, _]](arg1: Type, arg2: Type)
      (implicit tag: TypeTag[F[Nothing, Nothing]]): Type = {

      apply(_0[F[Nothing, Nothing]].typeConstructor, arg1, arg2)
    }

    /** Applies a type constructor. */
    def apply(constructor: Type, args: Type*): Type = {
      assert(Is.defined(constructor), s"Undefined type constructor: `$constructor`")
      assert(args.forall(Is.defined), "Undefined type arguments")
      assert(args.isEmpty || constructor.takesTypeArgs,
        s"Type `$constructor` is not a type constructor")
      appliedType(fix(constructor), args.map(fix): _*)
    }

    /** Returns the (concrete) type of `T`. */
    def apply[T: TypeTag]: Type =
      _0[T]

    /** Returns the weak (possibly abstract) type of `T`. */
    def weak[T: WeakTypeTag]: Type =
      fix(weakTypeOf[T])

    /** Returns the type of `tree` (must be type-checked). */
    def of(tree: Tree): Type =
      fix(tree.tpe)

    /** Returns the type of `sym` (must be type-checked). */
    def of(sym: Symbol): Type =
      fix(sym.info)

    def signature(sym: Symbol, in: Type = NoType): Type = {
      assert(Is.defined(sym), s"Undefined type signature of: `$sym`")
      assert(Has.tpe(sym), s"Untyped symbol: `$sym`")
      val tpe = fix(if (Is.defined(in)) sym.typeSignatureIn(in) else sym.typeSignature)
      if (Is(BYNAMEPARAM, sym)) tpe.typeArgs.head else tpe
    }

    def fix(tpe: Type): Type = {
      assert(Is.defined(tpe), s"Undefined type: `$tpe`")
      tpe.dealias.widen
    }

    /** Returns the `i`-th type argument of `tpe`. */
    def arg(i: Int, tpe: Type): Type = {
      assert(Is defined tpe, s"Undefined type: `$tpe`")
      assert(tpe.typeArgs.size >= i, s"Type `$tpe` has no type argument #$i")
      fix(tpe.typeArgs(i - 1))
    }

    /** Returns the `i`-th tyoe argument of `tree`'s type. */
    def arg(i: Int, tree: Tree): Type =
      arg(i, of(tree))

    /** Returns the result type of `tpe` if it's a method or function. */
    def result(tpe: Type): Type = fix(tpe match {
      case _: PolyType | _: MethodType | _: NullaryMethodType =>
        tpe.resultType
      case _ if tpe.typeArgs.nonEmpty =>
        tpe.typeArgs.last
      case _ =>
        warning(pos(tpe), s"Type `$tpe` is not a method or function")
        tpe
    })

    /** Returns the result type of `tree` if it's a method or function. */
    def result(tree: Tree): Type =
      result(of(tree))

    /** Finds type `member` accessible in `target` and returns its symbol. */
    def member(target: Symbol, member: TypeName): TypeSymbol = {
      assert(Is valid target, s"Invalid target: `$target`")
      assert(member.toString.nonEmpty, "Unspecified type member")
      of(target).member(member).asType
    }

    /** Finds type `member` accessible in `target` and returns its symbol. */
    def member(target: Tree, member: TypeName): TypeSymbol = {
      assert(Has tpe target, s"Untyped target:\n$target")
      assert(member.toString.nonEmpty, "Unspecified type member")
      of(target).member(member).asType
    }

    /** Type-checks `tree` (use `typeMode=true` for type trees). */
    def check(tree: Tree, typeMode: Boolean = false): Tree =
      if (Has tpe tree) tree else typeCheck(tree, typeMode)

    /** Un-type-checks `tree` (removes all its attributes). */
    def unCheck(tree: Tree): Tree =
      unTypeCheck(tree)

    /** Type-checks `tree` as if `imports` were in scope. */
    def checkWith(imports: Tree*)(tree: Tree): Tree = check {
      q"{ ..${imports map Tree.impAll}; $tree }"
    }.asInstanceOf[Block].expr

    /** Returns a tree representation of `tpe`. */
    def quote(tpe: Type): TypeTree =
      if (Is defined tpe) TypeTree(fix(tpe))
      else TypeTree()

    /** Returns a tree representation of `sym`'s type. */
    def quote(sym: Symbol): TypeTree =
      if (Has tpe sym) quote(of(sym))
      else TypeTree()

    /** Returns a tree representation of `T`. */
    def quote[T: TypeTag]: TypeTree =
      quote(Type[T])

    /** Imports a type from a tree. */
    def imp(from: Tree, sym: TypeSymbol): Import =
      imp(from, name(sym))

    /** Imports a type from a tree by name. */
    def imp(from: Tree, name: String): Import =
      imp(from, this.name(name))

    /** Imports a type from a tree by name. */
    def imp(from: Tree, name: TypeName): Import = {
      assert(Is valid from, s"Invalid import selector:\n$from")
      assert(name.toString.nonEmpty, "Unspecified import")
      Type.check(q"import $from.$name").asInstanceOf[Import]
    }

    /** Returns the least upper bound of all types. */
    def lub(tpe: Type, types: Type*): Type =
      if (types.isEmpty) tpe
      else universe.lub(tpe :: types.toList)

    /** Returns the least upper bound of the argument types. */
    def lub(tree: Tree, trees: Tree*): Type = {
      assert(Has tpe tree, s"Untyped tree:\n$tree")
      assert(trees forall Has.tpe, "Untyped trees")
      lub(Type of tree, trees map Type.of: _*)
    }

    /** Returns the least upper bound of the argument types. */
    def lub(sym: Symbol, symbols: Symbol*): Type = {
      assert(Has tpe sym, s"Untyped symbol: `$sym`")
      assert(symbols forall Has.tpe, "Untyped symbols")
      lub(Type of sym, symbols map Type.of: _*)
    }

    /** Returns the weak least upper bound of all types. */
    def weakLub(tpe: Type, types: Type*): Type =
      types.fold(tpe) { (T, U) =>
        if (T weak_<:< U) U
        else if (U weak_<:< T) T
        else lub(T, U)
      }

    /** Returns the weak least upper bound of the argument types. */
    def weakLub(tree: Tree, trees: Tree*): Type = {
      assert(Has tpe tree, s"Untyped tree:\n$tree")
      assert(trees forall Has.tpe, "Untyped trees")
      weakLub(Type of tree, trees map Type.of: _*)
    }

    /** Returns the weak least upper bound of the argument types. */
    def weakLub(sym: Symbol, symbols: Symbol*): Type = {
      assert(Has tpe sym, s"Untyped symbol: `$sym`")
      assert(symbols forall Has.tpe, "Untyped symbols")
      weakLub(Type of sym, symbols map Type.of: _*)
    }

    /** Returns the position of `tpe` if any, otherwise `NoPosition`. */
    def pos(tpe: Type): Position = {
      val sym =
        if (Has typeSym tpe) tpe.typeSymbol
        else if (Has termSym tpe) tpe.termSymbol
        else NoSymbol

      if (Has pos sym) sym.pos
      else NoPosition
    }

    /** Type ascriptions. */
    object ascription {

      /** Casts `tree` up to `tpe` (i.e. `tree: tpe`). */
      def apply(tree: Tree, tpe: Type): Typed = {
        assert(Has tpe tree, s"Untyped tree:\n$tree")
        assert(Is defined tpe, s"Undefined type ascription: `$tpe`")
        assert(of(tree) weak_<:< tpe, "Type ascription does not match")
        val typed = Typed(tree, Type quote tpe)
        setType(typed, tpe)
      }

      /** Returns `tree` without type ascriptions (ie. `x: Ascription`). */
      @tailrec
      def remove(tree: Tree): Tree = tree match {
        case Typed(inner, _) => remove(inner)
        case _ => tree
      }

      def unapply(tpd: Typed): Option[(Tree, Type)] =
        Some(tpd.expr, Type of tpd)
    }
  }
}
