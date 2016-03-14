package eu.stratosphere.emma
package compiler

import scala.language.higherKinds

/** Utility for types (depends on [[Trees]] and [[Symbols]]). */
trait Types extends Util { this: Trees with Symbols =>

  import universe._
  import internal.reificationSupport._
  import Const._

  /** Utility for [[Type]]s. */
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

    /** Returns a new tuple [[Type]] with specified elements. */
    def tuple(arg: Type, args: Type*): Type = {
      // Pre-conditions
      val n = args.size + 1
      require(n <= maxTupleElems,
        s"Tuples can't have $n > $maxTupleElems elements")

      val constructor = Symbol.tuple(n).toTypeConstructor
      apply(constructor, arg +: args: _*)
    }

    /** Returns a new [[Array]] [[Type]] with specified elements. */
    def array(elements: Type): Type =
      _1[Array](elements)

    /** Returns a new [[Function]] [[Type]] with specified arguments and result. */
    def fun(args: Type*)(result: Type): Type = {
      // Pre-conditions
      val n = args.size
      require(n <= maxFunArgs,
        s"Functions can't have $n > $maxFunArgs arguments")

      val constructor = Symbol.fun(n).toTypeConstructor
      apply(constructor, args :+ result: _*)
    }

    /** Applies a nullary [[Type]] constructor. */
    def _0[T: TypeTag]: Type =
      fix(typeOf[T])

    /** Applies a unary [[Type]] constructor. */
    def _1[F[_]](arg: Type)
      (implicit tag: TypeTag[F[Nothing]]): Type = {

      apply(_0[F[Nothing]].typeConstructor, arg)
    }

    /** Applies a binary [[Type]] constructor. */
    def _2[F[_, _]](arg1: Type, arg2: Type)
      (implicit tag: TypeTag[F[Nothing, Nothing]]): Type = {

      apply(_0[F[Nothing, Nothing]].typeConstructor, arg1, arg2)
    }

    /** Returns the [[TypeName]] of `sym`. */
    def name(sym: Symbol): TypeName = {
      // Pre-conditions
      Symbol.verify(sym)

      sym.name.toTypeName
    }

    /** Returns a new [[TypeName]]. */
    def name(name: String): TypeName = {
      // Pre-conditions
      verify(name)

      TypeName(name)
    }

    /** Returns a fresh [[TypeName]] starting with `prefix`. */
    def fresh(prefix: String): TypeName =
      freshType(prefix)

    /** Returns a free [[TypeSymbol]] with specified attributes. */
    def free(name: String,
      flags: FlagSet = Flag.SYNTHETIC,
      origin: String = null): FreeTypeSymbol = {

      // Pre-conditions
      verify(name)

      newFreeType(name, flags, origin)
    }

    /** Returns a new [[TypeSymbol]] with specified attributes. */
    def sym(owner: Symbol, name: TypeName,
      flags: FlagSet = Flag.SYNTHETIC,
      pos: Position = NoPosition): TypeSymbol = {

      // Pre-conditions
      verify(name)

      typeSymbol(owner, name, flags, pos)
    }

    /** Applies a [[Type]] constructor. */
    def apply(constructor: Type, args: Type*): Type = {
      // Pre-conditions
      Type.verify(constructor)
      args.foreach(Type.verify)
      require(constructor.takesTypeArgs,
        s"Type `$constructor` is not a type constructor")

      appliedType(fix(constructor), args.map(fix): _*)
    }

    /** Returns the (concrete) [[Type]] of `T`. */
    def apply[T: TypeTag]: Type =
      _0[T]

    /** Returns the weak (possibly abstract) [[Type]] of `T`. */
    def weak[T: WeakTypeTag]: Type =
      fix(weakTypeOf[T])

    /** Returns the [[Type]] of `tree` (must be type-checked). */
    def of(tree: Tree): Type = {
      // Pre-conditions
      Tree.verify(tree)

      fix(tree.tpe)
    }

    /** Returns the [[Type]] of `sym` (must be type-checked). */
    def of(sym: Symbol): Type = {
      // Pre-conditions
      Symbol.verify(sym)

      fix(sym.info)
    }

    /** Returns the [[TypeSymbol]] of `tree` (must be type-checked). */
    def symOf(tree: Tree): TypeSymbol = {
      // Pre-conditions
      Tree.verify(tree)
      require(Has.typeSym(tree))

      tree.symbol.asType
    }

    /** Equivalent to `tpe.dealias.widen`. */
    def fix(tpe: Type): Type = {
      // Pre-conditions
      verify(tpe)

      tpe.dealias.widen
    }

    /** Returns `true` if `tpe` is not degenerate. */
    def isDefined(tpe: Type): Boolean =
      tpe != null && tpe != NoType

    /** Returns the `i`-th [[Type]] argument of `tpe`. */
    def arg(i: Int, tpe: Type): Type = {
      // Pre-conditions
      verify(tpe)
      require(tpe.typeArgs.size >= i,
        s"Type `$tpe` doesn't have type argument $i")

      fix(tpe.typeArgs(i - 1))
    }

    /** Returns the `i`-th [[Type]] argument of `tree`'s type. */
    def arg(i: Int, tree: Tree): Type =
      arg(i, of(tree))

    /** Returns the return [[Type]] of `tpe` if it's a method or function. */
    def result(tpe: Type): Type = fix {
      tpe match {
        case _: PolyType | _: MethodType | _: NullaryMethodType =>
          tpe.finalResultType
        case _ if tpe.typeArgs.nonEmpty =>
          tpe.typeArgs.last
        case _ =>
          warning(NoPosition, s"`$tpe` doesn't have a return type")
          tpe
      }
    }

    /** Returns the return [[Type]] of `tree` if it's a method or function. */
    def result(tree: Tree): Type =
      result(of(tree))

    /** Finds `member` declared in `target` and returns it's [[TypeSymbol]]. */
    def decl(target: Symbol, member: TypeName): TypeSymbol = {
      // Pre-conditions
      Symbol.verify(target)
      verify(member)

      of(target).decl(member).asType
    }

    /** Finds `member` declared in `target` and returns it's [[TypeSymbol]]. */
    def decl(target: Tree, member: TypeName): TypeSymbol = {
      // Pre-conditions
      Tree.verify(target)
      verify(member)

      of(target).decl(member).asType
    }

    /** Is `tree` type-checked? */
    def isChecked(tree: Tree): Boolean =
      tree.forAll(t => !Has.term(t) || Has.tpe(t.symbol))

    /** Type-checks `tree` (use `typeMode=true` for [[TypeTree]]s). */
    def check(tree: Tree, typeMode: Boolean = false): Tree =
      if (Has.tpe(tree)) tree
      else typeCheck(tree, typeMode)

    /** Un-type-checks `tree` (removes all its attributes). */
    def unCheck(tree: Tree): Tree =
      unTypeCheck(tree)

    /** Type-checks `tree` as if `imports` were in scope. */
    def checkWith(imports: Tree*)(tree: Tree): Tree = check {
      q"{ ..${imports.map(Tree.impAll)}; $tree }"
    }.asInstanceOf[Block].expr

    /** Returns a [[Tree]] representation of `tpe`. */
    def quote(tpe: Type): TypeTree =
      if (isDefined(tpe)) TypeTree(fix(tpe))
      else TypeTree()

    /** Returns a [[Tree]] representation of `sym`'s [[Type]]. */
    def quote(sym: Symbol): TypeTree =
      if (Has.tpe(sym)) quote(of(sym))
      else TypeTree()

    /** Returns a [[Tree]] representation of `T`. */
    def quote[T: TypeTag]: TypeTree =
      quote(Type[T])

    /** Imports a [[Type]] from a [[Tree]]. */
    def imp(from: Tree, sym: TypeSymbol): Import =
      imp(from, name(sym))

    /** Imports a [[Type]] from a [[Tree]] by name. */
    def imp(from: Tree, name: String): Import =
      imp(from, this.name(name))

    /** Imports a [[Type]] from a [[Tree]] by name. */
    def imp(from: Tree, name: TypeName): Import = {
      // Pre-conditions
      Tree.verify(from)
      verify(name)

      check {
        q"import $from.$name"
      }.asInstanceOf[Import]
    }

    /** Checks common pre-conditions for [[Type]]s. */
    @inline
    def verify(tpe: Type): Unit =
      require(isDefined(tpe), "Undefined type")

    /** Checks common pre-conditions for [[TypeName]]s. */
    @inline
    def verify(name: TypeName): Unit =
      verify(name.toString)

    /** Checks common pre-conditions for [[Type]]s. */
    @inline
    def verify(name: String): Unit =
      require(name.nonEmpty, "Empty type type")
  }
}
