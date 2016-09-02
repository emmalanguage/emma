package eu.stratosphere.emma
package ast

import scala.annotation.tailrec
import scala.language.higherKinds

trait Types { this: AST =>

  trait TypeAPI { this: API =>

    import u.Flag._
    import u.internal._
    import reificationSupport.freshTypeName
    import universe._

    /** Type names. */
    object TypeName extends Node {

      // Predefined type names
      val empty = u.TypeName("")

      /** Creates a new type name (must be non-empty). */
      def apply(name: String): u.TypeName = {
        assert(name.nonEmpty, "Empty type name")
        apply(u.TypeName(name))
      }

      /** Encodes `name` and converts it to a type name. */
      def apply(name: u.Name): u.TypeName = {
        assert(is.defined(name), s"Undefined name `$name`")
        name.encodedName.toTypeName
      }

      /** Extracts the type name of `sym`, if any. */
      def apply(sym: u.Symbol): u.TypeName = {
        assert(is.defined(sym), s"Undefined symbol `$sym`")
        assert(has.name(sym), s"Symbol `$sym` has no name")
        apply(sym.name)
      }

      /** Creates a fresh type name with the given `prefix`. */
      def fresh(prefix: String): u.TypeName = apply {
        if (prefix.nonEmpty && prefix.last == '$') freshTypeName(prefix)
        else freshTypeName(s"$prefix$$")
      }

      /** Creates a fresh type name with the given `prefix`. */
      def fresh(prefix: u.Name): u.TypeName = {
        assert(is.defined(prefix), s"Undefined prefix `$prefix`")
        fresh(prefix.toString)
      }

      /** Creates a fresh type name with the given symbol's name as `prefix`. */
      def fresh(prefix: u.Symbol): u.TypeName = {
        assert(is.defined(prefix), s"Undefined prefix `$prefix`")
        assert(has.name(prefix), s"Prefix `$prefix` has no name")
        fresh(prefix.name)
      }

      def unapply(name: u.TypeName): Option[String] =
        for (name <- Option(name) if is.defined(name)) yield name.toString
    }

    /** Type symbols. */
    object TypeSym extends Node {

      /**
       * Creates a new type symbol.
       * @param owner The symbol of the enclosing named entity where this type is defined.
       * @param name The name of this type (will be encoded).
       * @param flags Any additional modifiers (e.g. deferred).
       * @param pos The (optional) source code position where this type is defined.
       * @return A new type symbol.
       */
      def apply(owner: u.Symbol, name: u.TypeName,
        flags: u.FlagSet = u.NoFlags,
        pos: u.Position = u.NoPosition): u.TypeSymbol = {

        assert(is.defined(name), s"$this name `$name` is not defined")
        newTypeSymbol(owner, TypeName(name), pos, flags)
      }

      /** Extracts the type symbol of `tree`, if any. */
      def of[T <: u.Tree](tree: T): u.TypeSymbol = {
        val sym = Sym.of(tree)
        assert(is.tpe(sym), s"Symbol `$sym` is not a type")
        sym.asType
      }

      /** Creates a free type symbol (without an owner). */
      def free(name: u.TypeName, flags: u.FlagSet = u.NoFlags): u.FreeTypeSymbol = {
        assert(is.defined(name), s"$this name `$name` is not defined")
        newFreeType(TypeName(name).toString, flags, null)
      }

      /** Creates a free type symbol with the same attributes as the `original`. */
      def free(original: u.TypeSymbol): u.FreeTypeSymbol =
        free(TypeName(original), get.flags(original))

      /** Creates a fresh type symbol with the same attributes as the `original`. */
      def fresh(original: u.TypeSymbol): u.TypeSymbol =
        Sym.copy(original)(name = TypeName.fresh(original)).asType

      def unapply(sym: u.TypeSymbol): Option[u.TypeSymbol] =
        Option(sym)
    }

    object Type extends Node {

      // ------------------
      // Predefined types
      // ------------------

      // Top and bottom
      lazy val any = u.definitions.AnyTpe
      lazy val nothing = u.definitions.NothingTpe

      // Primitives
      lazy val anyVal = u.definitions.AnyValTpe
      lazy val unit = u.definitions.UnitTpe
      lazy val bool = u.definitions.BooleanTpe
      lazy val char = u.definitions.CharTpe
      lazy val byte = u.definitions.ByteTpe
      lazy val short = u.definitions.ShortTpe
      lazy val int = u.definitions.IntTpe
      lazy val long = u.definitions.LongTpe
      lazy val float = u.definitions.FloatTpe
      lazy val double = u.definitions.DoubleTpe

      // Objects
      lazy val anyRef = u.definitions.AnyRefTpe
      lazy val obj = u.definitions.ObjectTpe
      lazy val null_ = u.definitions.NullTpe
      lazy val string = Type[String]
      lazy val bigInt = Type[BigInt]
      lazy val bigDec = Type[BigDecimal]

      // Java types
      object Java {
        lazy val void = Type[java.lang.Void]
        lazy val bool = Type[java.lang.Boolean]
        lazy val char = Type[java.lang.Character]
        lazy val byte = Type[java.lang.Byte]
        lazy val short = Type[java.lang.Short]
        lazy val int = Type[java.lang.Integer]
        lazy val long = Type[java.lang.Long]
        lazy val float = Type[java.lang.Float]
        lazy val double = Type[java.lang.Double]
        lazy val bigInt = Type[java.math.BigInteger]
        lazy val bigDec = Type[java.math.BigDecimal]
      }

      // Other
      lazy val loop = Type.method()(Seq.empty)(unit)

      /** Applies a type `constructor` to the supplied arguments. */
      def apply(constructor: u.Type, args: u.Type*): u.Type =
        if (args.isEmpty) constructor else {
          assert(is.defined(constructor), s"Type constructor `$constructor` is not defined")
          assert(is.poly(constructor), s"Type `$constructor` is not a type constructor")
          assert(args.forall(is.defined), "Not all type arguments are defined")
          lazy val params = constructor.typeParams
          assert(params.size == args.size, s"Type params <-> args size mismatch for `$constructor`")
          u.appliedType(fix(constructor), args.map(fix): _*)
        }

      /** Reifies a type from a tag. */
      def apply[T: u.TypeTag]: u.Type =
        kind0[T]

      /** Reifies a type from a weak tag. */
      def weak[T: u.WeakTypeTag]: u.Type =
        fix(u.weakTypeOf[T])

      /** Reifies a type of kind `*`. */
      def kind0[T: u.TypeTag]: u.Type =
        fix(u.typeOf[T])

      /** Reifies a type of kind `* -> *`. */
      def kind1[F[_]](arg: u.Type)
        (implicit tag: u.TypeTag[F[Nothing]]): u.Type
        = apply(apply(tag).typeConstructor, arg)

      /** Reifies a type of kind `* -> * -> *`. */
      def kind2[F[_, _]](arg1: u.Type, arg2: u.Type)
        (implicit tag: u.TypeTag[F[Nothing, Nothing]]): u.Type
        = apply(apply(tag).typeConstructor, arg1, arg2)

      /** Reifies a type of kind `* -> * -> * -> *`. */
      def kind3[F[_, _, _]](arg1: u.Type, arg2: u.Type, arg3: u.Type)
        (implicit tag: u.TypeTag[F[Nothing, Nothing, Nothing]]): u.Type
        = apply(apply(tag).typeConstructor, arg1, arg2, arg3)

      /** Creates a new array type. */
      def arrayOf(elements: u.Type): u.Type =
        kind1[Array](elements)

      /** Creates a new function (lambda) type. */
      def fun(params: u.Type*)(result: u.Type): u.Type = {
        val n = params.size
        assert(n <= Max.FunParams, s"Cannot have $n > ${Max.FunParams} lambda parameters")
        apply(Sym.fun(n).toTypeConstructor, params :+ result: _*)
      }

      /** Creates a `this` type of an enclosing class or module. */
      def thisOf(encl: u.Symbol): u.ThisType = {
        assert(is.clazz(encl) || is.module(encl), s"Cannot reference this of `$encl`")
        thisType(encl).asInstanceOf[u.ThisType]
      }

      /** Creates a new tuple type. */
      def tupleOf(first: u.Type, rest: u.Type*): u.Type = {
        val n = rest.size + 1
        assert(n <= Max.TupleElems, s"Cannot have $n > ${Max.TupleElems} tuple elements")
        apply(Sym.tuple(n).toTypeConstructor, first +: rest: _*)
      }

      /** Extracts the i-th (1-based) type argument of the applied type `tpe`. */
      def arg(i: Int, tpe: u.Type): u.Type = {
        assert(is.defined(tpe), s"Undefined type `$tpe`")
        val args = tpe.typeArgs
        assert(args.size >= i, s"Type `$tpe` has no type argument #$i")
        fix(args(i - 1))
      }


      /** Type-checks a `tree` (use `typeMode=true` for type-trees). */
      def check(tree: u.Tree, typeMode: Boolean = false): u.Tree = {
        assert(is.defined(tree), s"Cannot type-check undefined tree: $tree")
        assert(!has.tpe(tree), s"Tree is already type-checked:\n${Tree.showTypes(tree)}")
        typeCheck(tree, typeMode)
      }

      /** Type-checks a `tree` as if `imports` were in scope. */
      def checkWith(imports: u.Tree*)(tree: u.Tree): u.Tree = {
        assert(are.defined(imports), "Not all imports are defined")
        assert(is.defined(tree), s"Cannot type-check undefined tree: $tree")
        assert(!has.tpe(tree), s"Tree already type-checked:\n${Tree.showTypes(tree)}")
        check(u.Block(imports.toList, tree)).asInstanceOf[u.Block].expr
      }

      /** Removes all type and symbol attributes from a `tree`. */
      def unCheck(tree: u.Tree): u.Tree = {
        assert(is.defined(tree), s"Cannot un-type-check undefined tree: $tree")
        assert(has.tpe(tree), s"Untyped tree:\n${Tree.showTypes(tree)}")
        unTypeCheck(tree)
      }

      /** Equivalent to `tpe.dealias.widen`. */
      def fix(tpe: u.Type): u.Type = {
        assert(is.defined(tpe), s"Undefined type `$tpe`")
        tpe.dealias.widen
      }

      /** Returns the least upper bound of all types. */
      def lub(types: u.Type*): u.Type =
        u.lub(types.map(fix).toList)

      /** Returns the weak (considering coercions) least upper bound of all types. */
      def weakLub(types: u.Type*): u.Type =
        if (types.isEmpty) nothing
        else types.reduce { (T, U) =>
          if (T weak_<:< U) U
          else if (U weak_<:< T) T
          else lub(T, U)
        }

      /** Looks for `member` in `target` and returns its symbol (possibly overloaded). */
      def member(target: u.Type, member: u.TypeName): u.TypeSymbol = {
        assert(is.defined(target), s"Undefined target `$target`")
        assert(is.defined(member), s"Undefined type member `$member`")
        fix(target).member(member).asType
      }

      /** Looks for `member` in `target` and returns its symbol (possibly overloaded). */
      def member(target: u.Symbol, member: u.TypeName): u.TypeSymbol = {
        assert(is.defined(target), s"Undefined target `$target`")
        assert(has.tpe(target), s"Target `$target` has no type")
        Type.member(target.info, member)
      }

      /** Returns a new method type (possibly generic and with multiple arg lists). */
      def method(tparams: u.TypeSymbol*)(paramss: Seq[u.TermSymbol]*)(result: u.Type): u.Type = {
        assert(tparams.forall(is.defined), "Not all method type params are defined")
        assert(paramss.flatten.forall(is.defined), "Not all method param types are defined")
        assert(is.defined(result), s"Undefined method return type `$result`")

        val returnT = fix(result)
        val methodT = if (paramss.isEmpty) {
          nullaryMethodType(returnT)
        } else paramss.foldRight(returnT) { (params, ret) =>
          methodType(params.toList, ret)
        }

        if (tparams.isEmpty) methodT
        else polyType(tparams.toList, methodT)
      }

      /** Extracts the type of `tree`, if any. */
      def of(tree: u.Tree): u.Type =
        fix(tree.tpe)

      /** Extracts the type of `sym` (with an optional target), if any. */
      def of(sym: u.Symbol, in: u.Type = u.NoType): u.Type =
        fix(if (is.defined(in)) sym.infoIn(in) else sym.info)

      /** Extracts the result type of `tpe` if it's a lambda or a method type. */
      def result(tpe: u.Type): u.Type = fix(tpe match {
        case _ if has.typeSym(tpe) && Sym.funs(tpe.typeSymbol) => tpe.typeArgs.last
        case _ => tpe.resultType
      })

      /** Extracts the final result type of `tpe` if it's a lambda or a method type. */
      @tailrec
      def finalResult(tpe: u.Type): u.Type = tpe match {
        case _ if has.typeSym(tpe) && Sym.funs(tpe.typeSymbol) => finalResult(tpe.typeArgs.last)
        case _ => fix(tpe.finalResultType)
      }

      /** Extracts the type signature of `sym` (with an optional target), if any. */
      def signature(sym: u.Symbol, in: u.Type = u.NoType): u.Type = {
        assert(is.defined(sym), s"Symbol `$sym` is not defined")
        assert(has.tpe(sym), s"Symbol `$sym` has no type")
        val sign = fix(if (is.defined(in)) sym.typeSignatureIn(in) else sym.typeSignature)
        if (is(BYNAMEPARAM)(sym)) sign.typeArgs.head else sign
      }

      /** Infers an implicit value from the enclosing context (if possible). */
      def inferImplicit(tpe: u.Type): Option[u.Tree] = {
        val opt = Option(Types.this.inferImplicit(tpe)).filter(is.defined)
        for (value withType NullaryMethodType(result) <- opt) set.tpe(value, result)
        opt
      }

      /** Extractor for result types (legal for terms). */
      private[ast] object Result extends Node {
        def unapply(tpe: u.Type): Option[u.Type] =
          Option(tpe).filter(is.result)
      }
    }

    /** Quoted type-trees. */
    object TypeQuote extends Node {

      /** Reifies `tpe` as a tree. */
      def apply(tpe: u.Type): u.TypeTree = {
        assert(is.defined(tpe), s"$this type `$tpe` is not defined")
        u.TypeTree(Type.fix(tpe))
      }

      /** Reifies `sym`'s type as a tree. */
      def apply(sym: u.Symbol): u.TypeTree = {
        assert(is.defined(sym), s"$this symbol `$sym` is not defined")
        assert(has.tpe(sym), s"$this symbol `$sym` has no type")
        apply(sym.info)
      }

      /** Reifies type `T` as a tree. */
      def apply[T: u.TypeTag]: u.TypeTree =
        apply(Type[T])

      def unapply(tree: u.Tree): Option[u.Type] = tree match {
        case _ withType tpe if tree.isType => Some(tpe)
        case _ => None
      }
    }
  }
}
