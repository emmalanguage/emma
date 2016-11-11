/*
 * Copyright © 2014 TU Berlin (emma@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.emmalanguage
package ast

import scala.language.higherKinds

trait Types { this: AST =>

  trait TypeAPI { this: API =>

    import universe._
    import internal._
    import reificationSupport._

    /** Type names. */
    object TypeName extends Node {

      // Predefined type names
      val empty    = u.typeNames.EMPTY
      val wildcard = u.typeNames.WILDCARD
      val wildStar = u.typeNames.WILDCARD_STAR

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
        assert(has.nme(sym), s"Symbol `$sym` has no name")
        apply(sym.name)
      }

      /** Creates a fresh type name with the given `prefix`. */
      def fresh(prefix: String): u.TypeName = apply {
        assert(prefix.nonEmpty, "Cannot create a fresh name with empty prefix")
        freshTypeName(s"$prefix$$$freshNameSuffix")
      }

      /** Creates a fresh type name with the given `prefix`. */
      def fresh(prefix: u.Name): u.TypeName = {
        assert(is.defined(prefix), s"Undefined prefix `$prefix`")
        fresh(prefix.toString)
      }

      /** Creates a fresh type name with the given symbol's name as `prefix`. */
      def fresh(prefix: u.Symbol): u.TypeName = {
        assert(is.defined(prefix), s"Undefined prefix `$prefix`")
        assert(has.nme(prefix), s"Prefix `$prefix` has no name")
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

      /** Creates a free type symbol (without an owner). */
      def free(name: u.TypeName, flags: u.FlagSet = u.NoFlags): u.FreeTypeSymbol = {
        assert(is.defined(name), s"$this name `$name` is not defined")
        internal.newFreeType(TypeName(name).toString, flags, null)
      }

      /** Creates a free type symbol with the same attributes as the `original`. */
      def free(original: u.TypeSymbol): u.FreeTypeSymbol =
        free(TypeName(original), flags(original))

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
      lazy val any     = u.definitions.AnyTpe
      lazy val nothing = u.definitions.NothingTpe

      // Primitives
      lazy val anyVal = u.definitions.AnyValTpe
      lazy val unit   = u.definitions.UnitTpe
      lazy val bool   = u.definitions.BooleanTpe
      lazy val char   = u.definitions.CharTpe
      lazy val byte   = u.definitions.ByteTpe
      lazy val short  = u.definitions.ShortTpe
      lazy val int    = u.definitions.IntTpe
      lazy val long   = u.definitions.LongTpe
      lazy val float  = u.definitions.FloatTpe
      lazy val double = u.definitions.DoubleTpe

      // Objects
      lazy val anyRef = u.definitions.AnyRefTpe
      lazy val obj    = u.definitions.ObjectTpe
      lazy val null_  = u.definitions.NullTpe
      lazy val string = Type[String]
      lazy val bigInt = Type[BigInt]
      lazy val bigDec = Type[BigDecimal]

      // Java types
      object Java {
        lazy val void   = Type[java.lang.Void]
        lazy val bool   = Type[java.lang.Boolean]
        lazy val char   = Type[java.lang.Character]
        lazy val byte   = Type[java.lang.Byte]
        lazy val short  = Type[java.lang.Short]
        lazy val int    = Type[java.lang.Integer]
        lazy val long   = Type[java.lang.Long]
        lazy val float  = Type[java.lang.Float]
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
          assert(constructor.takesTypeArgs, s"Type `$constructor` is not a type constructor")
          assert(args.forall(is.defined), "Not all type arguments are defined")
          lazy val params = constructor.typeParams
          assert(params.size == args.size, s"Type params <-> args size mismatch for `$constructor`")
          u.appliedType(constructor, args.map(_.dealias.widen): _*)
        }

      /** Reifies a type from a tag. */
      def apply[T: u.TypeTag]: u.Type =
        kind0[T]

      /** Reifies a type from a weak tag. */
      def weak[T: u.WeakTypeTag]: u.Type = u.weakTypeOf[T]

      /** Reifies a type of kind `*`. */
      def kind0[T: u.TypeTag]: u.Type = u.typeOf[T]

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
        assert(encl.isClass || encl.isModule, s"Cannot reference this of `$encl`")
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
        val args = tpe.dealias.widen.typeArgs
        assert(args.size >= i, s"Type `$tpe` has no type argument #$i")
        args(i - 1)
      }

      /** Returns the least upper bound of all types. */
      def lub(types: u.Type*): u.Type =
        u.lub(types.toList)

      /** Returns the weak (considering coercions) least upper bound of all types. */
      def weakLub(types: u.Type*): u.Type =
        if (types.isEmpty) nothing
        else types.reduce { (T, U) =>
          if (T weak_<:< U) U
          else if (U weak_<:< T) T
          else lub(T, U)
        }

      /** Returns a new method type (possibly generic and with multiple arg lists). */
      def method(tparams: u.TypeSymbol*)(paramss: Seq[u.TermSymbol]*)(result: u.Type): u.Type = {
        assert(tparams.forall(is.defined), "Not all method type params are defined")
        assert(paramss.flatten.forall(is.defined), "Not all method param types are defined")
        assert(is.defined(result), s"Undefined method return type `$result`")

        val returnT = result.dealias.widen
        val methodT = if (paramss.isEmpty) {
          nullaryMethodType(returnT)
        } else paramss.foldRight(returnT) { (params, ret) =>
          methodType(params.toList, ret)
        }

        if (tparams.isEmpty) methodT
        else polyType(tparams.toList, methodT)
      }

      /** Extracts the result type of `tpe` if it's a lambda or a method type. */
      def result(tpe: u.Type): u.Type = {
        val wide = tpe.dealias.widen
        if (Sym.funs(wide.typeSymbol)) wide.typeArgs.last
        else wide.resultType
      }

      /** Extracts the type signature of `sym` (with an optional target), if any. */
      def signature(sym: u.Symbol, in: u.Type = u.NoType): u.Type = {
        assert(is.defined(sym), s"Symbol $sym is not defined")
        assert(has.tpe(sym),    s"Symbol $sym has no type signature")
        val sign = if (is.defined(in)) sym.typeSignatureIn(in) else sym.typeSignature
        if (is.byName(sym)) sign.typeArgs.head else sign
      }

      /** Returns the type constructor of an applied type `tpe`. */
      def constructor(tpe: u.Type): u.Type =
        tpe.dealias.widen.typeConstructor

      /** Returns the original type-tree corresponding to `tpe`. */
      def tree(tpe: u.Type): u.Tree = {
        def original(tpe: u.Type): u.Tree = setType(tpe match {
          // Degenerate type: `this[staticID]`.
          case u.ThisType(sym) if sym.isStatic =>
            api.Tree.resolveStatic(sym)
          // This type: `this[T]`.
          case u.ThisType(sym) =>
            api.This(sym)
          // Super type: `this.super[T]`
          case u.SuperType(ths, sup) =>
            val sym = sup.typeSymbol.asType
            setSymbol(u.Super(original(ths), sym.name), sym)
          // Package or class ref: `package` or `Class`.
          case u.SingleType(u.NoPrefix, target)
            if target.isPackage || target.isClass
            => api.Id(target)
          // Singleton type: `stableID.tpe`.
          case u.SingleType(u.NoPrefix, stableID) =>
            u.SingletonTypeTree(api.Id(stableID))
          // Qualified type: `pkg.T`.
          case u.SingleType(pkg, target) =>
            api.Sel(original(pkg), target)
          // Abstract type ref: `T`.
          case u.TypeRef(u.NoPrefix, target, Nil) =>
            api.Id(target)
          // Path dependent type: `path.T`.
          case u.TypeRef(path, target, Nil) =>
            api.Sel(original(path), target)
          // Applied type: `T[A, B, ...]`.
          case u.TypeRef(u.NoPrefix, target, args) =>
            u.AppliedTypeTree(api.Id(target), args.map(Type.tree))
          // Applied path dependent type: `path.T[A, B, ...]`
          case u.TypeRef(path, target, args) =>
            u.AppliedTypeTree(api.Sel(original(path), target), args.map(Type.tree))
          // Type bounds: `T >: lo <: hi`.
          case u.TypeBounds(lo, hi) =>
            u.TypeBoundsTree(Type.tree(lo), Type.tree(hi))
          // Existential type: `F[A, B, ...] forSome { type A; type B; ... }`
          case u.ExistentialType(quantified, underlying) =>
            u.ExistentialTypeTree(Type.tree(underlying), quantified.map(internal.typeDef))
          // Annotated type: `A @ann1 @ann2 ...`
          case AnnotatedType(annotations, underlying) =>
            annotations.foldLeft(original(underlying))((res, ann) =>
              u.Annotated(ann.tree, res)
            )
          // E.g. type refinement: `T { def size: Int }`
          case _ =>
            abort(s"Cannot convert type `$tpe` to a type-tree")
        }, tpe)
        setOriginal(u.TypeTree(tpe), original(tpe))
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
        u.TypeTree(tpe)
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

    /** By-name types (`=> T`), legal only in parameter declarations. */
    // TODO: Define a constructor?
    object ByNameType {

      val sym: u.ClassSymbol = u.definitions.ByNameParamClass

      def unapply(tpe: u.TypeRef): Option[u.Type] = tpe match {
        case u.TypeRef(_, `sym`, Seq(arg)) => Some(arg)
        case _ => None
      }
    }

    /** Vararg types (`T*`), legal only in parameter declarations. */
    // TODO: Define a constructor?
    object VarArgType {

      val scalaSym: u.ClassSymbol = u.definitions.RepeatedParamClass
      val javaSym: u.ClassSymbol = u.definitions.JavaRepeatedParamClass

      def unapply(tpe: u.TypeRef): Option[u.Type] = tpe match {
        case u.TypeRef(_, `scalaSym`, Seq(arg)) => Some(arg)
        case u.TypeRef(_, `javaSym`, Seq(arg)) => Some(arg)
        case _ => None
      }
    }
  }
}
