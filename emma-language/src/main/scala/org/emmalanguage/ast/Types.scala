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

import scala.collection.breakOut
import scala.collection.immutable.ListMap
import scala.language.higherKinds

trait Types { this: AST =>

  trait TypeAPI { this: API =>

    import u._
    import definitions._
    import internal._
    import reificationSupport._

    /** Type names. */
    object TypeName extends Node {

      private val regex = s"(.*)\\$$$freshNameSuffix(\\d+)".r

      // Predefined type names
      lazy val empty    = u.typeNames.EMPTY
      lazy val wildcard = u.typeNames.WILDCARD
      lazy val wildStar = u.typeNames.WILDCARD_STAR

      /** Creates a new type name (must be non-empty). */
      def apply(name: String): u.TypeName = {
        assert(name.nonEmpty, "Empty type name")
        apply(u.TypeName(name))
      }

      /** Encodes `name` and converts it to a type name. */
      def apply(name: u.Name): u.TypeName = {
        assert(is.defined(name), "Undefined name")
        name.encodedName.toTypeName
      }

      /** Extracts the type name of `sym`, if any. */
      def apply(sym: u.Symbol): u.TypeName = {
        assert(is.defined(sym), "Undefined symbol")
        assert(has.nme(sym),   s"Symbol $sym has no name")
        apply(sym.name)
      }

      /** Creates a fresh type name with the given `prefix`. */
      def fresh(prefix: String = "T"): u.TypeName = apply {
        assert(prefix.nonEmpty, "Cannot create a fresh name with empty prefix")
        freshTypeName(s"$prefix$$$freshNameSuffix")
      }

      /** Creates a fresh type name with the given `prefix`. */
      def fresh(prefix: u.Name): u.TypeName =
        if (is.defined(prefix)) fresh(prefix.toString) else fresh()

      /** Creates a fresh type name with the given symbol's name as `prefix`. */
      def fresh(prefix: u.Symbol): u.TypeName =
        if (is.defined(prefix)) fresh(prefix.name) else fresh()

      /** Tries to return the original name used to create this `fresh` name. */
      def original(fresh: u.Name): (u.TypeName, Int) = fresh match {
        case u.TypeName(regex(original, i)) => u.TypeName(original) -> i.toInt
        case _ => fresh.toTypeName -> 0
      }

      def unapply(name: u.TypeName): Option[String] =
        for (name <- Option(name) if is.defined(name)) yield name.toString
    }

    /** Type symbols. */
    object TypeSym extends Node {

      /**
       * Creates a type-checked type symbol.
       * @param own The symbol of the enclosing named entity where this type is defined.
       * @param nme The name of this type (will be encoded).
       * @param flg Any (optional) modifiers (e.g. final, abstract).
       * @param pos The (optional) source code position where this type is defined.
       * @param ans Any (optional) annotations associated with this type.
       * @return A new type symbol.
       */
      def apply(own: u.Symbol, nme: u.TypeName,
        flg: u.FlagSet         = u.NoFlags,
        pos: u.Position        = u.NoPosition,
        ans: Seq[u.Annotation] = Seq.empty
      ): u.TypeSymbol = {
        assert(is.defined(nme), s"$this name is not defined")
        val sym = newTypeSymbol(own, TypeName(nme), pos, flg)
        setAnnotations(sym, ans.toList)
      }

      /** Creates a free type symbol (without an owner). */
      def free(name: u.TypeName,
        flg: u.FlagSet         = u.NoFlags,
        ans: Seq[u.Annotation] = Seq.empty
      ): u.FreeTypeSymbol = {
        assert(is.defined(name), s"$this name is not defined")
        val free = internal.newFreeType(TypeName(name).toString, flg, null)
        setAnnotations(free, ans.toList)
      }

      /** Creates a free type symbol with the same attributes as the `original`. */
      def free(original: u.TypeSymbol): u.FreeTypeSymbol =
        free(TypeName(original), flags(original), original.annotations)

      /** Creates a fresh type symbol with the same attributes as the `original`. */
      def fresh(original: u.TypeSymbol): u.TypeSymbol =
        Sym.With(original)(nme = TypeName.fresh(original)).asType

      def unapply(sym: u.TypeSymbol): Option[u.TypeSymbol] =
        Option(sym)
    }

    object Type extends Node {

      // ------------------
      // Predefined types
      // ------------------

      // Top and bottom
      lazy val any     = AnyTpe
      lazy val nothing = NothingTpe

      // Primitives
      lazy val anyVal = AnyValTpe
      lazy val unit   = UnitTpe
      lazy val bool   = BooleanTpe
      lazy val char   = CharTpe
      lazy val byte   = ByteTpe
      lazy val short  = ShortTpe
      lazy val int    = IntTpe
      lazy val long   = LongTpe
      lazy val float  = FloatTpe
      lazy val double = DoubleTpe

      // Objects
      lazy val anyRef = AnyRefTpe
      lazy val obj    = ObjectTpe
      lazy val null_  = NullTpe
      lazy val string = Type[String]
      lazy val bigInt = Type[BigInt]
      lazy val bigDec = Type[BigDecimal]

      // Type constructors
      lazy val option = Type[Option[Any]].typeConstructor
      lazy val seq    = Type[Seq[Any]].typeConstructor
      lazy val array  = Type[Array[Any]].typeConstructor

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
        lazy val string = Type[java.lang.String]
        lazy val bigInt = Type[java.math.BigInteger]
        lazy val bigDec = Type[java.math.BigDecimal]
      }

      // Other
      lazy val none = u.NoType
      lazy val loop = Type.method(pss = Seq(Seq.empty))

      /** Applies a type `constructor` to the supplied arguments. */
      def apply(constructor: u.Type, args: Seq[u.Type]): u.Type =
        if (args.isEmpty) constructor else {
          assert(is.defined(constructor),    "Type constructor is not defined")
          assert(constructor.takesTypeArgs, s"Type $constructor takes no type arguments")
          assert(args.forall(is.defined),    "Not all type arguments are defined")
          assert(constructor.typeParams.size == args.size,
            s"Type params <-> args size mismatch for $constructor")
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
        = apply(apply(tag).typeConstructor, Seq(arg))

      /** Reifies a type of kind `* -> * -> *`. */
      def kind2[F[_, _]](arg1: u.Type, arg2: u.Type)
        (implicit tag: u.TypeTag[F[Nothing, Nothing]]): u.Type
        = apply(apply(tag).typeConstructor, Seq(arg1, arg2))

      /** Reifies a type of kind `* -> * -> * -> *`. */
      def kind3[F[_, _, _]](arg1: u.Type, arg2: u.Type, arg3: u.Type)
        (implicit tag: u.TypeTag[F[Nothing, Nothing, Nothing]]): u.Type
        = apply(apply(tag).typeConstructor, Seq(arg1, arg2, arg3))

      /** Creates a new array type. */
      def arrayOf(elements: u.Type): u.Type =
        kind1[Array](elements)

      /** Creates a new option type. */
      def optionOf(element: u.Type): u.Type =
        kind1[Option](element)

      /** Creates a new function (lambda) type. */
      def fun(params: Seq[u.Type] = Seq.empty, result: u.Type = Type.unit): u.Type = {
        val n = params.size
        assert(n <= Max.FunParams, s"Cannot have $n > ${Max.FunParams} lambda parameters")
        apply(Sym.fun(n).toTypeConstructor, params :+ result)
      }

      /** Creates a new tuple type. */
      def tupleOf(elements: Seq[u.Type]): u.Type = {
        val n = elements.size
        assert(n <= Max.TupleElems, s"Cannot have $n > ${Max.TupleElems} tuple elements")
        apply(Sym.tuple(n).toTypeConstructor, elements)
      }

      /** Extracts the i-th (1-based) type argument of the applied type `tpe`. */
      def arg(i: Int, tpe: u.Type): u.Type = {
        assert(is.defined(tpe), "Undefined type")
        val args = tpe.dealias.widen.typeArgs
        assert(args.size >= i, s"Type $tpe has no type argument #$i")
        args(i - 1)
      }

      /** Returns the least upper bound of all types. */
      def lub(types: Seq[u.Type]): u.Type =
        u.lub(types.toList)

      /** Returns the weak (considering coercions) least upper bound of all types. */
      def weakLub(types: Seq[u.Type]): u.Type =
        if (types.isEmpty) nothing
        else types.reduce { (T, U) =>
          if (T weak_<:< U) U
          else if (U weak_<:< T) T
          else lub(Seq(T, U))
        }

      /** Returns a new method type (possibly generic and with multiple arg lists). */
      def method(
        tps: Seq[u.TypeSymbol]      = Seq.empty,
        pss: Seq[Seq[u.TermSymbol]] = Seq.empty,
        res:  u.Type                = Type.unit
      ): u.Type = {
        assert(tps.forall(is.defined),         "Not all method type params are defined")
        assert(pss.flatten.forall(is.defined), "Not all method param types are defined")
        assert(is.defined(res),                "Undefined method return type")
        val mono = if (pss.isEmpty) {
          nullaryMethodType(res.dealias.widen)
        } else pss.foldRight(res.dealias.widen) {
          (params, ret) => methodType(params.toList, ret)
        }

        if (tps.isEmpty) mono
        else polyType(tps.toList, mono)
      }

      /** Extracts the type signature of `sym` (with an optional target), if any. */
      def signature(sym: u.Symbol, in: u.Type = Type.none): u.Type = {
        assert(is.defined(sym), "Symbol is not defined")
        assert(has.tpe(sym),   s"Symbol $sym has no type signature")
        val sign = if (is.defined(in)) sym.infoIn(in) else sym.info
        if (is.byName(sym)) sign.typeArgs.head else sign
      }

      /** Returns the type constructor of an applied type `tpe`. */
      def constructor(tpe: u.Type): u.Type =
        tpe.dealias.widen.typeConstructor

      /** Ensures that a type represents a case class. */
      def isCaseClass(tpe: u.Type): Boolean =
        tpe.typeSymbol.asClass.isCaseClass

      /**
       * Returns a map from formal getter methods to types, containing one
       * mapping for each argument in the primary constructor. The resulting map
       * (a ListMap) preserves the order of the constructor's parameter list.
       */
      def caseClassParamsOf(tpe: u.Type): ListMap[u.MethodSymbol, u.Type] = {
        val ctorSymb = tpe.decl(api.TermName.ctor)
        val ctorPrim =
          if (ctorSymb.isMethod) ctorSymb.asMethod
          else ctorSymb.alternatives.map(_.asMethod).find(_.isPrimaryConstructor).get

        ctorPrim.paramLists.flatMap(_.map(
          sym => {
            val get = tpe.member(sym.name).asMethod
            val ret = tpe.member(sym.name).infoIn(tpe).resultType
            get -> ret
          })
        )(breakOut)
      }

      /** Returns the original type-tree corresponding to `tpe`. */
      def tree(tpe: u.Type): u.Tree = {
        /* Resolve if static, otherwise reference. */
        def resolve(sym: u.Symbol) =
          if (sym.isStatic) api.Tree.resolveStatic(sym) else Id(sym)

        /* Converts stable path-dependent types to a selection chain. */
        def stable(tpe: u.Type): u.Tree = tpe match {
          // Qualified this type: `T.this`.
          case u.ThisType(encl) =>
            if (!encl.isStatic) api.This(encl)
            else if (encl.isClass && !encl.isModuleClass && api.Owner.inEnclChain(encl)) api.This(encl)
            else api.Tree.resolveStatic(encl)
          // Super type: `this.super[T]`
          case u.SuperType(ths, parent) =>
            val sym = parent.typeSymbol.asType
            val sup = u.Super(original(ths), sym.name)
            setType(setSymbol(sup, sym), tpe)
          // Singleton type: `target.type`.
          case u.SingleType(u.NoPrefix, target) =>
            resolve(target)
          // Path-dependent singleton type: `prefix.target.type`.
          case u.SingleType(prefix, target) =>
            Sel(stable(prefix), target)
          // Singleton type: `target.type`.
          case u.TypeRef(u.NoPrefix, target, Nil) =>
            resolve(target)
          // Path-dependent singleton type: `prefix.target.type`.
          case u.TypeRef(prefix, target, Nil) if is.stable(prefix) =>
            Sel(stable(prefix), target)
          case _ =>
            abort(s"Unstable path-dependent $tpe")
        }

        /* Creates the original field of the type-tree. */
        def original(tpe: u.Type): u.Tree = setType(tpe match {
          // This / super type
          case ThisType(_) | SuperType(_, _) =>
            stable(tpe)
          // Singleton type: `target.type`.
          case u.SingleType(_, _) =>
            u.SingletonTypeTree(stable(tpe))
          // Abstract type: `T`.
          case u.TypeRef(u.NoPrefix, target, Nil) =>
            resolve(target)
          // Path dependent type: `prefix.T`.
          // Type projection: `Prefix#T`
          case u.TypeRef(prefix, target, Nil) => original(prefix) match {
            case u.SingletonTypeTree(sng) => Sel(sng, target)
            case pre if is.stable(prefix) => Sel(pre, target)
            case pre => setSymbol(u.SelectFromTypeTree(pre, target.name.toTypeName), target)
          }
          // Applied abstract type: `T[A, B, ...]`.
          // Applied path dependent type: `prefix.T[A, B, ...]`
          // Applied type projection: `Prefix#T[A, B, ...]`
          case u.TypeRef(prefix, target, args) =>
            val sel = original(typeRef(prefix, target, Nil))
            u.AppliedTypeTree(sel, args.map(Type.tree))
          // Type bounds: `T >: lo <: hi`.
          case u.TypeBounds(lo, hi) =>
            u.TypeBoundsTree(Type.tree(lo), Type.tree(hi))
          // Existential type: `F[A, B, ...] forSome { type A; type B; ... }`
          case u.ExistentialType(quantified, underlying) =>
            u.ExistentialTypeTree(Type.tree(underlying), quantified.map(typeDef))
          // Annotated type: `A @ann1 @ann2 ...`
          case u.AnnotatedType(annotations, underlying) =>
            annotations.foldLeft(original(underlying)) {
              (res, ann) => u.Annotated(ann.tree, res)
            }
          // E.g. type refinement: `T { def size: Int }`
          case _ => mkTypeTree(tpe).original
        }, tpe)

        val tpt = u.TypeTree(tpe)
        setOriginal(tpt, original(tpe))
      }
    }

    /** Quoted type-trees. */
    object TypeQuote extends Node {

      /** An empty type-tree. */
      def empty: u.TypeTree = u.TypeTree()

      /** Reifies `tpe` as a tree. */
      def apply(tpe: u.Type): u.TypeTree = {
        assert(is.defined(tpe), s"$this type is not defined")
        u.TypeTree(tpe)
      }

      /** Reifies `sym`'s type as a tree. */
      def apply(sym: u.Symbol): u.TypeTree = {
        assert(is.defined(sym), s"$this symbol is not defined")
        assert(has.tpe(sym),    s"$this symbol $sym has no type")
        apply(sym.info)
      }

      /** Reifies type `T` as a tree. */
      def apply[T: u.TypeTag]: u.TypeTree =
        apply(Type[T])

      def unapply(tpt: u.TypeTree): Option[u.Type] =
        for (tpt <- Option(tpt) if has.tpe(tpt)) yield tpt.tpe
    }

    /** By-name types (`=> T`), legal only in parameter declarations. */
    object ByNameType extends Node {

      lazy val sym: u.ClassSymbol = ByNameParamClass

      def apply(arg: u.Type): u.Type = {
        assert(is.defined(arg), s"$this type argument is not defined")
        typeRef(u.NoPrefix, sym, arg :: Nil)
      }

      def unapply(tpe: u.TypeRef): Option[u.Type] = tpe match {
        case u.TypeRef(_, `sym`, Seq(arg)) => Some(arg)
        case _ => None
      }
    }

    /** Vararg types (`T*`), legal only in parameter declarations. */
    object VarArgType extends Node {

      lazy val scalaSym: u.ClassSymbol = RepeatedParamClass
      lazy val javaSym:  u.ClassSymbol = JavaRepeatedParamClass

      def apply(arg: u.Type): u.Type = {
        assert(is.defined(arg), s"$this type argument is not defined")
        typeRef(u.NoPrefix, scalaSym, arg :: Nil)
      }

      def unapply(tpe: u.TypeRef): Option[u.Type] = tpe match {
        case u.TypeRef(_, `scalaSym`, Seq(arg)) => Some(arg)
        case u.TypeRef(_, `javaSym`,  Seq(arg)) => Some(arg)
        case _ => None
      }
    }
  }
}
