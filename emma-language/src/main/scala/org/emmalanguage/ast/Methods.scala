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

/** Methods (`def`s). */
trait Methods { this: AST =>

  /**
   * Methods (`def`s).
   *
   * === Examples ===
   * {{{
   *   // definitions
   *   def pi = 3.14
   *   def greet() = println("Hello, World!")
   *   def nil[A] = List.empty[A]
   *   def pNil[A]() = println(nil[A])
   *   def succ(i: Int) = i + 1
   *   def one[A](a: A) = a :: Nil
   *   def add(x: Int, y: Int) = x + y
   *   def pair[A, B](a: A, b: B) = (a, b)
   *   def addCurry(x: Int)(y: Int) = x + y
   *   def pairCurry[A, B](a: A)(b: B) = (a, b)
   *   def zero[N: Numeric] = implicitly[Numeric[N]].zero
   *   def times2[N](n: N)(implicit N: Numeric[N]) = N.times(n, N.fromInt(2))
   *
   *   // calls
   *   pi
   *   greet()
   *   nil[String]
   *   pNil[String]()
   *   succ(42)
   *   one[Char]('1')
   *   add(2, 3)
   *   pair[String, Double]("pi = ", 3.14)
   *   addCurry(2)(3)
   *   pairCurry[String, Double]("pi = ")(3.14)
   *   zero[Double]
   *   times2[Long](5)
   * }}}
   */
  trait MethodAPI { this: API =>

    import u.Flag._
    import u.internal.{newMethodSymbol, typeDef}
    import universe._

    /** Method (`def`) symbols. */
    object DefSym extends Node {

      /**
       * Creates a new method symbol.
       * @param owner The enclosing named entity where this method is defined.
       * @param name The name of this method (will be encoded).
       * @param flags Any additional modifiers (e.g. access modifiers).
       * @param pos The (optional) source code position where this method is defined.
       * @param tparams The symbols of type parameters (to be copied with the new symbol as owner).
       * @param paramss The symbols of all parameters (to be copied with the new symbol as owner).
       * @param result The result type of this method.
       * @return A new method symbol.
       */
      def apply(owner: u.Symbol, name: u.TermName,
        flags: u.FlagSet = u.NoFlags,
        pos: u.Position = u.NoPosition)
        (tparams: u.TypeSymbol*)
        (paramss: Seq[u.TermSymbol]*)
        (result: u.Type): u.MethodSymbol = {

        assert(are.not(CONTRAVARIANT)(flags), s"$this `$name` cannot be a label")
        val method = newMethodSymbol(owner, TermName(name), pos, flags)
        val tps = tparams.map(Sym.copy(_)(owner = method, flags = DEFERRED | PARAM).asType)
        val pss = paramss.map(_.map(Sym.copy(_)(owner = method, flags = PARAM).asTerm))
        set.tpe(method, Type.method(tps: _*)(pss: _*)(result))
        method
      }

      def unapply(sym: u.MethodSymbol): Option[u.MethodSymbol] =
        Option(sym).filter(is.method)
    }

    /** Method (`def`) calls. */
    object DefCall extends Node {

      /**
       * Creates a type-checked method call
       * @param target The (optional) target (must be a term if any).
       * @param method Must be a method symbol or an overloaded symbol.
       * @param targs The type arguments (if the method is generic).
       * @param argss All argument lists (partial application not supported).
       * @return `[target.]method[..targs](...argss)`.
       */
      def apply(target: Option[u.Tree] = None)
        (method: u.TermSymbol, targs: u.Type*)
        (argss: Seq[u.Tree]*): u.Tree = {

        val fun = target match {
          case Some(tgt) =>
            assert(is.defined(tgt), s"$this target is not defined: $tgt")
            assert(is.term(tgt), s"$this target is not a term:\n${Tree.show(tgt)}")
            assert(has.tpe(tgt), s"$this target has no type:\n${Tree.showTypes(tgt)}")
            val resolved = Sym.resolveOverloaded(tgt.tpe)(method, targs: _*)(argss: _*)
            assert(is.method(resolved), s"$this resolved variant `$resolved` is not a method")
            Sel(tgt, resolved)

          case None =>
            val resolved = Sym.resolveOverloaded()(method, targs: _*)(argss: _*)
            assert(is.method(resolved), s"$this resolved variant `$resolved` is not a method")
            Id(resolved)
        }

        val app = TermApp(fun, targs: _*)(argss: _*)
        set(app, tpe = app.tpe.finalResultType)
        app
      }

      def unapplySeq(call: u.Tree)
        : Option[(Option[u.Tree], u.MethodSymbol, Seq[u.Type], Seq[Seq[u.Tree]])] = call match {

        case Id(DefSym(method)) withType Type.Result(_) =>
          Some(None, method, Seq.empty, Seq.empty)
        case Sel(Term(target), DefSym(method)) withType Type.Result(_) =>
          Some(Some(target), method, Seq.empty, Seq.empty)
        case TermApp(Id(DefSym(method)), targs, argss@_*) withType Type.Result(_) =>
          Some(None, method, targs, argss)
        case TermApp(Sel(Term(target), DefSym(method)), targs, argss@_*) withType Type.Result(_) =>
          Some(Some(target), method, targs, argss)
        case _ => None
      }
    }

    /** Method (`def`) definitions. */
    object DefDef extends Node {

      /**
       * Creates a type-checked method definition.
       * @param sym Must be a method symbol.
       * @param flags Any additional modifiers (e.g. access modifiers).
       * @param tparams The symbols of type parameters (to be substituted with `sym.typeParams`).
       * @param paramss The symbols of all parameters (to be substituted with `sym.paramLists`).
       * @param body The body of this method (with parameters substituted), owned by `sym`.
       * @return `..flags def method[..tparams](...paramss) = body`.
       */
      def apply(sym: u.MethodSymbol, flags: u.FlagSet = u.NoFlags)
        (tparams: u.TypeSymbol*)
        (paramss: Seq[u.TermSymbol]*)
        (body: u.Tree): u.DefDef = {

        assert(is.defined(sym), s"$this symbol `$sym` is not defined")
        assert(is.method(sym), s"$this symbol `$sym` is not a method")
        assert(has.name(sym), s"$this symbol `$sym` has no name")
        assert(is.encoded(sym), s"$this symbol `$sym` is not encoded")
        assert(has.tpe(sym), s"$this symbol `$sym` has no type")
        assert(tparams.forall(is.defined), s"Not all $this type parameters are defined")
        assert(paramss.flatten.forall(is.defined), s"Not all $this parameters are defined")
        assert(have.name(paramss.flatten), s"Not all $this parameters have names")
        assert(paramss.flatten.forall(has.tpe), s"Not all $this parameters have types")
        assert(is.defined(body), s"$this body is not defined: $body")
        assert(is.term(body), s"$this body is not a term:\n${Tree.show(body)}")
        assert(has.tpe(body), s"$this body has no type:\n${Tree.showTypes(body)}")
        assert(tparams.size == sym.typeParams.size, s"Wrong number of $this type parameters")
        assert(paramss.size == sym.paramLists.size, s"Wrong number of $this parameter lists")
        assert(paramss.flatten.size == sym.paramLists.flatten.size,
          s"Shape of $this parameter lists doesn't match")
        assert({
          val tpMap = (tparams.map(_.toType) zip sym.typeParams.map(_.asType.toType))
            .toMap.withDefault(identity[u.Type])
          (paramss.flatten zip sym.paramLists.flatten)
            .forall { case (p, q) => p.info.map(tpMap) =:= q.info }
        }, s"Not all $this parameters have the correct type")

        val mods = u.Modifiers(get.flags(sym) | flags)
        val tpeDefs = sym.typeParams.map(typeDef)
        val parDefs = sym.paramLists.map(_.map(p => ParDef(p.asTerm)))
        val original = tparams ++ paramss.flatten
        val aliases = sym.typeParams ++ sym.paramLists.flatten
        val rhs = Sym.subst(sym, original zip aliases: _*)(body)
        val res = sym.info.finalResultType
        assert(rhs.tpe <:< sym.info.finalResultType,
          s"$this body type `${rhs.tpe}` is not a subtype of return type `$res`")

        val defn = u.DefDef(mods, sym.name, tpeDefs, parDefs, TypeQuote(res), rhs)
        set(defn, sym = sym)
        defn
      }

      def unapply(defn: u.DefDef)
        : Option[(u.MethodSymbol, u.FlagSet, Seq[u.TypeSymbol], Seq[Seq[u.ValDef]], u.Tree)]
        = defn match {
          case defDef@u.DefDef(mods, _, tparams, paramss, _, Term(body)) withSym DefSym(method) =>
            assert(paramss forall { _ forall { _.rhs == Empty() } },
              "DefDef parameters with default parameter values are not supported.")
            assert(defDef.name.toString != "<init>", "DefDef.unapply encountered a constructor. " +
              "This shouldn't happen, since we don't allow class definitions in the source language.")
            Some(method, mods.flags, tparams.map(TypeSym.of), paramss, body)
          case _ => None
        }
    }
  }
}
