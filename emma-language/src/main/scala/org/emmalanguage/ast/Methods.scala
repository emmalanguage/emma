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

/** Methods (defs). */
trait Methods { this: AST =>

  /**
   * Methods (defs).
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

    import universe._
    import internal._
    import reificationSupport._
    import Flag._

    /** Method (def) symbols. */
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

        assert(are.not(CONTRAVARIANT)(flags), s"$this $name cannot be a label")
        val sym = newMethodSymbol(owner, TermName(name), pos, flags)
        val tps = tparams.map(Sym.With(_)(own = sym, flg = DEFERRED | PARAM).asType)
        val pss = paramss.map(_.map(Sym.With(_)(own = sym, flg = PARAM).asTerm))
        setInfo(sym, Type.method(tps: _*)(pss: _*)(result))
      }

      def unapply(sym: u.MethodSymbol): Option[u.MethodSymbol] =
        Option(sym).filter(is.method)
    }

    /** Method (def) calls. */
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
            assert(is.defined(tgt), s"$this target is not defined")
            assert(is.term(tgt),    s"$this target is not a term:\n${Tree.show(tgt)}")
            assert(has.tpe(tgt),    s"$this target has no type:\n${Tree.showTypes(tgt)}")
            val resolved = Sym.resolveOverloaded(tgt.tpe)(method, targs: _*)(argss: _*)
            assert(is.method(resolved), s"$this resolved variant $resolved is not a method")
            Sel(tgt, resolved)

          case None =>
            val resolved = Sym.resolveOverloaded()(method, targs: _*)(argss: _*)
            assert(is.method(resolved), s"$this resolved variant $resolved is not a method")
            Id(resolved)
        }

        val app = TermApp(fun, targs: _*)(argss: _*)
        setType(app, app.tpe.finalResultType)
      }

      def unapplySeq(call: u.Tree)
        : Option[(Option[u.Tree], u.MethodSymbol, Seq[u.Type], Seq[Seq[u.Tree]])]
        = call match {
          case ref @ Id(DefSym(met))
            if is.result(ref.tpe) => Some(None, met, Seq.empty, Seq.empty)
          case sel @ Sel(Term(tgt), DefSym(met))
            if is.result(sel.tpe) => Some(Some(tgt), met, Seq.empty, Seq.empty)
          case app @ TermApp(Id(DefSym(met)), targs, argss@_*)
            if is.result(app.tpe) => Some(None, met, targs, argss)
          case app @ TermApp(Sel(Term(tgt), DefSym(met)), targs, argss@_*)
            if is.result(app.tpe) => Some(Some(tgt), met, targs, argss)
          case _ => None
        }
    }

    /** Method (def) definitions. */
    object DefDef extends Node {

      /**
       * Creates a type-checked method definition.
       * @param sym Must be a method symbol.
       * @param flg Any additional modifiers (e.g. access modifiers).
       * @param tparams The symbols of type parameters (to be substituted with `sym.typeParams`).
       * @param paramss The symbols of all parameters (to be substituted with `sym.paramLists`).
       * @param body The body of this method (with parameters substituted), owned by `sym`.
       * @return `..flags def method[..tparams](...paramss) = body`.
       */
      def apply(sym: u.MethodSymbol, flg: u.FlagSet = u.NoFlags)
        (tparams: u.TypeSymbol*)
        (paramss: Seq[u.TermSymbol]*)
        (body: u.Tree): u.DefDef = {

        assert(is.defined(sym), s"$this symbol is not defined")
        assert(is.method(sym),  s"$this symbol $sym is not a method")
        assert(has.nme(sym),    s"$this symbol $sym has no name")
        assert(is.encoded(sym), s"$this symbol $sym is not encoded")
        assert(has.tpe(sym),    s"$this symbol $sym has no type")
        assert(tparams.forall(is.defined),         s"Not all $this type parameters are defined")
        assert(paramss.flatten.forall(is.defined), s"Not all $this parameters are defined")
        assert(have.nme(paramss.flatten),          s"Not all $this parameters have names")
        assert(paramss.flatten.forall(has.tpe),    s"Not all $this parameters have types")
        assert(is.defined(body), s"$this body is not defined")
        assert(is.term(body),    s"$this body is not a term:\n${Tree.show(body)}")
        assert(has.tpe(body),    s"$this body has no type:\n${Tree.showTypes(body)}")
        assert(tparams.size == sym.typeParams.size, s"Wrong number of $this type parameters")
        assert(paramss.size == sym.paramLists.size, s"Wrong number of $this parameter lists")
        assert(paramss.flatten.size == sym.paramLists.flatten.size,
          s"Shape of $this parameter lists doesn't match")
        val tps = sym.typeParams.map(typeDef)
        val pss = sym.paramLists.map(_.map(p => ParDef(p.asTerm)))
        val src = tparams ++ paramss.flatten
        val dst = sym.typeParams ++ sym.paramLists.flatten
        val rhs = Sym.subst(sym, src zip dst: _*)(body)
        val res = sym.info.finalResultType
        assert(rhs.tpe <:< sym.info.finalResultType,
          s"$this body type ${rhs.tpe} is not a subtype of return type $res")
        val dfn = u.DefDef(Sym.mods(sym), sym.name, tps, pss, TypeQuote(res), rhs)
        setSymbol(dfn, sym)
      }

      def unapply(defn: u.DefDef)
        : Option[(u.MethodSymbol, u.FlagSet, Seq[u.TypeSymbol], Seq[Seq[u.ValDef]], u.Tree)]
        = defn match {
          case Tree.With.sym(u.DefDef(mods, _, tps, pss, _, Term(rhs)), DefSym(lhs)) =>
            Some(lhs, mods.flags, tps.map(_.symbol.asType), pss, rhs)
          case _ => None
        }
    }
  }
}
