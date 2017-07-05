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

    import u._
    import internal._
    import reificationSupport._
    import Flag._

    /** Method (def) symbols. */
    object DefSym extends Node {

      /**
       * Creates a type-checked method symbol.
       * @param own The enclosing named entity where this method is defined.
       * @param nme The name of this method (will be encoded).
       * @param tps The symbols of type parameters (to be copied with the new symbol as owner).
       * @param pss The symbols of all parameters (to be copied with the new symbol as owner).
       * @param res The result type of this method.
       * @param flg Any additional modifiers (e.g. access modifiers).
       * @param pos The (optional) source code position where this method is defined.
       * @param ans Any (optional) annotations associated with this method.
       * @return A new method symbol.
       */
      def apply(own: u.Symbol, nme: u.TermName,
        tps: Seq[u.TypeSymbol]      = Seq.empty,
        pss: Seq[Seq[u.TermSymbol]] = Seq.empty,
        res: u.Type                 = Type.unit,
        flg: u.FlagSet              = u.NoFlags,
        pos: u.Position             = u.NoPosition,
        ans: Seq[u.Annotation]      = Seq.empty
      ): u.MethodSymbol = {
        assert(are.not(CONTRAVARIANT, flg), s"$this $nme cannot be a label")
        val method  = newMethodSymbol(own, TermName(nme), pos, flg)
        val tparams = tps.map(Sym.With(_)(own = method, flg = DEFERRED | PARAM).asType)
        val paramss = pss.map(_.map(Sym.With(_)(own = method, flg = PARAM).asTerm))
        setInfo(method, Type.method(tparams, paramss, res))
        setAnnotations(method, ans.toList)
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
       * @param targs  The type arguments (if the method is generic).
       * @param argss  All argument lists (partial application not supported).
       * @return `[target.]method[..targs](...argss)`.
       */
      def apply(target: Option[u.Tree], method: u.TermSymbol,
        targs: Seq[u.Type]      = Seq.empty,
        argss: Seq[Seq[u.Tree]] = Seq.empty
      ): u.Tree = {
        val fun = target match {
          case Some(tgt) =>
            assert(is.defined(tgt), s"$this target is not defined")
            assert(is.term(tgt),    s"$this target is not a term:\n${Tree.show(tgt)}")
            assert(has.tpe(tgt),    s"$this target has no type:\n${Tree.showTypes(tgt)}")
            val resolved = Sym.resolveOverloaded(tgt.tpe, method, targs, argss)
            assert(is.method(resolved), s"$this resolved variant $resolved is not a method")
            Sel(tgt, resolved)

          case None =>
            val resolved = Sym.resolveOverloaded(Type.none, method, targs, argss)
            assert(is.method(resolved), s"$this resolved variant $resolved is not a method")
            Id(resolved)
        }

        val app = TermApp(fun, targs, argss)
        setType(app, app.tpe.finalResultType)
      }

      def unapply(call: u.Tree)
        : Option[(Option[u.Tree], u.MethodSymbol, Seq[u.Type], Seq[Seq[u.Tree]])]
        = call match {
          case ref @ Id(DefSym(met))
            if is.result(ref.tpe) => Some(None, met, Seq.empty, Seq.empty)
          case sel @ Sel(Term(tgt), DefSym(met))
            if is.result(sel.tpe) => Some(Some(tgt), met, Seq.empty, Seq.empty)
          case app @ TermApp(Id(DefSym(met)), targs, argss)
            if is.result(app.tpe) => Some(None, met, targs, argss)
          case app @ TermApp(Sel(Term(tgt), DefSym(met)), targs, argss)
            if is.result(app.tpe) => Some(Some(tgt), met, targs, argss)
          case _ => None
        }
    }

    /** Method (def) definitions. */
    object DefDef extends Node {

      /**
       * Creates a type-checked method definition.
       * @param method  Must be a method symbol.
       * @param tparams The symbols of type parameters (to be substituted with `sym.typeParams`).
       * @param paramss The symbols of all parameters (to be substituted with `sym.paramLists`).
       * @param body    The body of this method (with parameters substituted), owned by `sym`.
       * @return `def method[..tparams](...paramss) = body`.
       */
      def apply(method: u.MethodSymbol,
        tparams: Seq[u.TypeSymbol]      = Seq.empty,
        paramss: Seq[Seq[u.TermSymbol]] = Seq.empty,
        body:    u.Tree                 = Term.unit
      ): u.DefDef = {
        assert(is.defined(method), s"$this symbol is not defined")
        assert(is.method(method),  s"$this symbol $method is not a method")
        assert(has.nme(method),    s"$this symbol $method has no name")
        assert(is.encoded(method), s"$this symbol $method is not encoded")
        assert(has.tpe(method),    s"$this symbol $method has no type")
        assert(tparams.forall(is.defined),         s"Not all $this type parameters are defined")
        assert(paramss.flatten.forall(is.defined), s"Not all $this parameters are defined")
        assert(have.nme(paramss.flatten),          s"Not all $this parameters have names")
        assert(paramss.flatten.forall(has.tpe),    s"Not all $this parameters have types")
        assert(is.defined(body), s"$this body is not defined")
        assert(is.term(body),    s"$this body is not a term:\n${Tree.show(body)}")
        assert(has.tpe(body),    s"$this body has no type:\n${Tree.showTypes(body)}")
        assert(tparams.size == method.typeParams.size, s"Wrong number of $this type parameters")
        assert(paramss.size == method.paramLists.size, s"Wrong number of $this parameter lists")
        assert(paramss.flatten.size == method.paramLists.flatten.size,
          s"Shape of $this parameter lists doesn't match")
        val mod = Sym.mods(method)
        val tps = method.typeParams.map(typeDef)
        val pss = method.paramLists.map(_.map(p => ParDef(p.asTerm)))
        val src = tparams ++ paramss.flatten
        val dst = method.typeParams ++ method.paramLists.flatten
        val rhs = Sym.subst(method, src zip dst)(body)
        val res = method.info.finalResultType
        assert(rhs.tpe <:< res, s"$this body type ${rhs.tpe} is not a subtype of return type $res")
        val tpt = TypeQuote(res)
        val dfn = u.DefDef(mod, method.name, tps, pss, tpt, rhs)
        setSymbol(dfn, method)
        setType(dfn, u.NoType)
      }

      def unapply(defn: u.DefDef)
        : Option[(u.MethodSymbol, Seq[u.TypeSymbol], Seq[Seq[u.ValDef]], u.Tree)]
        = defn match {
          case Tree.With.sym(u.DefDef(_, _, tps, pss, _, Term(rhs)), DefSym(lhs)) =>
            Some(lhs, tps.map(_.symbol.asType), pss, rhs)
          case _ => None
        }
    }
  }
}
