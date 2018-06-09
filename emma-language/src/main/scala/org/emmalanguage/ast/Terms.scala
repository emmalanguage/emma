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

trait Terms { this: AST =>

  trait TermAPI { this: API =>

    import u._
    import internal._
    import reificationSupport._

    /** Term names. */
    object TermName extends Node {

      val regex = s"(.*)\\$$$freshNameSuffix(\\d+)".r

      // Predefined term names
      lazy val anon      = apply("anon")
      lazy val app       = apply("apply")
      lazy val ctor      = u.termNames.CONSTRUCTOR
      lazy val empty     = u.termNames.EMPTY
      lazy val exprOwner = u.TermName("<expression-owner>")
      lazy val foreach   = apply("foreach")
      lazy val init      = u.termNames.CONSTRUCTOR
      lazy val lambda    = apply("anonfun")
      lazy val local     = u.TermName(s"<local $exprOwner>")
      lazy val root      = u.termNames.ROOTPKG
      lazy val unApp     = apply("unapply")
      lazy val unAppSeq  = apply("unapplySeq")
      lazy val wildcard  = u.termNames.WILDCARD

      /** Creates a new term name (must be non-empty). */
      def apply(name: String): u.TermName = {
        assert(name.nonEmpty, "Empty term name")
        apply(u.TermName(name))
      }

      /** Encodes `name` and converts it to a term name. */
      def apply(name: u.Name): u.TermName = {
        assert(is.defined(name), "Undefined name")
        name.encodedName.toTermName
      }

      /** Extracts the term name of `sym`, if any. */
      def apply(sym: u.Symbol): u.TermName = {
        assert(is.defined(sym), "Undefined symbol")
        assert(has.nme(sym),   s"Symbol $sym has no name")
        apply(sym.name)
      }

      /** Creates a fresh term name with the given `prefix`. */
      def fresh(prefix: String = "x"): u.TermName = apply {
        assert(prefix.nonEmpty, "Cannot create a fresh name with empty prefix")
        freshTermName(s"$prefix$$$freshNameSuffix")
      }

      /** Creates a fresh term name with the given `prefix`. */
      def fresh(prefix: u.Name): u.TermName =
        if (is.defined(prefix)) prefix match {
          case u.TermName(regex(original, _)) => fresh(original.toString)
          case _ => fresh(prefix.toString)
        } else fresh()

      /** Creates a fresh term name with the given symbol's name as `prefix`. */
      def fresh(prefix: u.Symbol): u.TermName =
        if (is.defined(prefix)) fresh(prefix.name) else fresh()

      /** Tries to return the original name used to create this `fresh` name. */
      def original(fresh: u.Name): (u.TermName, Int) = fresh match {
        case u.TermName(regex(original, i)) => u.TermName(original) -> i.toInt
        case _ => fresh.toTermName -> 0
      }

      def unapply(name: u.TermName): Option[String] =
        for (name <- Option(name) if is.defined(name)) yield name.toString
    }

    /** Term symbols. */
    object TermSym extends Node {

      /**
       * Creates a type-checked term symbol.
       * @param own The symbol of the enclosing named entity where this term is defined.
       * @param nme The name of this term (will be encoded).
       * @param tpe The type of this term (will be dealiased and widened).
       * @param flg Any (optional) modifiers (e.g. var, parameter, implicit, lazy).
       * @param pos The (optional) source code position where this term is defined.
       * @param ans Any (optional) annotations associated with this term.
       * @return A new type-checked term symbol.
       */
      def apply(own: u.Symbol, nme: u.TermName, tpe: u.Type,
        flg: u.FlagSet         = u.NoFlags,
        pos: u.Position        = u.NoPosition,
        ans: Seq[u.Annotation] = Seq.empty
      ): u.TermSymbol = {
        assert(is.defined(nme), s"$this name is not defined")
        assert(is.defined(tpe),  s"$this type is not defined")
        val sym = newTermSymbol(own, TermName(nme), pos, flg)
        setInfo(sym, tpe.dealias)
        setAnnotations(sym, ans.toList)
      }

      /** Creates a free term symbol (without an owner). */
      def free(name: u.TermName, tpe: u.Type,
        flg: u.FlagSet         = u.NoFlags,
        ans: Seq[u.Annotation] = Seq.empty
      ): u.FreeTermSymbol = {
        assert(is.defined(name), s"$this name is not defined")
        assert(is.defined(tpe),  s"$this type is not defined")
        val free = internal.newFreeTerm(TermName(name).toString, null, flg, null)
        setInfo(free, tpe.dealias)
        setAnnotations(free, ans.toList)
      }

      /** Creates a free term symbol with the same attributes as the `original`. */
      def free(original: u.TermSymbol): u.FreeTermSymbol =
        free(TermName(original), original.info, flags(original), original.annotations)

      /** Creates a fresh term symbol with the same attributes as the `original`. */
      def fresh(original: u.TermSymbol): u.TermSymbol =
        Sym.With(original)(nme = TermName.fresh(original)).asTerm

      def unapply(sym: u.TermSymbol): Option[u.TermSymbol] =
        Option(sym)
    }

    object Term extends Node {

      // Predefined terms
      lazy val unit = Lit(())

      def unapply(tree: u.Tree): Option[u.Tree] =
        Option(tree).filter(is.term)
    }

    /** Term applications (for internal use). */
    private[ast] object TermApp extends Node {

      def apply(target: u.Tree, args: Seq[u.Tree]): u.Apply = {
        assert(is.defined(target), s"$this target is not defined")
        assert(has.sym(target),    s"$this target has no symbol:\n${Tree.showSymbols(target)}")
        assert(has.tpe(target),    s"$this target has no type:\n${Tree.showTypes(target)}")
        assert(are.defined(args),  s"Not all $this args are defined")
        assert(are.terms(args),    s"Not all $this args are terms")
        assert(have.tpe(args),     s"Not all $this args have type")
        val varParam = target.tpe.paramLists.head.lastOption.map(_.info)
        val argList = ((varParam, args) match {
          // Handle variable length arguments.
          case (Some(VarArgType(paramTpe)), initArgs :+ lastArg)
            if lastArg.tpe <:< Type.kind1[Seq](paramTpe) =>
            val tpt = setType(u.Ident(TypeName.wildStar), paramTpe)
            initArgs :+ setType(u.Typed(lastArg, tpt), paramTpe)
          case _ => args
        }).toList
        val app = u.Apply(target, argList)
        setSymbol(app, target.symbol)
        setType(app, target.tpe.resultType)
      }

      def apply(target: u.Tree,
        targs: Seq[u.Type]      = Seq.empty,
        argss: Seq[Seq[u.Tree]] = Seq.empty
      ): u.Tree = {
        assert(is.defined(target), s"$this target is not defined")
        assert(has.tpe(target),    s"$this target has no type:\n${Tree.showTypes(target)}")
        if (targs.isEmpty) {
          if (argss.isEmpty) Tree.With(target)(tpe = target.tpe.resultType)
          else argss.foldLeft(target)(apply)
        } else apply(TypeApp(target, targs), argss = argss)
      }

      def unapply(tree: u.Tree): Option[(u.Tree, Seq[u.Type], Seq[Seq[u.Tree]])] = tree match {
        case u.Apply(TermApp(target, targs, argss), args) =>
          Some(target, targs, argss :+ cleanVarArgs(args))
        case u.Apply(target, args) =>
          Some(target, Nil, Seq(cleanVarArgs(args)))
        case TypeApp(target, targs) =>
          Some(target, targs, Nil)
        case _ => None
      }

      /** Removes var-arg specific type ascriptions. */
      private def cleanVarArgs(args: Seq[u.Tree]) = args.map {
        case u.Typed(arg, u.Ident(TypeName.wildStar)) => arg
        case arg => arg
      }
    }

    /** Type applications (for internal use). */
    private[ast] object TypeApp extends Node {

      def apply(target: u.Tree, targs: Seq[u.Type] = Seq.empty): u.TypeApply = {
        assert(is.defined(target), s"$this target is not defined")
        assert(has.tpe(target),    s"$this target has no type:\n${Tree.showTypes(target)}")
        assert(targs.nonEmpty,     s"No type args supplied to $this")
        assert(targs.forall(is.defined), s"Not all $this type args are defined")
        val tpts = targs.map(targ => TypeQuote(targ.dealias.widen)).toList
        val tapp = u.TypeApply(target, tpts)
        setType(tapp, Type(target.tpe, targs))
      }

      def unapply(tapp: u.TypeApply): Option[(u.Tree, Seq[u.Type])] =
        Some(tapp.fun, tapp.args.map(_.tpe))
    }

    /** Term references (values, variables, parameters and modules). */
    object TermRef extends Node {

      /**
       * Creates a type-checked term reference.
       * @param target Must be a term symbol
       * @return `target`.
       */
      def apply(target: u.TermSymbol): u.Ident =
        Ref(target)

      def unapply(tree: u.Tree): Option[u.TermSymbol] = tree match {
        case Ref(TermSym(target)) => Some(target)
        case _ => None
      }
    }

    /** Term accesses (modules and private[this] values and variables). */
    object TermAcc extends Node {

      /**
       * Creates a type-checked term access.
       * @param target Must be a term.
       * @param member Must be a term symbol (but not a method).
       * @return `target.member`
       */
      def apply(target: u.Tree, member: u.TermSymbol): u.Select =
        Acc(target, member)

      def unapply(acc: u.Select): Option[(u.Tree, u.TermSymbol)] = acc match {
        case Acc(target, TermSym(member)) => Some(target, member)
        case _ => None
      }
    }

    /** Term definitions. */
    object TermDef extends Node {
      def unapply(tree: u.Tree): Option[u.TermSymbol] = tree match {
        case Def(TermSym(lhs)) => Some(lhs)
        case _ => None
      }
    }

    /** Atomic terms (literals, references and `this`). */
    object Atomic extends Node {
      def unapply(tree: u.Tree): Option[u.Tree] = tree match {
        case lit @ Lit(_)     => Some(lit)
        case ref @ TermRef(_) => Some(ref)
        case ths @ This(_)    => Some(ths)
        case _ => None
      }
    }

    /** Literals. */
    object Lit extends Node {

      /**
       * Creates a type-checked literal.
       * @param value Must be a literal ([[Null]], [[AnyVal]] or [[String]]).
       * @return `value`.
       */
      def apply(value: Any): u.Literal = {
        val cst = u.Constant(value)
        val tpe = if (() == value) Type.unit else constantType(cst)
        setType(u.Literal(cst), tpe)
      }

      def unapply(lit: u.Literal): Option[Any] =
        Some(lit.value.value)
    }

    /** `this` references to enclosing classes or objects. */
    object This extends Node {

      /**
       * Creates a type-checked this reference.
       * @param encl The symbol of the enclosing class or object.
       * @return `this.sym`.
       */
      def apply(encl: u.Symbol): u.This = {
        assert(is.defined(encl), s"$this enclosing is not defined")
        assert(has.nme(encl),    s"$this enclosing $encl has no name")
        assert(encl.isClass || encl.isModule, s"$this $encl is neither a class nor a module")
        val (sym, nme) = if (encl.isClass) encl -> encl.name.toTypeName
          else encl.asModule.moduleClass -> TypeName.empty
        val ths = u.This(nme)
        setSymbol(ths, sym)
        setType(ths, thisType(sym))
      }

      def unapply(ths: u.This): Option[u.Symbol] =
        for (ths <- Option(ths) if has.sym(ths)) yield ths.symbol
    }

    /** Type ascriptions. */
    object TypeAscr extends Node {

      /**
       * Creates a type-checked type ascription.
       * @param expr Must be a term.
       * @param tpe The type to cast `expr` to (must be a weak super-type).
       * @return `expr: tpe`.
       */
      def apply(expr: u.Tree, tpe: u.Type): u.Typed = {
        assert(is.defined(expr), s"$this expr is not defined")
        assert(is.term(expr),    s"$this expr is not a term:\n${Tree.show(expr)}")
        assert(has.tpe(expr),    s"$this expr has no type:\n${Tree.showTypes(expr)}")
        assert(is.defined(tpe),  s"$this type is not defined")
        assert(expr.tpe <:< tpe, s"Expression of type ${expr.tpe} cannot be casted to $tpe")
        val tpd = u.Typed(expr, TypeQuote(tpe))
        setType(tpd, tpe)
      }

      def unapply(ascr: u.Typed): Option[(u.Tree, u.Type)] = ascr match {
        // This encodes variable length arguments: method(seq: _*).
        case u.Typed(_, u.Ident(TypeName.wildStar)) => None
        case u.Typed(Term(expr), tpt) => Some(expr, tpt.tpe)
        case _ => None
      }
    }

    /** Class instantiations. */
    object Inst extends Node {

      /**
       * Creates a type-checked class instantiation.
       * @param target The type of the class to instantiate (might be path-dependent).
       * @param targs  The type arguments (if `target` is generic).
       * @param argss  All argument lists (partial application not supported).
       * @return `new target[..targs](...argss)`.
       */
      def apply(target: u.Type,
        targs: Seq[u.Type]      = Seq.empty,
        argss: Seq[Seq[u.Tree]] = Seq.empty
      ): u.Tree = {
        assert(is.defined(target),         s"$this target is not defined")
        assert(targs.forall(is.defined),   s"Not all $this type args are defined")
        assert(are.defined(argss.flatten), s"Not all $this args are defined")
        assert(have.tpe(argss.flatten),    s"Not all $this args have type")
        val cls = Type.constructor(target)
        val ini = cls.decl(TermName.init)
        val tpe = Type(cls, targs)
        val ctr = Sym.resolveOverloaded(tpe, ini, argss = argss)
        val tpt = setType(u.New(TypeQuote(tpe)), tpe)
        val app = TermApp(Sel(tpt, ctr), argss = argss)
        setSymbol(app, ctr)
        setType(app, tpe)
      }

      def unapply(tree: u.Tree): Option[(u.Type, Seq[u.Type], Seq[Seq[u.Tree]])] = tree match {
        case app @ TermApp(Sel(u.New(tpt), _), _, argss) if is.result(app.tpe) =>
          Some(tpt.tpe.typeConstructor, tpt.tpe.typeArgs, argss)
        case _ => None
      }
    }

    /** Lambdas (anonymous functions). */
    object Lambda extends Node {

      /**
       * Creates a type-checked lambda.
       * @param params The symbols of all parameters (to be copied with a new owner).
       * @param body   The function body (with parameter symbols substituted), owned by the lambda.
       * @return `(..params) => body`.
       */
      def apply(
        params: Seq[u.TermSymbol] = Seq.empty,
        body:   u.Tree            = Term.unit
      ): u.Function = {
        assert(params.forall(is.defined), s"Not all $this parameters are defined")
        assert(have.nme(params),          s"Not all $this parameters have names")
        assert(params.forall(has.tpe),    s"Not all $this parameters have types")
        assert(is.defined(body),          s"$this body is not defined  ")
        assert(is.term(body),             s"$this body is not a term:\n${Tree.show(body)}")
        assert(has.tpe(body),             s"$this body has no type:\n${Tree.showTypes(body)}")
        val pts = params.map(_.info)
        val tpe = Type.fun(pts, body.tpe)
        val sym = TermSym.free(TermName.lambda, tpe)
        val als = for ((p, t) <- params zip pts)
          yield ParSym(sym, p.name, t, if (p.isImplicit) u.Flag.IMPLICIT else u.NoFlags)
        val rhs = Sym.subst(sym, params zip als)(body)
        val fun = u.Function(als.map(ParDef(_)).toList, rhs)
        setSymbol(fun, sym)
        setType(fun, tpe)
      }

      def unapply(fun: u.Function): Option[(u.TermSymbol, Seq[u.ValDef], u.Tree)] = fun match {
        case Tree.With.sym(u.Function(pss, Term(rhs)), TermSym(sym)) => Some(sym, pss, rhs)
        case _ => None
      }
    }

    /** Blocks. */
    object Block extends Node {

      /**
       * Creates a type-checked block.
       * @param stats Statements (`Unit`s are filtered out).
       * @param expr  Must be a term (use `Unit` to simulate a statement block).
       * @return `{ ..stats; expr }`.
       */
      def apply(
        stats: Seq[u.Tree] = Seq.empty,
        expr:  u.Tree      = Term.unit
      ): u.Block = {
        assert(are.defined(stats), s"Not all $this statements are defined")
        assert(is.defined(expr),   s"$this expr is not defined")
        assert(is.term(expr),      s"$this expr is not a term:\n${Tree.show(expr)}")
        assert(has.tpe(expr),      s"$this expr has no type:\n${Tree.showTypes(expr)}")
        val compressed = stats.filter {
          case Lit(()) => false
          case _ => true
        }.toList
        val block = u.Block(compressed, expr)
        setType(block, expr.tpe)
      }

      def unapply(block: u.Block): Option[(Seq[u.Tree], u.Tree)] = block match {
        // Avoid matching loop bodies
        case DoWhileBody(_, _, _) => None
        case u.Block(_ :: Nil, LoopCall(_)) => None
        case u.Block(stats, Term(expr)) => Some(stats, expr)
        case _ => None
      }
    }

    /** If-else branches. */
    object Branch extends Node {

      /**
       * Creates a type-checked `if-else` branch.
       * @param cond Must be a boolean expression.
       * @param thn  Then branch (must be a term).
       * @param els  Else branch (must be a term) - use `Unit` for one-sided branches.
       * @return `if (cond) thn else els`.
       */
      def apply(cond: u.Tree, thn: u.Tree, els: u.Tree = Term.unit): u.If = {
        assert(is.defined(cond), s"$this condition is not defined")
        assert(is.defined(thn),  s"$this then is not defined")
        assert(is.defined(els),  s"$this else is not defined")
        assert(is.term(cond),    s"$this condition is not a term:\n${Tree.show(cond)}")
        assert(is.term(thn),     s"$this then is not a term:\n${Tree.show(thn)}")
        assert(is.term(els),     s"$this else is not a term:\n${Tree.show(els)}")
        assert(has.tpe(cond),    s"$this condition has no type:\n${Tree.showTypes(cond)}")
        assert(has.tpe(thn),     s"$this then has no type:\n${Tree.showTypes(thn)}")
        assert(has.tpe(els),     s"$this else has no type:\n${Tree.showTypes(els)}")
        assert(cond.tpe <:< Type.bool, s"$this condition is not boolean:\n${Tree.showTypes(cond)}")
        val branch = u.If(cond, thn, els)
        setType(branch, Type.lub(Seq(thn.tpe, els.tpe)))
      }

      def unapply(branch: u.If): Option[(u.Tree, u.Tree, u.Tree)] = branch match {
        // Avoid matching loop branches
        case WhileBody(_, _, _) => None
        case u.If(_, LoopCall(_), Lit(())) => None
        case u.If(Term(cond), Term(thn), Term(els)) => Some(cond, thn, els)
        case _ => None
      }
    }
  }
}
