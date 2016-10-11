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

    import u.internal.{constantType, newFreeTerm, newTermSymbol}
    import u.internal.reificationSupport.freshTermName
    import universe._

    /** Term names. */
    object TermName extends Node {

      // Predefined term names
      lazy val anon = apply("anon")
      lazy val app = apply("apply")
      lazy val empty = u.termNames.EMPTY
      lazy val exprOwner = u.TermName("<expression-owner>")
      lazy val foreach = apply("foreach")
      lazy val init = u.termNames.CONSTRUCTOR
      lazy val lambda = apply("anonfun")
      lazy val local = u.TermName(s"<local $exprOwner>")
      lazy val root = u.termNames.ROOTPKG
      lazy val unApp = apply("unapply")
      lazy val unAppSeq = apply("unapplySeq")
      lazy val wildcard = u.termNames.WILDCARD

      /** Creates a new term name (must be non-empty). */
      def apply(name: String): u.TermName = {
        assert(name.nonEmpty, "Empty term name")
        apply(u.TermName(name))
      }

      /** Encodes `name` and converts it to a term name. */
      def apply(name: u.Name): u.TermName = {
        assert(is.defined(name), s"Undefined name `$name`")
        name.encodedName.toTermName
      }

      /** Extracts the term name of `sym`, if any. */
      def apply(sym: u.Symbol): u.TermName = {
        assert(is.defined(sym), s"Undefined symbol `$sym`")
        assert(has.name(sym), s"Symbol `$sym` has no name")
        apply(sym.name)
      }

      /** Creates a fresh term name with the given `prefix`. */
      def fresh(prefix: String): u.TermName = apply {
        if (prefix.nonEmpty && prefix.last == '$') freshTermName(prefix)
        else freshTermName(s"$prefix$$")
      }

      /** Creates a fresh term name with the given `prefix`. */
      def fresh(prefix: u.Name): u.TermName = {
        assert(is.defined(prefix), s"Undefined prefix `$prefix`")
        fresh(prefix.toString)
      }

      /** Creates a fresh term name with the given symbol's name as `prefix`. */
      def fresh(prefix: u.Symbol): u.TermName = {
        assert(is.defined(prefix), s"Undefined prefix `$prefix`")
        assert(has.name(prefix), s"Prefix `$prefix` has no name")
        fresh(prefix.name)
      }

      def unapply(name: u.TermName): Option[String] =
        for (name <- Option(name) if is.defined(name)) yield name.toString

      /** Names generated during eta expansion. */
      object Eta extends Node {

        /** Creates a fresh `eta` name. */
        def apply(): u.TermName =
          fresh("eta$")

        def unapply(name: u.TermName): Option[String] = name match {
          case TermName(str) if str.matches("""eta(\$\d+)+""") => Some(str)
          case _ => None
        }
      }

      /** Names for `while` loop labels. */
      object While extends Node {

        /** Creates a fresh `while` name. */
        def apply(): u.TermName =
          fresh("while$")

        def unapply(name: u.TermName): Option[String] = name match {
          case TermName(str) if str.matches("""while(\$\d+)+""") => Some(str)
          case _ => None
        }
      }

      /** Names for `do-while` loop labels. */
      object DoWhile extends Node {

        /** Creates a fresh `do-while` name. */
        def apply(): u.TermName =
          fresh("doWhile$")

        def unapply(name: u.TermName): Option[String] = name match {
          case TermName(str) if str.matches("""doWhile(\$\d+)+""") => Some(str)
          case _ => None
        }
      }
    }

    /** Term symbols. */
    object TermSym extends Node {

      /**
       * Creates a new term symbol.
       * @param owner The symbol of the enclosing named entity where this term is defined.
       * @param name The name of this term (will be encoded).
       * @param tpe The type of this term (will be dealiased and widened).
       * @param flags Any additional modifiers (e.g. mutable, parameter, implicit).
       * @param pos The (optional) source code position where this term is defined.
       * @return A new term symbol.
       */
      def apply(owner: u.Symbol, name: u.TermName, tpe: u.Type,
        flags: u.FlagSet = u.NoFlags,
        pos: u.Position = u.NoPosition): u.TermSymbol = {

        assert(is.defined(name), s"$this name `$name` is not defined")
        assert(is.defined(tpe), s"$this type `$tpe` is not defined")

        val sym = newTermSymbol(owner, TermName(name), pos, flags)
        set.tpe(sym, Type.fix(tpe))
        sym
      }

      /** Extracts the term symbol of `tree`, if any. */
      def of[T <: u.Tree](tree: T): u.TermSymbol = {
        val sym = Sym.of(tree)
        assert(is.term(sym), s"Symbol `$sym` is not a term")
        sym.asTerm
      }

      /** Creates a free term symbol (without an owner). */
      def free(name: u.TermName, tpe: u.Type, flags: u.FlagSet = u.NoFlags): u.FreeTermSymbol = {
        assert(is.defined(name), s"$this name `$name` is not defined")
        assert(is.defined(tpe), s"$this type `$tpe` is not defined")

        val free = newFreeTerm(TermName(name).toString, null, flags, null)
        set.tpe(free, Type.fix(tpe))
        free
      }

      /** Creates a free term symbol with the same attributes as the `original`. */
      def free(original: u.TermSymbol): u.FreeTermSymbol =
        free(TermName(original), original.info, get.flags(original))

      /** Creates a fresh term symbol with the same attributes as the `original`. */
      def fresh(original: u.TermSymbol): u.TermSymbol =
        Sym.copy(original)(name = TermName.fresh(original)).asTerm

      def unapply(sym: u.TermSymbol): Option[u.TermSymbol] =
        Option(sym)
    }

    object Term extends Node {

      // Predefined terms
      lazy val unit = Lit(())

      /** Looks for `member` in `target` and returns its symbol (possibly overloaded). */
      def member(target: u.Type, member: u.TermName): u.TermSymbol = {
        assert(is.defined(target), s"Undefined target `$target`")
        assert(is.defined(member), s"Undefined term member `$member`")
        Type.fix(target).member(member).asTerm
      }

      /** Looks for `member` in `target` and returns its symbol (possibly overloaded). */
      def member(target: u.Symbol, member: u.TermName): u.TermSymbol = {
        assert(is.defined(target), s"Undefined target `$target`")
        assert(has.tpe(target), s"Target `$target` has no type")
        Term.member(target.info, member)
      }

      def unapply(tree: u.Tree): Option[u.Tree] =
        Option(tree).filter(is.term)
    }

    /** Term applications (for internal use). */
    private[ast] object TermApp extends Node {

      def apply(target: u.Tree, args: Seq[u.Tree]): u.Apply = {
        assert(is.defined(target), s"$this target is not defined: $target")
        assert(has.sym(target), s"$this target has no symbol:\n${Tree.showSymbols(target)}")
        assert(has.tpe(target), s"$this target has no type:\n${Tree.showTypes(target)}")
        assert(are.defined(args), s"Not all $this args are defined")
        assert(are.terms(args), s"Not all $this args are terms")
        assert(have.tpe(args), s"Not all $this args have type")

        val app = u.Apply(target, args.toList)
        set(app, sym = Sym.of(target), tpe = Type.of(target).resultType)
        app
      }

      def apply(target: u.Tree, targs: u.Type*)(argss: Seq[u.Tree]*): u.Tree = {
        assert(is.defined(target), s"$this target is not defined: $target")
        assert(has.tpe(target), s"$this target has no type:\n${Tree.showTypes(target)}")

        if (targs.isEmpty) {
          if (argss.isEmpty) Tree.copy(target)(tpe = Type.of(target).resultType)
          else argss.foldLeft(target)(apply)
        } else apply(TypeApp(target, targs: _*))(argss: _*)
      }

      def unapplySeq(tree: u.Tree): Option[(u.Tree, Seq[u.Type], Seq[Seq[u.Tree]])] = tree match {
        case u.Apply(TermApp(target, targs, argss@_*), args) => Some(target, targs, argss :+ args)
        case u.Apply(target, args) => Some(target, Nil, Seq(args))
        case TypeApp(target, targs@_*) => Some(target, targs, Nil)
        case _ => None
      }
    }

    /** Type applications (for internal use). */
    private[ast] object TypeApp extends Node {

      def apply(target: u.Tree, targs: u.Type*): u.TypeApply = {
        assert(is.defined(target), s"$this target is not defined: $target")
        assert(has.tpe(target), s"$this target has no type:\n${Tree.showTypes(target)}")
        assert(targs.nonEmpty, s"No type args supplied to $this")
        assert(targs.forall(is.defined), s"Not all $this type args are defined")

        val tpts = targs.map(TypeQuote(_)).toList
        val tapp = u.TypeApply(target, tpts)
        set(tapp, tpe = Type(target.tpe, targs: _*))
        tapp
      }

      def unapplySeq(tapp: u.TypeApply): Option[(u.Tree, Seq[u.Type])] =
        Some(tapp.fun, tapp.args.map(Type.of))
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

    /**
     * Term accesses (values, variables, parameters and modules).
     *
     * NOTE: All terms except fields with `private[this]` visibility and objects are accessed via
     * getter methods (thus covered by [[DefCall]]).
     */
    private[ast] object TermAcc extends Node {

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
        case lit @ Lit(_) => Some(lit)
        case ref @ TermRef(_) => Some(ref)
        case ths @ This(_) => Some(ths)
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
        val const = u.Constant(value)
        val tpe = if (() == value) Type.unit else constantType(const)
        val lit = u.Literal(const)
        set(lit, tpe = tpe)
        lit
      }

      def unapply(lit: u.Literal): Option[Any] =
        Some(lit.value.value)
    }

    /** `this` references to enclosing classes or objects. */
    object This extends Node {

      /**
       * Creates a type-checked `this` reference.
       * @param sym The symbol of the enclosing class or object.
       * @return `this.sym`.
       */
      def apply(sym: u.Symbol): u.This = {
        assert(is.defined(sym), s"$this `$sym` is not defined")
        assert(has.name(sym), s"$this `$sym` has no name")
        assert(is.clazz(sym) || is.module(sym), s"$this `$sym` is neither a class nor a module")

        val ths = u.This(if (is.clazz(sym)) sym.asType.name else TypeName.empty)
        set(ths, sym = sym, tpe = Type.thisOf(sym))
        ths
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
        assert(is.defined(expr), s"$this expr is not defined: $expr")
        assert(is.term(expr), s"$this expr is not a term:\n${Tree.show(expr)}")
        assert(has.tpe(expr), s"$this expr has no type:\n${Tree.showTypes(expr)}")
        assert(is.defined(tpe), s"$this type `$tpe` is not defined")
        lazy val (lhT, rhT) = (Type.of(expr), Type.fix(tpe))
        assert(lhT weak_<:< rhT, s"Type `$lhT` cannot be casted to `$rhT`")

        val ascr = u.Typed(expr, TypeQuote(rhT))
        set(ascr, tpe = rhT)
        ascr
      }

      def unapply(ascr: u.Typed): Option[(u.Tree, u.Type)] = ascr match {
        case u.Typed(Term(expr), _) withType tpe => Some(expr, tpe)
        case _ => None
      }
    }

    /** `class` instantiations. */
    object Inst extends Node {

      /**
       * Creates a type-checked `class` instantiation.
       * @param target The type of the class to instantiate (might be path-dependent).
       * @param targs The type arguments (if `target` is generic).
       * @param argss All argument lists (partial application not supported).
       * @return `new target[..targs](...argss)`.
       */
      def apply(target: u.Type, targs: u.Type*)(argss: Seq[u.Tree]*): u.Tree = {
        assert(is.defined(target), s"$this target `$target` is not defined")
        assert(targs.forall(is.defined), s"Not all $this type args are defined")
        assert(are.defined(argss.flatten), s"Not all $this args are defined")
        assert(have.tpe(argss.flatten), s"Not all $this args have type")

        val clazz = Type.fix(target).typeConstructor
        val constructor = clazz.decl(TermName.init)
        val init = Sym.resolveOverloaded(appliedType(clazz, targs: _*))(constructor)(argss: _*)
        val tpe = Type(clazz, targs: _*)
        val tpt = u.New(TypeQuote(tpe))
        set(tpt, tpe = tpe)
        val inst = TermApp(Sel(tpt, init))(argss: _*)
        set(inst, sym = init, tpe = tpe)
        inst
      }

      def unapplySeq(tree: u.Tree): Option[(u.Type, Seq[u.Type], Seq[Seq[u.Tree]])] = tree match {
        case TermApp(Sel(u.New(_ withType clazz), _), _, argss@_*) withType Type.Result(_) =>
          Some(clazz.typeConstructor, clazz.typeArgs, argss)
        case _ => None
      }
    }

    /** Lambdas (anonymous functions). */
    object Lambda extends Node {

      /**
       * Creates a type-checked lambda.
       * @param params The symbols of all parameters (to be copied with a new owner).
       * @param body The function body (with parameter symbols substituted), owned by the lambda.
       * @return `(..params) => body`.
       */
      def apply(params: u.TermSymbol*)(body: u.Tree): u.Function = {
        assert(params.forall(is.defined), s"Not all $this parameters are defined")
        assert(have.name(params), s"Not all $this parameters have names")
        assert(params.forall(has.tpe), s"Not all $this parameters have types")
        assert(is.defined(body), s"$this body is not defined: $body")
        assert(is.term(body), s"$this body is not a term:\n${Tree.show(body)}")
        assert(has.tpe(body), s"$this body has no type:\n${Tree.showTypes(body)}")

        val parTs = params.map(_.info)
        val tpe = Type.fun(parTs: _*)(body.tpe)
        val fun = TermSym.free(TermName.lambda, tpe)
        val aliases = for ((p, t) <- params zip parTs) yield ParSym(fun, p.name, t)
        val rhs = Owner.at(fun)(Tree.renameUnsafe(params zip aliases: _*)(body))
        val lambda = u.Function(aliases.map(ParDef(_)).toList, rhs)
        set(lambda, sym = fun, tpe = tpe)
        lambda
      }

      def unapply(lambda: u.Function): Option[(u.TermSymbol, Seq[u.ValDef], u.Tree)] = lambda match {
        case u.Function(params, Term(body)) withSym TermSym(fun) => Some(fun, params, body)
        case _ => None
      }
    }

    /** Blocks. */
    object Block extends Node {

      /**
       * Creates a type-checked block.
       * @param stats Statements (`Unit`s are filtered out).
       * @param expr Must be a term (use `Unit` to simulate a statement block).
       * @return `{ ..stats; expr }`.
       */
      def apply(stats: u.Tree*)(expr: u.Tree = Term.unit): u.Block = {
        assert(are.defined(stats), s"Not all $this statements are defined")
        assert(is.defined(expr), s"$this expr is not defined: $expr")
        assert(is.term(expr), s"$this expr is not a term:\n${Tree.show(expr)}")
        assert(has.tpe(expr), s"$this expr has no type:\n${Tree.showTypes(expr)}")

        val compressed = stats.filter {
          case Lit(()) => false
          case _ => true
        }.toList
        val block = u.Block(compressed, expr)
        set(block, tpe = Type.of(expr))
        block
      }

      def unapply(block: u.Block): Option[(Seq[u.Tree], u.Tree)] = block match {
        // Avoid matching loop bodies
        case DoWhileBody(_, _, _) => None
        case u.Block(_ :: Nil, TermApp(Id(LabelSym(_)), Seq(), Seq())) => None
        case u.Block(stats, Term(expr)) => Some(stats, expr)
        case _ => None
      }
    }

    /** `if-else` branches. */
    object Branch extends Node {

      /**
       * Creates a type-checked `if-else` branch.
       * @param cond Must be a boolean expression.
       * @param thn Then branch (must be a term).
       * @param els Else branch (must be a term) - use `Unit` for one-sided branches.
       * @return `if (cond) thn else els`.
       */
      def apply(cond: u.Tree, thn: u.Tree, els: u.Tree = Term.unit): u.If = {
        assert(is.defined(cond), s"$this condition is not defined: $cond")
        assert(is.defined(thn), s"$this then is not defined: $thn")
        assert(is.defined(els), s"$this else is not defined: $els")
        assert(is.term(cond), s"$this condition is not a term:\n${Tree.show(cond)}")
        assert(is.term(thn), s"$this then is not a term:\n${Tree.show(thn)}")
        assert(is.term(els), s"$this else is not a term:\n${Tree.show(els)}")
        assert(has.tpe(cond), s"$this condition has no type:\n${Tree.showTypes(cond)}")
        assert(has.tpe(thn), s"$this then has no type:\n${Tree.showTypes(thn)}")
        assert(has.tpe(els), s"$this else has no type:\n${Tree.showTypes(els)}")
        lazy val (condT, thnT, elsT) = (Type.of(cond), Type.of(thn), Type.of(els))
        assert(condT =:= Type.bool, s"$this condition is not boolean:\n${Tree.showTypes(cond)}")

        val branch = u.If(cond, thn, els)
        set(branch, tpe = Type.weakLub(thnT, elsT))
        branch
      }

      def unapply(branch: u.If): Option[(u.Tree, u.Tree, u.Tree)] = branch match {
        // Avoid matching loop branches
        case WhileBody(_, _, _) => None
        case u.If(_, TermApp(Id(LabelSym(_)), Seq(), Seq()), Lit(())) => None
        case u.If(Term(cond), Term(thn), Term(els)) => Some(cond, thn, els)
        case _ => None
      }
    }
  }
}
