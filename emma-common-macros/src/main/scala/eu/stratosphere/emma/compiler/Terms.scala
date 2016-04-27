package eu.stratosphere.emma
package compiler

import scala.annotation.tailrec
import scala.reflect.ClassTag

/** Utility for terms. */
trait Terms extends Util { this: Trees with Types with Symbols =>

  import universe._
  import internal.reificationSupport._

  object Term {

    // Predefined terms
    lazy val unit = lit(())
    lazy val predef = Type.check(q"${Tree.Scala}.Predef")

    /** Utility for term names. */
    object name {

      // Predefined names
      val anon = apply("anon")
      val exprOwner = apply("<expression-owner>")
      val init = termNames.CONSTRUCTOR
      val lambda = apply("anonfun")
      val local = apply(s"<local $exprOwner>")
      val root = termNames.ROOTPKG
      val wildcard = termNames.WILDCARD

      /** Returns a new term name. */
      def apply(name: String): TermName = {
        assert(name.nonEmpty, "Empty term name")
        TermName(name)
      }

      /** Returns the term name of `sym`. */
      def apply(sym: Symbol): TermName = {
        assert(Is defined sym, s"Undefined symbol: `$sym`")
        sym.name.toTermName
      }

      /** Returns an encoded version (i.e. matching `\w+`) of `name`. */
      def encoded(name: String): TermName =
        encoded(apply(name))

      /** Returns an encoded version (i.e. matching `\w+`) of `name`. */
      def encoded(name: Name): TermName =
        name.encodedName.toTermName

      /** Returns a fresh term name starting with `prefix$`. */
      def fresh(prefix: Name): TermName =
        fresh(prefix.toString)

      /** Returns a fresh term name starting with `prefix$`. */
      def fresh(prefix: String): TermName = encoded {
        if (prefix.nonEmpty && prefix.last == '$') freshTermName(prefix)
        else freshTermName(s"$prefix$$")
      }

      def unapply(name: TermName): Option[String] =
        Some(name.toString)

      /** "eta" term name extractor (cf. eta-expansion). */
      object eta {

        val pattern = """eta(\$\d+)+"""

        def unapply(name: TermName): Option[String] = {
          val str = name.toString
          if (str matches pattern) Some(str) else None
        }
      }
    }

    /** Utility for term symbols. */
    object sym {

      /** Returns a new term symbol with specific properties. */
      def apply(owner: Symbol, name: TermName, tpe: Type,
        flags: FlagSet = Flag.SYNTHETIC,
        pos: Position = NoPosition): TermSymbol = {

        assert(name.toString.nonEmpty, "Empty term name")
        assert(Is defined tpe, s"Undefined type: `$tpe`")
        val term = termSymbol(owner, name, flags, pos)
        setInfo(term, Type fix tpe)
      }

      /** Returns the term symbol of `tree`. */
      def apply(tree: Tree): TermSymbol = {
        assert(Has termSym tree, s"No term symbol found for:\n$tree")
        tree.symbol.asTerm
      }

      /** Returns a free term symbol with specific properties. */
      def free(name: TermName, tpe: Type,
        flags: FlagSet = Flag.SYNTHETIC,
        origin: String = null): FreeTermSymbol = {

        val strName = name.toString
        assert(strName.nonEmpty, "Empty term name")
        assert(Is defined tpe, s"Undefined type: `$tpe`")
        val term = newFreeTerm(strName, null, flags, origin)
        setInfo(term, Type fix tpe)
      }

      def unapply(sym: TermSymbol): Option[(TermName, FlagSet)] =
        Some(sym.name, Symbol flags sym)
    }

    /** Finds field / method `member` accessible in `target` and returns its symbol. */
    def member(target: Symbol, member: TermName): TermSymbol = {
      assert(Is valid target, s"Invalid target: `$target`")
      assert(member.toString.nonEmpty, "Unspecified term member")
      Type.of(target).member(member).asTerm
    }

    /** Finds field / method `member` accessible in `target` and returns its symbol. */
    def member(target: Tree, member: TermName): TermSymbol = {
      assert(Has tpe target, s"Untyped target:\n$target")
      assert(member.toString.nonEmpty, "Unspecified term member")
      Type.of(target).member(member).asTerm
    }

    /** Imports a term from a tree. */
    def imp(from: Tree, sym: TermSymbol): Import =
      imp(from, name(sym))

    /** Imports a term from a tree by name. */
    def imp(from: Tree, name: String): Import =
      imp(from, this.name(name))

    /** Imports a term from a tree by name. */
    def imp(from: Tree, name: TermName): Import = {
      assert(Is valid from, s"Invalid import selector:\n$from")
      assert(name.toString.nonEmpty, "Unspecified import")
      Type.check(q"import $from.$name").asInstanceOf[Import]
    }

    /** Literals (constants). */
    object lit {

      /** Returns a new literal containing `const`. */
      def apply[A](const: A): Literal =
        Type.check(Literal(Constant(const))).asInstanceOf[Literal]

      def unapply(lit: Literal): Option[Any] = lit match {
        case Literal(Constant(const: Any)) => Some(const)
        case _ => None
      }
    }

    /** Null values (considered different than literals). */
    object null_ {

      /** Returns `null` of type `T`. */
      def apply[T: TypeTag]: Tree =
        apply(Type[T])

      /** Returns `null` of type `tpe`. */
      def apply(tpe: Type): Tree = {
        assert(Is defined tpe, s"Undefined type: `$tpe`")
        Type.check(q"null.asInstanceOf[$tpe]")
      }

      def unapply(tree: Tree): Option[Type] = tree match {
        case q"null.asInstanceOf[$_]" => Some(Type of tree)
        case q"null" => Some(Type of tree)
        case _ => None
      }
    }

    /** Term references (Idents). */
    object ref {

      /** Returns a term reference to `sym` (use `quoted=true` for Unicode support). */
      def apply(sym: TermSymbol, quoted: Boolean = false): Ident = {
        assert(Is valid sym, s"Invalid symbol: `$sym`")
        val id = if (quoted) q"`$sym`".asInstanceOf[Ident] else Ident(sym)
        setType(id, Type of sym)
        setSymbol(id, sym)
      }

      def unapply(id: Ident): Option[TermSymbol] =
        if (id.isTerm) Some(Term sym id) else None
    }

    /** Term member selection. */
    object sel {

      /** Returns a new field access (Select). */
      def apply(target: Tree, member: TermSymbol, tpe: Type = NoType): Select = {
        assert(Has tpe target, s"Untyped target:\n$target")
        assert(member.toString.nonEmpty, "Unspecified term member")
        val sel = Select(target, member)
        val result =
          if (Is defined tpe) tpe
          else member.infoIn(Type of target)

        setSymbol(sel, member)
        setType(sel, result)
      }

      def unapply(sel: Select): Option[(Tree, TermSymbol)] =
        if (Has termSym sel) Some(sel.qualifier, Term sym sel) else None
    }

    /** Method calls and Function applications. */
    object app {

      /** Returns `target` applied to the (type) arguments. */
      @tailrec
      def apply(target: Tree, types: Type*)(argss: Seq[Tree]*): Apply = {
        assert(Has tpe target, s"Untyped target:\n$target")
        assert(types forall Is.defined, "Unspecified type arguments")
        assert(argss.flatten forall Has.tpe, "Untyped arguments")
        if (types.isEmpty) {
          if (argss.isEmpty) {
            val app = Apply(target, Nil)
            setType(app, Type result target)
          } else argss.foldLeft(target) { (tgt, args) =>
            val app = Apply(tgt, args.toList)
            setType(app, Type result target)
          }.asInstanceOf[Apply]
        } else {
          val typeApp = Type.app(target, types: _*)
          apply(typeApp)(argss: _*)
        }
      }

      def unapplySeq(tree: Apply): Option[(Tree, Seq[Type], Seq[Seq[Tree]])] = tree match {
        case Apply(TypeApply(fn, targs), args) =>
          Some(fn, targs map Type.of, Seq(args))
        case Apply(app: Apply, args) => unapplySeq(app) match {
          case Some((fn, targs, argss)) => Some(fn, targs, argss :+ args)
          case None => None
        }
        case Apply(fn, args) =>
          Some(fn, Nil, Seq(args))
        case _ => None
      }
    }

    /** Object creation / class instantiation. */
    object inst {

      /** Returns a new class instantiation. */
      def apply(target: TypeSymbol, types: Type*)(args: Tree*): Tree = {
        assert(Is valid target, s"Invalid target: `$target`")
        assert(types forall Is.defined, "Unspecified type arguments")
        assert(args forall Has.tpe, "Untyped arguments")
        // TODO: Handle alternatives properly
        val clazz = Type of target
        val constructor = clazz decl Term.name.init
        val T = if (types.isEmpty) clazz else Type(clazz, types: _*)
        val inst = q"new ${Tree.resolve(target)}[..$types](..$args)"
        setSymbol(inst, constructor)
        setType(inst, T)
      }

      /** Returns a new class instantiation. */
      def apply(tpe: Type, types: Type*)(args: Tree*): Tree =
        apply(tpe.typeSymbol.asType, types: _*)(args: _*)

      def unapplySeq(tree: Tree): Option[(TypeSymbol, Seq[Type], Seq[Tree])] = tree match {
        case q"new ${clazz: Tree}[..${types: Seq[Tree]}](..${args: Seq[Tree]})" =>
          Some(Type sym clazz, types map Type.of, args)
        case _ => None
      }
    }

    /** Anonymous Functions. */
    object lambda {

      /** Returns a new anonymous function. */
      def apply(args: TermSymbol*)(body: Tree*): Function = {
        assert(args forall Is.valid, "Invalid lambda parameters")
        assert(body forall Is.valid, "Invalid lambda body")
        assert(Has tpe body.last, s"Invalid expression:\n${body.last}")
        val bodyBlock = if (body.size == 1) body.head else Tree.block(body: _*)
        val types = args map Type.of
        val T = Type.fun(types: _*)(Type of bodyBlock)
        val anon = Term.sym.free(Term.name.lambda, T)
        val argFlags = Flag.SYNTHETIC | Flag.PARAM
        val params = for ((arg, tpe) <- args zip types) yield
          Term.sym(anon, arg.name, tpe, argFlags)

        val paramList = params.map(Tree.val_(_, flags = argFlags)).toList
        val rhs = Owner.at(anon)(Tree.rename(bodyBlock, args zip params: _*))
        val fun = Function(paramList, rhs)
        setSymbol(fun, anon)
        setType(fun, T)
      }

      def unapply(fun: Function): Option[(TermSymbol, Seq[TermSymbol], Tree)] =
        Some(Term sym fun, fun.vparams.map(Term sym _), fun.body)
    }
  }
}
