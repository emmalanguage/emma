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

import com.typesafe.config.Config

import scala.reflect.ClassTag
import scala.reflect.api.Universe
import scala.reflect.macros.Attachments

/**
 * Implements various utility functions that mitigate and/or workaround deficiencies in Scala's
 * macros and runtime reflection APIs, e.g. non-idempotent type checking, lack of hygiene,
 * capture-avoiding substitution, fully-qualified names, fresh name generation, identifying
 * closures, etc.
 *
 * This trait has to be instantiated with a [[scala.reflect.api.Universe]] type and works for both
 * runtime and compile time reflection.
 */
trait CommonAST {

  val u: Universe

  /** Syntax sugar for partial functions. */
  type =?>[-A, +B] = PartialFunction[A, B]

  import UniverseImplicits._
  import u.addFlagOps
  import u.definitions._
  import u.internal._

  // ----------------------------------------------------------------------------------------------
  // Abstract methods
  // ----------------------------------------------------------------------------------------------

  /** Meta information (attachments). */
  trait Meta {
    def all: Attachments
    def apply[T: ClassTag]: T = get[T].get
    def contains[T: ClassTag]: Boolean = all.contains[T]
    def get[T: ClassTag]: Option[T] = all.get[T]
    def remove[T: ClassTag](): Unit
    def update[T: ClassTag](att: T): Unit
  }

  private[ast] def freshNameSuffix: Char

  /** Returns `tpt` with its original field set. */
  private[ast] def setOriginal(tpt: u.TypeTree, original: u.Tree): u.TypeTree

  /** Returns the meta information associated with `sym`. */
  def meta(sym: u.Symbol): Meta

  /** Returns the meta information associated with `tree`. */
  def meta(tree: u.Tree): Meta

  /** Returns the enclosing named entity (class, method, value, etc). */
  def enclosingOwner: u.Symbol

  /** Infers an implicit value from the enclosing context (if possible). */
  def inferImplicit(tpe: u.Type): Option[u.Tree]

  /** Raises a compiler warning. */
  def warning(msg: String, pos: u.Position = u.NoPosition): Unit

  /** Raises an error and terminates compilation. */
  def abort(msg: String, pos: u.Position = u.NoPosition): Nothing

  /** Parses a snippet of source code and returns the AST. */
  def parse(code: String): u.Tree

  /** Type-checks a `tree` (use `typeMode=true` for type-trees). */
  def typeCheck(tree: u.Tree, typeMode: Boolean = false): u.Tree

  /** Removes all type and symbol attributes from a `tree`. */
  // FIXME: Replace with `c.untypecheck` or `tb.untypecheck` once SI-5464 is resolved.
  def unTypeCheck(tree: u.Tree): u.Tree =
    parse(u.showCode(tree, printRootPkg = true))

  /**
   * Evaluates a snippet of code and returns a value of type `T`.
   * Note: this can be called on type--checked trees (as opposed to the eval method in ToolBox).
   */
  def eval[T](code: u.Tree): T

  // ----------------------------------------------------------------------------------------------
  // Property checks
  // ----------------------------------------------------------------------------------------------

  /** Constant limits. */
  object Max {

    /** Maximum number of lambda arguments. */
    val FunParams = FunctionClass.seq.size

    /** Maximum number of tuple elements. */
    val TupleElems = TupleClass.seq.size
  }

  object is {

    /** Does `sym` have the property encoded as flag(s)? */
    def apply(property: u.FlagSet, sym: u.Symbol): Boolean =
      are(property, flags(sym))

    /** The opposite of `is(property, sym)`. */
    def not(property: u.FlagSet, sym: u.Symbol): Boolean =
      are.not(property, flags(sym))

    /** Is `name` non-degenerate? */
    def defined(name: u.Name): Boolean =
      name != null && name.toString.nonEmpty

    /** Is `pos` non-degenerate? */
    def defined(pos: u.Position): Boolean =
      pos != null && pos != u.NoPosition

    /** Is `sym` non-degenerate? */
    def defined(sym: u.Symbol): Boolean =
      sym != null && sym != u.NoSymbol

    /** Is `tree` non-degenerate? */
    def defined(tree: u.Tree): Boolean =
      tree != null && tree.nonEmpty

    /** Is `tpe` non-degenerate? */
    def defined(tpe: u.Type): Boolean =
      tpe != null && tpe != u.NoType

    /** Is `tpe` the type of a case class? */
    def caseClass(tpe: u.Type): Boolean =
      tpe.typeSymbol match {
        case cls: u.ClassSymbol => cls.isCaseClass
        case _ => false
      }

    /** Is `name` a legal identifier (i.e. consisting only of word `\w+` characters)? */
    def encoded(name: u.Name): Boolean =
      name == name.encodedName

    /** Is `sym` a legal identifier (i.e. having an `encoded` name)? */
    def encoded(sym: u.Symbol): Boolean =
      is.encoded(sym.name)

    /** Is `sym` local (its owner being a binding or a method)? */
    def local(sym: u.Symbol): Boolean = {
      val owner = sym.owner
      !owner.isPackage && !owner.isClass && !owner.isModule
    }

    /** Is `sym` overloaded (i.e. having variants with different type signatures). */
    def overloaded(sym: u.Symbol): Boolean =
      sym.isTerm && sym.asTerm.isOverloaded

    /** Is `sym` the `_root_` symbol? */
    def root(sym: u.Symbol): Boolean =
      // This is tricky for Java classes
      sym.name == u.termNames.ROOTPKG || sym.name == u.rootMirror.RootClass.name

    /** Is `sym` a binding symbol (i.e. value, variable or parameter)? */
    def binding(sym: u.Symbol): Boolean = sym.isTerm && {
      val term = sym.asTerm
      term.isVal || term.isVar || term.isParameter
    }

    /** Is `sym` a value (`val`) symbol? */
    def value(sym: u.Symbol): Boolean =
      sym.isTerm && !sym.isParameter && sym.asTerm.isVal

    /** Is `sym` a variable (`var`) symbol? */
    def variable(sym: u.Symbol): Boolean =
      sym.isTerm && sym.asTerm.isVar

    /** Is `sym` a label (loop) symbol? */
    def label(sym: u.Symbol): Boolean =
      sym.isMethod && is(u.Flag.CONTRAVARIANT, sym)

    /** Is `sym` a method (`def`) symbol? */
    def method(sym: u.Symbol): Boolean =
      sym.isMethod && is.not(u.Flag.CONTRAVARIANT, sym)

    /** Is `sym` a by-name parameter? */
    def byName(sym: u.Symbol): Boolean =
      sym.isTerm && sym.asTerm.isByNameParam

    /** Is `sym` a stable identifier? */
    def stable(sym: u.Symbol): Boolean =
      sym.isTerm && sym.asTerm.isStable

    /** Is `tpe` legal for a term (i.e. not of a higher kind or method)? */
    def result(tpe: u.Type): Boolean =
      !tpe.takesTypeArgs && !is.method(tpe)

    /** Is `tpe` the type of a method (illegal for a term)? */
    def method(tpe: u.Type): Boolean =
      !(tpe =:= tpe.finalResultType)

    /** Is `tpe` a stable path? */
    def stable(tpe: u.Type): Boolean = tpe match {
      case u.ThisType(_)      => true
      case u.SuperType(_, _)  => true
      case u.SingleType(_, _) => true
      case _ => false
    }

    /** Does `tree` define a symbol that owns the children of `tree`? */
    def owner(tree: u.Tree): Boolean = tree match {
      case _: u.Bind     => false
      case _: u.Function => true
      case _: u.LabelDef => false
      case _ => tree.isDef
    }

    /** Is `tree` a statement? */
    def stat(tree: u.Tree): Boolean = tree match {
      case _: u.Assign   => true
      case _: u.Bind     => false
      case _: u.LabelDef => true
      case _ => tree.isDef
    }

    /** Is `tree` a term? */
    def term(tree: u.Tree): Boolean = tree match {
      case id: u.Ident       => is.defined(id.symbol) && id.symbol.isTerm && is.result(id.tpe)
      case sel: u.Select     => is.defined(sel.symbol) && sel.symbol.isTerm && is.result(sel.tpe)
      case app: u.Apply      => is.result(app.tpe)
      case tapp: u.TypeApply => is.result(tapp.tpe)
      case _: u.Assign       => false
      case _: u.Bind         => false
      case _: u.LabelDef     => false
      case _: u.New          => false
      case _ => tree.isTerm
    }

    /** Is `tree` a valid pattern? */
    def pattern(tree: u.Tree): Boolean = tree match {
      case u.Ident(u.termNames.WILDCARD) => true
      case id: u.Ident =>
        is.stable(id.symbol) && is.result(id.tpe)
      case u.Apply(target, args) =>
        target.isType && args.nonEmpty && is.result(tree.tpe)
      case _: u.Alternative => true
      case _: u.Bind        => true
      case _: u.Literal     => true
      case _: u.Typed       => true
      case _: u.UnApply     => true
      case _ => false
    }
  }

  object are {

    /** Are `flags` a superset of `property`? */
    def apply(property: u.FlagSet, flags: u.FlagSet): Boolean =
      (flags | property) == flags

    /** The opposite of `are(property, flags)`. */
    def not(property: u.FlagSet, flags: u.FlagSet): Boolean =
      (flags | property) != flags

    /** Are all `trees` non-degenerate? */
    def defined(trees: Traversable[u.Tree]): Boolean =
      trees.forall(is.defined)

    /** Are all `trees` statements? */
    def stats(trees: Traversable[u.Tree]): Boolean =
      trees.forall(is.stat)

    /** Are all `trees` terms? */
    def terms(trees: Traversable[u.Tree]): Boolean =
      trees.forall(is.term)

    /** Are all `trees` valid patterns? */
    def patterns(trees: Traversable[u.Tree]): Boolean =
      trees.forall(is.pattern)
  }

  object has {

    /** Does `sym` have an owner? */
    def own(sym: u.Symbol): Boolean =
      is.defined(sym.owner)

    /** Does `sym` have a name? */
    def nme(sym: u.Symbol): Boolean =
      is.defined(sym.name)

    /** Does `sym` have a type? */
    def tpe(sym: u.Symbol): Boolean =
      is.defined(sym.info)

    /** Does `sym` have a position? */
    def pos(sym: u.Symbol): Boolean =
      is.defined(sym.pos)

    /** Does `tree` reference a symbol? */
    def sym(tree: u.Tree): Boolean =
      is.defined(tree.symbol)

    /** Does `tree` have a type? */
    def tpe(tree: u.Tree): Boolean =
      is.defined(tree.tpe)

    /** Does `tree` have a position? */
    def pos(tree: u.Tree): Boolean =
      is.defined(tree.pos)
  }

  object have {

    /** Do all `symbols` have an owner? */
    def own(symbols: Traversable[u.Symbol]): Boolean =
      symbols.forall(has.own)

    /** Do all `symbols` have a name? */
    def nme(symbols: Traversable[u.Symbol]): Boolean =
      symbols.forall(has.nme)

    /** Do all `trees` have a symbol? */
    def sym(trees: Traversable[u.Tree]): Boolean =
      trees.forall(has.sym)

    /** Do all `trees` have a type? */
    def tpe(trees: Traversable[u.Tree]): Boolean =
      trees.forall(has.tpe)

    /** Do all `trees` have a position? */
    def pos(trees: Traversable[u.Tree]): Boolean =
      trees.forall(has.pos)
  }

  // ----------------------------------------------------------------------------------------------
  // Flags
  // ----------------------------------------------------------------------------------------------

  lazy val FlagsNoSynthetic =
    Flags - u.Flag.SYNTHETIC

  /** All explicit flags. */
  lazy val Flags = Set(
    u.Flag.ABSOVERRIDE,
    u.Flag.ABSTRACT,
    u.Flag.ARTIFACT,
    u.Flag.BYNAMEPARAM,
    u.Flag.CASE,
    u.Flag.CASEACCESSOR,
    u.Flag.ARTIFACT,
    u.Flag.CONTRAVARIANT,
    u.Flag.COVARIANT,
    u.Flag.DEFAULTINIT,
    u.Flag.DEFAULTPARAM,
    u.Flag.DEFERRED,
    u.Flag.ENUM,
    u.Flag.FINAL,
    u.Flag.IMPLICIT,
    u.Flag.INTERFACE,
    u.Flag.LAZY,
    u.Flag.LOCAL,
    u.Flag.MACRO,
    u.Flag.MUTABLE,
    u.Flag.OVERRIDE,
    u.Flag.PARAM,
    u.Flag.PARAMACCESSOR,
    u.Flag.PRIVATE,
    u.Flag.PROTECTED,
    u.Flag.SEALED,
    u.Flag.STABLE,
    u.Flag.SYNTHETIC,
    u.Flag.TRAIT)

  // ----------------------------------------------------------------------------------------------
  // Virtual nodes
  // ----------------------------------------------------------------------------------------------

  /** Common parent for all virtual AST nodes. */
  trait Node {
    override def toString: String =
      getClass.getSimpleName.dropRight(1)
  }

  // ----------------------------------------------------------------------------------------------
  // Transformations
  // ----------------------------------------------------------------------------------------------

  type Xfrm = TreeTransform
  final val Xfrm = TreeTransform

  //@formatter:off
  sealed trait TreeTransform {
    def name: String
    def time: Boolean
  }
  private case class XfrmSeq(name: String, seq: Seq[Xfrm], time: Boolean = false) extends Xfrm
  private case class XfrmFun(name: String, fun: Xfrm.Fun, time: Boolean = false) extends Xfrm

  trait XfrmAlg[B] {
    def seq(name: String, seq: Seq[B], time: Boolean): B
    def fun(name: String, fun: Xfrm.Fun, time: Boolean): B
  }
  //@formatter:on

  object TreeTransform {
    type Fun = u.Tree => u.Tree

    def fold[B](a: XfrmAlg[B])(xfrm: Xfrm): B = xfrm match {
      case XfrmSeq(name, seq, time) => a.seq(name, seq.map(fold(a)), time)
      case XfrmFun(name, fun, time) => a.fun(name, fun, time)
    }

    def apply(name: String, seq: Seq[Xfrm]): Xfrm =
      XfrmSeq(name, seq)

    def apply(name: String, fun: Xfrm.Fun): Xfrm =
      XfrmFun(name, fun)
  }

  trait XfrmEval {
    type Dom
    type Inf
    type Xtr = scalaz.Tree[Inf]

    case class Res(tree: u.Tree, xtra: Xtr)

    case class Acc(tree: u.Tree, accx: Stream[Xtr] = Stream.empty[Xtr])

    val eval: Xfrm => Dom

    def apply(xfrm: Xfrm)(tree: u.Tree): u.Tree
  }

  /** Just evaluate the tree transform. */
  object NaiveEval extends XfrmEval {
    override type Dom = u.Tree => u.Tree
    override type Inf = Unit

    override val eval = Xfrm.fold(new XfrmAlg[Dom] {
      def seq(name: String, seq: Seq[Dom], time: Boolean): Dom =
        Function.chain(seq)

      def fun(name: String, fun: Xfrm.Fun, time: Boolean): Dom =
        fun
    }) _

    override def apply(xfrm: Xfrm)(tree: u.Tree): u.Tree =
      eval(xfrm)(tree)
  }

  /** Evaluate and time each node. */
  object TimerEval extends XfrmEval {
    type Dom = u.Tree => Res

    case class Inf(name: String, time: Long)

    implicit val showInf = scalaz.Show.shows[Inf](x => {
      s"${x.name} (${x.time / 1e6}ms)"
    })

    val eval = Xfrm.fold(new XfrmAlg[Dom] {
      def seq(name: String, seq: Seq[Dom], time: Boolean): Dom = tree => {
        val tim1 = System.nanoTime
        val rslt = seq.foldLeft(Acc(tree))((acc, fun) => {
          val rslt = fun(acc.tree)
          Acc(rslt.tree, acc.accx :+ rslt.xtra)
        })
        val tim2 = System.nanoTime
        val info = Inf(name, tim2 - tim1)
        Res(rslt.tree, scalaz.Tree.Node(info, rslt.accx))
      }

      def fun(name: String, fun: Xfrm.Fun, time: Boolean): Dom = tree => {
        val tim1 = System.nanoTime
        val rslt = fun(tree)
        val tim2 = System.nanoTime
        val info = Inf(name, tim2 - tim1)
        Res(rslt, scalaz.Tree.Leaf(info))
      }
    }) _

    def apply(xfrm: Xfrm)(tree: u.Tree): u.Tree = {
      val rslt = eval(xfrm)(tree)
      print(rslt.xtra.drawTree)
      rslt.tree
    }
  }

  implicit class XfrmOps(xfrm: Xfrm) extends (u.Tree => u.Tree) {
    def apply(tree: u.Tree): u.Tree =
      NaiveEval(xfrm)(tree)

    def timed: Xfrm = xfrm match {
      case xfrm: XfrmSeq => xfrm.copy(time = true)
      case xfrm: XfrmFun => xfrm.copy(time = true)
    }

    def iff(k: String)(implicit cfg: Config): Is =
      new Is(k)

    class Is(k: String)(implicit cfg: Config) {
      def is(v: Boolean): Xfrm =
        if (cfg.getBoolean(k) == v) xfrm
        else noop

      def is(v: String): Xfrm =
        if (cfg.getString(k) == v) xfrm
        else noop
    }

  }

  val noop = TreeTransform("Predef.identity", Predef.identity[u.Tree] _)

  // --------------------------------------------------------------------------
  // Universe API that needs to be imported
  // --------------------------------------------------------------------------

  /**
   * Factors out the implicit objects that need to be imported from the Scala universe.
   *
   * Please use
   *
   * {{{
   * import UniverseImplicits._
   *
   * u.Type
   * u.method
   * }}}
   *
   * as opposed to
   *
   * {{{
   * import universe._ // or import u._
   *
   * Type
   * method
   * }}}
   *
   * in order to make the parts of the Emma compiler which depend on the Scala metaprogramming API explicit.
   */
  protected[emmalanguage] object UniverseImplicits {

    import u._

    import scala.reflect.ClassTag

    // Tags for Types.
    implicit val AnnotatedTypeTag: ClassTag[AnnotatedType] = u.AnnotatedTypeTag
    implicit val BoundedWildcardTypeTag: ClassTag[BoundedWildcardType] = u.BoundedWildcardTypeTag
    implicit val ClassInfoTypeTag: ClassTag[ClassInfoType] = u.ClassInfoTypeTag
    implicit val CompoundTypeTag: ClassTag[CompoundType] = u.CompoundTypeTag
    implicit val ConstantTypeTag: ClassTag[ConstantType] = u.ConstantTypeTag
    implicit val ExistentialTypeTag: ClassTag[ExistentialType] = u.ExistentialTypeTag
    implicit val MethodTypeTag: ClassTag[MethodType] = u.MethodTypeTag
    implicit val NullaryMethodTypeTag: ClassTag[NullaryMethodType] = u.NullaryMethodTypeTag
    implicit val PolyTypeTag: ClassTag[PolyType] = u.PolyTypeTag
    implicit val RefinedTypeTag: ClassTag[RefinedType] = u.RefinedTypeTag
    implicit val SingleTypeTag: ClassTag[SingleType] = u.SingleTypeTag
    implicit val SingletonTypeTag: ClassTag[SingletonType] = u.SingletonTypeTag
    implicit val SuperTypeTag: ClassTag[SuperType] = u.SuperTypeTag
    implicit val ThisTypeTag: ClassTag[ThisType] = u.ThisTypeTag
    implicit val TypeBoundsTag: ClassTag[TypeBounds] = u.TypeBoundsTag
    implicit val TypeRefTag: ClassTag[TypeRef] = u.TypeRefTag
    implicit val TypeTagg: ClassTag[Type] = u.TypeTagg

    // Tags for Names.
    implicit val NameTag: ClassTag[Name] = u.NameTag
    implicit val TermNameTag: ClassTag[TermName] = u.TermNameTag
    implicit val TypeNameTag: ClassTag[TypeName] = u.TypeNameTag

    // Tags for Scopes.
    implicit val ScopeTag: ClassTag[Scope] = u.ScopeTag
    implicit val MemberScopeTag: ClassTag[MemberScope] = u.MemberScopeTag

    // Tags for Annotations.
    implicit val AnnotationTag: ClassTag[Annotation] = u.AnnotationTag
    // deprecated in the Scala Universe:
    // implicit val JavaArgumentTag: ClassTag[JavaArgument] = u.JavaArgumentTag
    // implicit val LiteralArgumentTag: ClassTag[LiteralArgument] = u.LiteralArgumentTag
    // implicit val ArrayArgumentTag: ClassTag[ArrayArgument] = u.ArrayArgumentTag
    // implicit val NestedArgumentTag: ClassTag[NestedArgument] = u.NestedArgumentTag

    // Tags for Symbols.
    implicit val TermSymbolTag: ClassTag[TermSymbol] = u.TermSymbolTag
    implicit val MethodSymbolTag: ClassTag[MethodSymbol] = u.MethodSymbolTag
    implicit val SymbolTag: ClassTag[Symbol] = u.SymbolTag
    implicit val TypeSymbolTag: ClassTag[TypeSymbol] = u.TypeSymbolTag
    implicit val ModuleSymbolTag: ClassTag[ModuleSymbol] = u.ModuleSymbolTag
    implicit val ClassSymbolTag: ClassTag[ClassSymbol] = u.ClassSymbolTag

    // Tags for misc Tree relatives.
    implicit val PositionTag: ClassTag[Position] = u.PositionTag
    implicit val ConstantTag: ClassTag[Constant] = u.ConstantTag
    implicit val FlagSetTag: ClassTag[FlagSet] = u.FlagSetTag
    implicit val ModifiersTag: ClassTag[Modifiers] = u.ModifiersTag

    // Tags for Trees. WTF.
    implicit val AlternativeTag: ClassTag[Alternative] = u.AlternativeTag
    implicit val AnnotatedTag: ClassTag[Annotated] = u.AnnotatedTag
    implicit val AppliedTypeTreeTag: ClassTag[AppliedTypeTree] = u.AppliedTypeTreeTag
    implicit val ApplyTag: ClassTag[Apply] = u.ApplyTag
    implicit val AssignOrNamedArgTag: ClassTag[AssignOrNamedArg] = u.AssignOrNamedArgTag
    implicit val AssignTag: ClassTag[Assign] = u.AssignTag
    implicit val BindTag: ClassTag[Bind] = u.BindTag
    implicit val BlockTag: ClassTag[Block] = u.BlockTag
    implicit val CaseDefTag: ClassTag[CaseDef] = u.CaseDefTag
    implicit val ClassDefTag: ClassTag[ClassDef] = u.ClassDefTag
    implicit val CompoundTypeTreeTag: ClassTag[CompoundTypeTree] = u.CompoundTypeTreeTag
    implicit val DefDefTag: ClassTag[DefDef] = u.DefDefTag
    implicit val DefTreeTag: ClassTag[DefTree] = u.DefTreeTag
    implicit val ExistentialTypeTreeTag: ClassTag[ExistentialTypeTree] = u.ExistentialTypeTreeTag
    implicit val FunctionTag: ClassTag[Function] = u.FunctionTag
    implicit val GenericApplyTag: ClassTag[GenericApply] = u.GenericApplyTag
    implicit val IdentTag: ClassTag[Ident] = u.IdentTag
    implicit val IfTag: ClassTag[If] = u.IfTag
    implicit val ImplDefTag: ClassTag[ImplDef] = u.ImplDefTag
    implicit val ImportSelectorTag: ClassTag[ImportSelector] = u.ImportSelectorTag
    implicit val ImportTag: ClassTag[Import] = u.ImportTag
    implicit val LabelDefTag: ClassTag[LabelDef] = u.LabelDefTag
    implicit val LiteralTag: ClassTag[Literal] = u.LiteralTag
    implicit val MatchTag: ClassTag[Match] = u.MatchTag
    implicit val MemberDefTag: ClassTag[MemberDef] = u.MemberDefTag
    implicit val ModuleDefTag: ClassTag[ModuleDef] = u.ModuleDefTag
    implicit val NameTreeTag: ClassTag[NameTree] = u.NameTreeTag
    implicit val NewTag: ClassTag[New] = u.NewTag
    implicit val PackageDefTag: ClassTag[PackageDef] = u.PackageDefTag
    implicit val RefTreeTag: ClassTag[RefTree] = u.RefTreeTag
    implicit val ReturnTag: ClassTag[Return] = u.ReturnTag
    implicit val SelectFromTypeTreeTag: ClassTag[SelectFromTypeTree] = u.SelectFromTypeTreeTag
    implicit val SelectTag: ClassTag[Select] = u.SelectTag
    implicit val SingletonTypeTreeTag: ClassTag[SingletonTypeTree] = u.SingletonTypeTreeTag
    implicit val StarTag: ClassTag[Star] = u.StarTag
    implicit val SuperTag: ClassTag[Super] = u.SuperTag
    implicit val SymTreeTag: ClassTag[SymTree] = u.SymTreeTag
    implicit val TemplateTag: ClassTag[Template] = u.TemplateTag
    implicit val TermTreeTag: ClassTag[TermTree] = u.TermTreeTag
    implicit val ThisTag: ClassTag[This] = u.ThisTag
    implicit val ThrowTag: ClassTag[Throw] = u.ThrowTag
    implicit val TreeTag: ClassTag[Tree] = u.TreeTag
    implicit val TryTag: ClassTag[Try] = u.TryTag
    implicit val TypTreeTag: ClassTag[TypTree] = u.TypTreeTag
    implicit val TypeApplyTag: ClassTag[TypeApply] = u.TypeApplyTag
    implicit val TypeBoundsTreeTag: ClassTag[TypeBoundsTree] = u.TypeBoundsTreeTag
    implicit val TypeDefTag: ClassTag[TypeDef] = u.TypeDefTag
    implicit val TypeTreeTag: ClassTag[TypeTree] = u.TypeTreeTag
    implicit val TypedTag: ClassTag[Typed] = u.TypedTag
    implicit val UnApplyTag: ClassTag[UnApply] = u.UnApplyTag
    implicit val ValDefTag: ClassTag[ValDef] = u.ValDefTag
    implicit val ValOrDefDefTag: ClassTag[ValOrDefDef] = u.ValOrDefDefTag

    // Miscellaneous
    implicit val TreeCopierTag: ClassTag[TreeCopier] = u.TreeCopierTag
    implicit val RuntimeClassTag: ClassTag[RuntimeClass] = u.RuntimeClassTag
    implicit val MirrorTag: ClassTag[Mirror] = u.MirrorTag
  }
}
