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
package compiler

import api._
import ast.AST
import io.csv.CSVConverter
import io.parquet.ParquetConverter

import org.scalactic._
import org.scalactic.Accumulation._

/** Common IR tools. */
trait Common extends AST {

  import universe._

  // --------------------------------------------------------------------------
  // Emma API
  // --------------------------------------------------------------------------

  val rootPkg = "org.emmalanguage"

  trait IRModule {
    def module: u.ModuleSymbol
    def ops: Set[u.MethodSymbol]

    protected def op(name: String): u.MethodSymbol =
      methodIn(module, name)

    protected def methodIn(target: u.Symbol, name: String): u.MethodSymbol =
      target.info.member(api.TermName(name)).asMethod
  }

  /** A set of API method symbols to be comprehended. */
  protected[emmalanguage] object API extends IRModule {
    //@formatter:off
    val emmaModuleSymbol      = api.Sym[emma.`package`.type].asModule
    val csvPkgSymbol          = rootMirror.staticPackage(s"$rootPkg.io.csv")
    val bagSymbol             = api.Sym[DataBag[Any]].asClass
    val groupSymbol           = api.Sym[Group[Any, Any]].asClass
    val bagModuleSymbol       = bagSymbol.companion.asModule
    val scalaSeqModuleSymbol  = api.Sym[ScalaSeq.type].asModule
    def module                = emmaModuleSymbol

    private def bagOp(name: String) =
      methodIn(bagSymbol, name)

    // Sources
    val empty                 = methodIn(bagModuleSymbol, "empty")
    val apply                 = methodIn(bagModuleSymbol, "apply")
    val readCSV               = methodIn(bagModuleSymbol, "readCSV")
    val readParquet           = methodIn(bagModuleSymbol, "readParquet")
    val readText              = methodIn(bagModuleSymbol, "readText")
    // Sinks
    val fetch                 = bagOp("fetch")
    val as                    = bagOp("as")
    val writeCSV              = bagOp("writeCSV")
    val writeParquet          = bagOp("writeParquet")
    val writeText             = bagOp("writeText")
    // Monad ops
    val map                   = bagOp("map")
    val flatMap               = bagOp("flatMap")
    val withFilter            = bagOp("withFilter")
    // Grouping
    val groupBy               = bagOp("groupBy")
    // Set operations
    val union                 = bagOp("union")
    val distinct              = bagOp("distinct")
    // Structural recursion & Folds
    val fold                  = bagOp("fold")
    val isEmpty               = bagOp("isEmpty")
    val nonEmpty              = bagOp("nonEmpty")
    val reduce                = bagOp("reduce")
    val reduceOption          = bagOp("reduceOption")
    val min                   = bagOp("min")
    val max                   = bagOp("max")
    val sum                   = bagOp("sum")
    val product               = bagOp("product")
    val size                  = bagOp("size")
    val count                 = bagOp("count")
    val exists                = bagOp("exists")
    val forall                = bagOp("forall")
    val find                  = bagOp("find")
    val bottom                = bagOp("bottom")
    val top                   = bagOp("top")
    val sample                = bagOp("sample")

    val sourceOps             = Set(empty, apply, readCSV, readParquet, readText)
    val sinkOps               = Set(fetch, as, writeCSV, writeParquet, writeText)
    val monadOps              = Set(map, flatMap, withFilter)
    val nestOps               = Set(groupBy)
    val setOps                = Set(union, distinct)
    val foldOps               = Set(
      fold,
      isEmpty, nonEmpty,
      reduce, reduceOption,
      min, max, sum, product,
      size, count,
      exists, forall,
      find, top, bottom,
      sample
    )

    val ops = sourceOps | sinkOps | monadOps | nestOps | setOps | foldOps

    val implicitTypes = Set(
      api.Type[org.emmalanguage.api.Meta[Any]].typeConstructor,
      api.Type[ScalaEnv],
      api.Type[CSVConverter[Any]].typeConstructor,
      api.Type[ParquetConverter[Any]].typeConstructor
    )

    // Type constructors
    val DataBag               = api.Type[DataBag[Any]].typeConstructor
    val Group                 = api.Type[Group[Any, Any]].typeConstructor

    // Backend-only operations
    val byFetch               = methodIn(scalaSeqModuleSymbol, "byFetch")
    //@formatter:on
  }

  protected[emmalanguage] object MutableBagAPI extends IRModule {
    //@formatter:off
    val module                = api.Sym[org.emmalanguage.api.MutableBag.type].asModule
    val clzSym                = api.Sym[org.emmalanguage.api.MutableBag[Any, Any]].asClass

    val apply                 = op("apply")
    val update                = methodIn(clzSym, "update")
    val bag                   = methodIn(clzSym, "bag")
    val copy                  = methodIn(clzSym, "copy")

    val ops                   = Set(apply, update, bag, copy)
    //@formatter:on
  }

  protected[emmalanguage] object ComprehensionSyntax extends IRModule {
    //@formatter:off
    val module                = api.Sym[ir.ComprehensionSyntax.type].asModule

    val flatten               = op("flatten")
    val generator             = op("generator")
    val comprehension         = op("comprehension")
    val guard                 = op("guard")
    val head                  = op("head")

    val ops                   = Set(flatten, generator, comprehension, guard, head)
    //@formatter:on
  }

  protected[emmalanguage] object ComprehensionCombinators extends IRModule {
    //@formatter:off
    val module                = api.Sym[ir.ComprehensionCombinators.type].asModule

    val cross                 = op("cross")
    val equiJoin              = op("equiJoin")

    val ops                   = Set(cross, equiJoin)
    //@formatter:on
  }

  protected[emmalanguage] object Runtime extends IRModule {
    //@formatter:off
    val module                = api.Sym[ir.Runtime.type].asModule

    val cache                 = op("cache")

    val ops                   = Set(cache)
    //@formatter:on
  }

  protected[emmalanguage] object GraphRepresentation extends IRModule {
    //@formatter:off
    val module  = api.Sym[ir.GraphRepresentation.type].asModule

    val phi     = op("phi")

    val ops     = Set(phi)
    //@formatter:on
  }

  protected[emmalanguage] object DSCFAnnotations extends IRModule {
    import ir.DSCFAnnotations._
    //@formatter:off
    val module        = api.Sym[ir.DSCFAnnotations.type].asModule

    // Annotation symbols
    val branch        = api.Sym[branch].asClass
    val loop          = api.Sym[loop].asClass
    val suffix        = api.Sym[suffix].asClass
    val thenBranch    = api.Sym[thenBranch].asClass
    val elseBranch    = api.Sym[elseBranch].asClass
    val whileLoop     = api.Sym[whileLoop].asClass
    val doWhileLoop   = api.Sym[doWhileLoop].asClass
    val loopBody      = api.Sym[loopBody].asClass

    private def ann(sym: u.ClassSymbol) =
      u.Annotation(api.Inst(sym.toType, argss = Seq(Seq.empty)))

    // Annotation trees
    val suffixAnn     = ann(suffix)
    val thenAnn       = ann(thenBranch)
    val elseAnn       = ann(elseBranch)
    val whileAnn      = ann(whileLoop)
    val doWhileAnn    = ann(doWhileLoop)
    val loopBodyAnn   = ann(loopBody)

    val ops           = Set.empty[u.MethodSymbol]
    //@formatter:on
  }

  /** Common validation helpers. */
  object Validation {

    val ok = ()
    val pass = Good(ok)

    type Valid = Unit
    type Invalid = Every[Error]
    type Verdict = Valid Or Invalid
    type Validator = Tree =?> Verdict

    def validateAs(expected: Validator, tree: Tree,
      violation: => String = "Unexpected tree"
    ): Verdict = expected.applyOrElse(tree, (unexpected: Tree) => {
      Bad(One(Error(unexpected, violation)))
    })

    def oneOf(allowed: Validator*): Validator =
      allowed.reduceLeft(_ orElse _)

    case class Error(at: Tree, violation: String) {
      override def toString = s"$violation:\n${api.Tree.show(at)}"
    }

    case class all(trees: Seq[Tree]) {
      case class are(expected: Validator) {
        def otherwise(violation: => String): Verdict =
          if (trees.isEmpty) pass
          else trees validatedBy expected.orElse {
            case unexpected => Bad(One(Error(unexpected, violation)))
          } map (_.head)
      }
    }

    object all {
      def apply(tree: Tree, trees: Tree*): all =
        apply(tree +: trees)
    }

    implicit class And(verdict: Verdict) {
      def and(other: Verdict): Verdict =
        withGood(verdict, other)((_, _) => ok)
    }

  }

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
   * u.$Type
   * u.$method
   * }}}
   *
   * as opposed to
   *
   * {{{
   * import universe._ // or import u._
   *
   * $Type
   * $method
   * }}}
   *
   * in order to make the parts of the Emma compiler which depend on the Scala metaprogramming API explicit.
   */
  protected[emmalanguage] object UniverseImplicits {

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
