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

import ast.AST

import org.scalactic._
import org.scalactic.Accumulation._

/** Common IR tools. */
trait Common extends AST {

  import universe._

  // --------------------------------------------------------------------------
  // Emma API
  // --------------------------------------------------------------------------

  val rootPkg = "org.emmalanguage"

  /** A set of API method symbols to be comprehended. */
  protected[emmalanguage] object API {
    //@formatter:off
    val apiModuleSymbol       = rootMirror.staticModule(s"$rootPkg.api.package")
    val emmaModuleSymbol      = rootMirror.staticModule(s"$rootPkg.api.emma.package")
    val csvPkgSymbol          = rootMirror.staticPackage(s"$rootPkg.io.csv")
    val bagSymbol             = rootMirror.staticClass(s"$rootPkg.api.DataBag")
    val groupSymbol           = rootMirror.staticClass(s"$rootPkg.api.Group")
    val bagModuleSymbol       = rootMirror.staticModule(s"$rootPkg.api.DataBag")
    val scalaSeqModuleSymbol  = rootMirror.staticModule(s"$rootPkg.api.ScalaSeq")

    // Sources
    val empty                 = bagSymbol.companion.info.member(TermName("empty")).asMethod
    val apply                 = bagSymbol.companion.info.member(TermName("apply")).asMethod
    val readCSV               = bagSymbol.companion.info.member(TermName("readCSV")).asMethod
    val readParquet           = bagSymbol.companion.info.member(TermName("readParquet")).asMethod
    val readText              = bagSymbol.companion.info.member(TermName("readText")).asMethod
    // Sinks
    val fetch                 = bagSymbol.info.decl(TermName("fetch"))
    val as                    = bagSymbol.info.decl(TermName("as"))
    val writeCSV              = bagSymbol.info.decl(TermName("writeCSV"))
    val writeParquet          = bagSymbol.info.decl(TermName("writeParquet"))
    val writeText             = bagSymbol.info.decl(TermName("writeText"))
    // Monad ops
    val map                   = bagSymbol.info.decl(TermName("map"))
    val flatMap               = bagSymbol.info.decl(TermName("flatMap"))
    val withFilter            = bagSymbol.info.decl(TermName("withFilter"))
    // Grouping
    val groupBy               = bagSymbol.info.decl(TermName("groupBy"))
    // Set operations
    val union                 = bagSymbol.info.decl(TermName("union"))
    val distinct              = bagSymbol.info.decl(TermName("distinct"))
    // Structural recursion & Folds
    val fold                  = bagSymbol.info.decl(TermName("fold"))
    val isEmpty               = bagSymbol.info.decl(TermName("isEmpty"))
    val nonEmpty              = bagSymbol.info.decl(TermName("nonEmpty"))
    val reduce                = bagSymbol.info.decl(TermName("reduce"))
    val reduceOption          = bagSymbol.info.decl(TermName("reduceOption"))
    val min                   = bagSymbol.info.decl(TermName("min"))
    val max                   = bagSymbol.info.decl(TermName("max"))
    val sum                   = bagSymbol.info.decl(TermName("sum"))
    val product               = bagSymbol.info.decl(TermName("product"))
    val size                  = bagSymbol.info.decl(TermName("size"))
    val count                 = bagSymbol.info.decl(TermName("count"))
    val exists                = bagSymbol.info.decl(TermName("exists"))
    val forall                = bagSymbol.info.decl(TermName("forall"))
    val find                  = bagSymbol.info.decl(TermName("find"))
    val bottom                = bagSymbol.info.decl(TermName("bottom"))
    val top                   = bagSymbol.info.decl(TermName("top"))
    val sample                = bagSymbol.info.decl(TermName("sample"))

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

    val ops                   =  for {
      m <- sourceOps ++ sinkOps ++ monadOps ++ nestOps ++ setOps ++ foldOps
    } yield m

    val implicitTypes         = Set(
      typeOf[org.emmalanguage.api.Meta[Nothing]].typeConstructor,
      typeOf[org.emmalanguage.io.csv.CSVConverter[Nothing]].typeConstructor,
      typeOf[org.emmalanguage.io.parquet.ParquetConverter[Nothing]].typeConstructor
    )

    // Type constructors
    val DataBag               = typeOf[org.emmalanguage.api.DataBag[Nothing]].typeConstructor
    val Group                 = typeOf[org.emmalanguage.api.Group[Nothing, Nothing]].typeConstructor

    // Backend-only operations
    val byFetch               = scalaSeqModuleSymbol.info.member(api.TermName("byFetch")).asMethod
    //@formatter:on
  }

  protected[emmalanguage] object ComprehensionSyntax {
    //@formatter:off
    val module                = rootMirror.staticModule(s"$rootPkg.compiler.ir.ComprehensionSyntax").asModule

    val flatten               = module.info.decl(TermName("flatten")).asMethod
    val generator             = module.info.decl(TermName("generator")).asMethod
    val comprehension         = module.info.decl(TermName("comprehension")).asMethod
    val guard                 = module.info.decl(TermName("guard")).asMethod
    val head                  = module.info.decl(TermName("head")).asMethod

    val ops                   = Set(flatten, generator, comprehension, guard, head)
    //@formatter:on
  }

  protected[emmalanguage] object ComprehensionCombinators {
    //@formatter:off
    val module                = rootMirror.staticModule(s"$rootPkg.compiler.ir.ComprehensionCombinators").asModule

    val cross                 = module.info.decl(TermName("cross")).asMethod
    val equiJoin              = module.info.decl(TermName("equiJoin")).asMethod

    val ops                   = Set(cross, equiJoin)
    //@formatter:on
  }

  protected[emmalanguage] object GraphRepresentation {
    //@formatter:off
    val module  = rootMirror.staticModule(s"$rootPkg.compiler.ir.GraphRepresentation").asModule

    val phi     = module.info.member(api.TermName("phi")).asMethod

    val methods = Set(phi)
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

    def validateAs(expected: Validator, tree: Tree, violation: => String = "Unexpected tree"): Verdict = {

      expected.applyOrElse(tree, (unexpected: Tree) => {
        Bad(One(Error(unexpected, violation)))
      })
    }

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
