package eu.stratosphere.emma
package compiler

import ast.AST

import org.scalactic._
import org.scalactic.Accumulation._

/** Common IR tools. */
trait Common extends ReflectUtil with AST {

  import universe._

  // --------------------------------------------------------------------------
  // Emma API
  // --------------------------------------------------------------------------

  /** A set of API method symbols to be comprehended. */
  protected[emma] object API {
    val moduleSymbol /*    */ = rootMirror.staticModule("eu.stratosphere.emma.api.package")
    val bagSymbol /*       */ = rootMirror.staticClass("eu.stratosphere.emma.api.DataBag")
    val groupSymbol /*     */ = rootMirror.staticClass("eu.stratosphere.emma.api.Group")
    val statefulSymbol /*  */ = rootMirror.staticClass("eu.stratosphere.emma.api.Stateful.Bag")
    val inputFmtSymbol /*  */ = rootMirror.staticClass("eu.stratosphere.emma.api.InputFormat")
    val outputFmtSymbol /* */ = rootMirror.staticClass("eu.stratosphere.emma.api.OutputFormat")

    val apply /*           */ = bagSymbol.companion.info.decl(TermName("apply"))
    val read /*            */ = moduleSymbol.info.decl(TermName("read"))
    val write /*           */ = moduleSymbol.info.decl(TermName("write"))
    val stateful /*        */ = moduleSymbol.info.decl(TermName("stateful"))
    val fold /*            */ = bagSymbol.info.decl(TermName("fold"))
    val map /*             */ = bagSymbol.info.decl(TermName("map"))
    val flatMap /*         */ = bagSymbol.info.decl(TermName("flatMap"))
    val withFilter /*      */ = bagSymbol.info.decl(TermName("withFilter"))
    val groupBy /*         */ = bagSymbol.info.decl(TermName("groupBy"))
    val minus /*           */ = bagSymbol.info.decl(TermName("minus"))
    val plus /*            */ = bagSymbol.info.decl(TermName("plus"))
    val distinct /*        */ = bagSymbol.info.decl(TermName("distinct"))
    val fetchToStateless /**/ = statefulSymbol.info.decl(TermName("bag"))
    val updateWithZero /*  */ = statefulSymbol.info.decl(TermName("updateWithZero"))
    val updateWithOne /*   */ = statefulSymbol.info.decl(TermName("updateWithOne"))
    val updateWithMany /*  */ = statefulSymbol.info.decl(TermName("updateWithMany"))

    val methods = Set(
      read, write,
      stateful, fetchToStateless, updateWithZero, updateWithOne, updateWithMany,
      fold,
      map, flatMap, withFilter,
      groupBy,
      minus, plus, distinct
    ) ++ apply.alternatives

    val monadic = Set(map, flatMap, withFilter)
    val updateWith = Set(updateWithZero, updateWithOne, updateWithMany)

    // Type constructors
    val DATA_BAG /*        */ = typeOf[eu.stratosphere.emma.api.DataBag[Nothing]].typeConstructor
    val GROUP /*           */ = typeOf[eu.stratosphere.emma.api.Group[Nothing, Nothing]].typeConstructor
  }

  protected[emma] object IR {
    val module /*          */ = rootMirror.staticModule("eu.stratosphere.emma.compiler.ir.package").asModule

    val flatten /*         */ = module.info.decl(TermName("flatten")).asMethod
    val generator /*       */ = module.info.decl(TermName("generator")).asMethod
    val comprehension /*   */ = module.info.decl(TermName("comprehension")).asMethod
    val guard /*           */ = module.info.decl(TermName("guard")).asMethod
    val head /*            */ = module.info.decl(TermName("head")).asMethod

    val comprehensionOps /**/ = Set(flatten, generator, comprehension, guard, head)
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
      override def toString = s"$violation:\n${Tree show at}"
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
        withGood(verdict, other) { case _ => ok }
    }

  }

  // --------------------------------------------------------------------------
  // Universe API that needs to be importet
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
  protected[emma] object UniverseImplicits {

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
