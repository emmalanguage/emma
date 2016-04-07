package eu.stratosphere.emma.compiler.ir

import eu.stratosphere.emma.compiler.ReflectUtil

/** Common IR tools. */
trait CommonIR extends ReflectUtil {

  import universe._

  // --------------------------------------------------------------------------
  // Emma API
  // --------------------------------------------------------------------------

  /** A set of API method symbols to be comprehended. */
  protected object API {
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

  protected object IR {
    val moduleSymbol /*    */ = rootMirror.staticModule("eu.stratosphere.emma.compiler.ir.package")

    val flatten /*         */ = moduleSymbol.info.decl(TermName("flatten"))
    val generator /*       */ = moduleSymbol.info.decl(TermName("generator"))
    val comprehension /*   */ = moduleSymbol.info.decl(TermName("comprehension"))
    val guard /*           */ = moduleSymbol.info.decl(TermName("guard"))
    val head /*            */ = moduleSymbol.info.decl(TermName("head"))

    val comprehensionOps /**/ = Set(flatten, generator, comprehension, guard, head)
  }

  // ---------------------------------------------------------------------------
  // Helper Patterns
  // ---------------------------------------------------------------------------

  object NewIOFormat {
    def unapply(root: Tree): Option[(Tree, Tree)] = root match {
      case Apply(apply@Apply(Select(New(tpt), name), args), List(implicitArgs)) if isSubclass(tpt) =>
        Some(apply, implicitArgs)
      case _ =>
        Option.empty[(Tree, Tree)]
    }

    def isSubclass(tree: Tree): Boolean =
      tree.tpe.baseClasses.contains(API.inputFmtSymbol) || tree.tpe.baseClasses.contains(API.outputFmtSymbol)
  }

  object WhileLoop {
    def unapply(root: Tree): Option[(Tree, Tree)] = root match {
      case LabelDef(name, Nil, If(cond, Block(List(body), Apply(fun, Nil)), Literal(Constant(()))))
        if fun.symbol == root.symbol => Some(cond, body)
      case _ =>
        Option.empty[(Tree, Tree)]
    }
  }

  object DoWhileLoop {
    def unapply(root: Tree): Option[(Tree, Tree)] = root match {
      case LabelDef(name, Nil, Block(List(body), If(cond, Apply(fun, Nil), Literal(Constant(())))))
        if fun.symbol == root.symbol => Some(cond, body)
      case _ =>
        Option.empty[(Tree, Tree)]
    }
  }

}
