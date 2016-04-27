package eu.stratosphere.emma.compiler

/** Common IR tools. */
trait Common extends ReflectUtil {

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

    val flatten /*         */ = module.info.decl(TermName("flatten")).asTerm
    val generator /*       */ = module.info.decl(TermName("generator")).asTerm
    val comprehension /*   */ = module.info.decl(TermName("comprehension")).asTerm
    val guard /*           */ = module.info.decl(TermName("guard")).asTerm
    val head /*            */ = module.info.decl(TermName("head")).asTerm

    val comprehensionOps /**/ = Set(flatten, generator, comprehension, guard, head)
  }
}
