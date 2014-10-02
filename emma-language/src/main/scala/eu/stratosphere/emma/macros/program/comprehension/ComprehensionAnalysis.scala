package eu.stratosphere.emma.macros.program.comprehension

import eu.stratosphere.emma.macros.program.comprehension.rewrite.ComprehensionNormalization
import eu.stratosphere.emma.macros.program.util.ProgramUtils

import scala.collection.mutable
import eu.stratosphere.emma.macros.program.ContextHolder
import eu.stratosphere.emma.macros.program.controlflow.ControlFlowModel

import scala.reflect.macros._

private[emma] trait ComprehensionAnalysis[C <: blackbox.Context]
  extends ContextHolder[C]
  with ControlFlowModel[C]
  with ComprehensionModel[C]
  with ComprehensionNormalization[C]
  with ProgramUtils[C] {

  import c.universe._

  /**
   * A set of API method symbols to be comprehended.
   */
  private object api {
    val moduleSymbol = rootMirror.staticModule("eu.stratosphere.emma.api.package")
    val bagSymbol = rootMirror.staticClass("eu.stratosphere.emma.api.DataBag")

    val read = moduleSymbol.info.decl(TermName("read"))
    val write = moduleSymbol.info.decl(TermName("write"))
    val stateful = moduleSymbol.info.decl(TermName("stateful"))
    val fold = bagSymbol.info.decl(TermName("fold"))
    val map = bagSymbol.info.decl(TermName("map"))
    val flatMap = bagSymbol.info.decl(TermName("flatMap"))
    val withFilter = bagSymbol.info.decl(TermName("withFilter"))
    val groupBy = bagSymbol.info.decl(TermName("groupBy"))
    val minus = bagSymbol.info.decl(TermName("minus"))
    val plus = bagSymbol.info.decl(TermName("plus"))
    val distinct = bagSymbol.info.decl(TermName("distinct"))
    val minBy = bagSymbol.info.decl(TermName("minBy"))
    val maxBy = bagSymbol.info.decl(TermName("maxBy"))
    val min = bagSymbol.info.decl(TermName("min"))
    val max = bagSymbol.info.decl(TermName("max"))
    val sum = bagSymbol.info.decl(TermName("sum"))
    val product = bagSymbol.info.decl(TermName("product"))
    val count = bagSymbol.info.decl(TermName("count"))
    val exists = bagSymbol.info.decl(TermName("exists"))
    val forall = bagSymbol.info.decl(TermName("forall"))
    val empty = bagSymbol.info.decl(TermName("empty"))

    val methods = Set(
      read, write,
      stateful,
      fold,
      map, flatMap, withFilter,
      groupBy,
      minus, plus, distinct,
      minBy, maxBy, min, max, sum, product, count, exists, forall, empty
    )

    val monadic = Set(map, flatMap, withFilter)
  }

  // --------------------------------------------------------------------------
  // Comprehension Store Constructor
  // --------------------------------------------------------------------------

  /**
   * Comprehension store constructor.
   *
   * @param cfGraph The control flow graph for the comprehended algorithm.
   */
  def createComprehensionStore(cfGraph: CFGraph) = {

    // step #1: compute the set of maximal terms that can be translated to comprehension syntax
    val root = cfGraph.nodes.find(_.inDegree == 0).get
    // implicit helpers
    implicit def cfBlockTraverser = root.outerNodeTraverser()

    // step #1: compute the set of maximal terms that can be translated to comprehension syntax
    val terms = (for (block <- cfBlockTraverser; stmt <- block.stats) yield {
      // find all value applications on methods from the comprehended API for this statement
      implicit var comprehendedTerms = mutable.Set(stmt.filter({
        case Apply(fun, _) if api.methods.contains(fun.symbol) => true
        case _ => false
      }): _*)

      // reduce by removing obsolete terms
      var obsolete = mutable.Set.empty[Tree]
      // a) remove applies that will be comprehended with their parent selector
      do {
        comprehendedTerms = comprehendedTerms diff obsolete
        obsolete = for (t <- comprehendedTerms; p <- comprehendedSelectParent(t)) yield p
      } while (obsolete.nonEmpty)
      // b) remove applies that will be comprehended with their enclosing flatMap
      do {
        comprehendedTerms = comprehendedTerms diff obsolete
        obsolete = for (t <- comprehendedTerms; c <- comprehendedFlatMapBody(t)) yield c
      } while (obsolete.nonEmpty)

      // return the reduced set of applies
      comprehendedTerms
    }).flatten.toSet

    // step #2: create ComprehendedTerm entries for the identified terms
    val comprehendedTerms = mutable.Seq((for (t <- terms) yield {
      val id = TermName(c.freshName("comprehension"))
      val definition = comprehendedTermDefinition(t)
      val comprehension = normalize(ExpressionRoot(comprehend(Nil)(t)))

      ComprehendedTerm(id, t, comprehension, definition)
    }).toSeq: _*)

    // step #3: build the comprehension store
    new ComprehensionStore(comprehendedTerms)
  }

  /**
   * Checks whether the parent expression in a selector chain is also comprehended.
   *
   * @param t the expression to be checked
   * @param comprehendedTerms the set of comprehended terms
   * @return An option holding the comprehended parent term (if such exists).
   */
  private def comprehendedSelectParent(t: Tree)(implicit comprehendedTerms: mutable.Set[Tree]) = t match {
    case Apply(TypeApply(Select(parent, _), _), _) if comprehendedTerms.contains(parent) =>
      Some(parent)
    case Apply(Select(parent, _), _) if comprehendedTerms.contains(parent) =>
      Some(parent)
    case Apply(parent@Apply(_, _), _) if comprehendedTerms.contains(parent) =>
      Some(parent)
    case _ =>
      Option.empty[Tree]
  }

  /**
   * Checks whether the body of the function passed to a flatMap is a comprehension term.
   *
   * @param t the expression to be checked
   * @param comprehendedTerms the set of comprehended terms
   * @return An option holding the comprehended function body term (if such exists).
   */
  private def comprehendedFlatMapBody(t: Tree)(implicit comprehendedTerms: mutable.Set[Tree]) = t match {
    case Apply(fun, (f: Function) :: Nil) if fun.symbol == api.flatMap && api.monadic.contains(f.body.symbol) && comprehendedTerms.contains(f.body) =>
      Some(f.body)
    case _ =>
      Option.empty[Tree]
  }

  /**
   * Looks up a definition term (ValDef or Assign) for a comprehended term.
   *
   * @param t The term to lookup.
   * @return The (optional) definition for the term.
   */
  private def comprehendedTermDefinition(t: Tree)(implicit cfBlockTraverser: TraversableOnce[CFBlock]) = {
    var optSymbol = Option.empty[Tree]
    for (block <- cfBlockTraverser; s <- block.stats) s.foreach({
      case ValDef(_, name, _, rhs) if t == rhs =>
        optSymbol = Some(s)
      case Assign(lhs, rhs) if t == rhs =>
        optSymbol = Some(s)
      case _ =>
    })
    optSymbol
  }

  // --------------------------------------------------------------------------
  // Comprehension Constructor
  // --------------------------------------------------------------------------

  /**
   * Recursive comprehend method.
   *
   * @param env The ValDef environment for the currently lifted tree
   * @param tree The tree to be lifted
   * @return A lifted, MC syntax version of the given tree
   */
  private def comprehend(env: List[ValDef])(tree: Tree): Expression = {

    // ignore a top-level Typed node (side-effect of the Algebra inlining macros)
    val t = tree match {
      case Typed(inner, _) => inner
      case _ => tree
    }

    // translate based on matched expression type
    t match {

      // -----------------------------------------------------
      // Monad Ops
      // -----------------------------------------------------

      // in.map(fn)
      case Apply(TypeApply(select@Select(in, _), List(tpt)), List(fn@Function(List(arg), body))) if select.symbol == api.map =>
        val vd_arg = q"val ${arg.name} = null.asInstanceOf[${arg.tpt}]".asInstanceOf[ValDef]

        val bind = Generator(arg.name, comprehend(env)(in))
        val head = comprehend(vd_arg :: env)(body)

        Comprehension(tpt.tpe, head, bind :: Nil)

      // in.flatMap(fn)
      case Apply(TypeApply(select@Select(in, _), List(tpt)), List(fn@Function(List(arg), body))) if select.symbol == api.flatMap =>
        val vd_arg = q"val ${arg.name} = null.asInstanceOf[${arg.tpt}]".asInstanceOf[ValDef]

        val bind = Generator(arg.name, comprehend(env)(in))
        val head = comprehend(vd_arg :: env)(body)

        MonadJoin(Comprehension(tpt.tpe, head, bind :: Nil))

      // in.withFilter(fn)
      case Apply(select@Select(in, _), List(fn@Function(List(arg), body))) if select.symbol == api.withFilter =>
        val vd_arg = q"val ${arg.name}: ${arg.tpt} = null.asInstanceOf[${arg.tpt}]".asInstanceOf[ValDef]

        val bind = Generator(arg.name, comprehend(env)(in))
        val filter = Filter(comprehend(vd_arg :: env)(body))
        val head = comprehend(vd_arg :: env)(q"${arg.name}")

        Comprehension(arg.tpt.tpe, head, bind :: filter :: Nil)

      // -----------------------------------------------------
      // Grouping and Set operations
      // -----------------------------------------------------

      // in.groupBy(k)
      case Apply(TypeApply(select@Select(in, _), List(tpt)), List(k@Function(List(arg), body))) if select.symbol == api.groupBy =>
        Group(comprehend(Nil)(k).asInstanceOf[ScalaExpr], comprehend(Nil)(in))

      // in.minus(subtrahend)
      case Apply(TypeApply(select@Select(in, _), List(_)), List(subtrahend)) if select.symbol == api.minus =>
        Diff(comprehend(Nil)(in), comprehend(Nil)(subtrahend))

      // in.plus(addend)
      case Apply(TypeApply(select@Select(in, _), List(tpt)), List(addend)) if select.symbol == api.minus =>
        Union(comprehend(Nil)(in), comprehend(Nil)(addend))

      // in.distinct()
      case Apply(select@Select(in, _), Nil) if select.symbol == api.distinct =>
        Distinct(comprehend(Nil)(in))

      // -----------------------------------------------------
      // Aggregates
      // -----------------------------------------------------

      // in.minBy()(n)
      case Apply(TypeApply(select@Select(in, _), List(tpt)), List(Function(List(x, y), body))) if select.symbol == api.minBy =>
        val vd_x = q"val x: $tpt = null.asInstanceOf[$tpt]".asInstanceOf[ValDef]
        val vd_y = q"val y: $tpt = null.asInstanceOf[$tpt]".asInstanceOf[ValDef]

        // replace the body of the fn to use 'u', 'v' parameters instead of the given arguments
        // FIXME: changes semantics if 'u' and 'v' are definen in the body
        val bodyNew = substitute(body, Map(x.name.toString -> Ident(TermName("u")), y.name.toString -> Ident(TermName("v"))))

        // quasiquote fold operators using the minBy parameter function
        val empty = q"Option.empty[$tpt]"
        val sng = q"Some[$tpt](x)"
        val union = q"""{
          if (x.isEmpty && y.isDefined) y
          else if (x.isDefined && y.isEmpty) x
          else for (u <- x; v <- y) yield if ($bodyNew) u else v
        }"""

        Fold(tpt.tpe, ScalaExpr(Nil, empty), ScalaExpr(vd_x :: Nil, sng), ScalaExpr(vd_x :: vd_y :: Nil, union), comprehend(Nil)(in))

      // in.maxBy()(n)
      case Apply(TypeApply(select@Select(in, _), List(tpt)), List(Function(List(x, y), body))) if select.symbol == api.maxBy =>
        val vd_x = q"val x: $tpt = null.asInstanceOf[$tpt]".asInstanceOf[ValDef]
        val vd_y = q"val y: $tpt = null.asInstanceOf[$tpt]".asInstanceOf[ValDef]

        // replace the body of the fn to use 'u', 'v' parameters instead of the given arguments
        // FIXME: changes semantics if 'u' and 'v' are definen in the body
        val bodyNew = substitute(body, Map(x.name.toString -> Ident(TermName("u")), y.name.toString -> Ident(TermName("v"))))

        // quasiquote fold operators using the minBy parameter function
        val empty = q"Option.empty[$tpt]"
        val sng = q"Some[$tpt](x)"
        val union = q"""{
          if (x.isEmpty && y.isDefined) y
          else if (x.isDefined && y.isEmpty) x
          else for (u <- x; v <- y) yield if ($bodyNew) v else u
        }"""

        Fold(tpt.tpe, ScalaExpr(Nil, empty), ScalaExpr(vd_x :: Nil, sng), ScalaExpr(vd_x :: vd_y :: Nil, union), comprehend(Nil)(in))

      // in.min()(n)
      case Apply(Apply(TypeApply(select@Select(in, _), List(tpt)), Nil), n :: l :: Nil) if select.symbol == api.min =>
        val vd_x = q"val x: $tpt = null.asInstanceOf[$tpt]".asInstanceOf[ValDef]
        val vd_y = q"val y: $tpt = null.asInstanceOf[$tpt]".asInstanceOf[ValDef]

        Fold(tpt.tpe, ScalaExpr(Nil, q"$l.max"), ScalaExpr(vd_x :: Nil, q"x"), ScalaExpr(vd_x :: vd_y :: Nil, q"$n.min(x, y)"), comprehend(Nil)(in))

      // in.max()(n)
      case Apply(Apply(TypeApply(select@Select(in, _), List(tpt)), Nil), n :: l :: Nil) if select.symbol == api.max =>
        val vd_x = q"val x: $tpt = null.asInstanceOf[$tpt]".asInstanceOf[ValDef]
        val vd_y = q"val y: $tpt = null.asInstanceOf[$tpt]".asInstanceOf[ValDef]

        Fold(tpt.tpe, ScalaExpr(Nil, q"$l.min"), ScalaExpr(vd_x :: Nil, q"x"), ScalaExpr(vd_x :: vd_y :: Nil, q"$n.max(x, y)"), comprehend(Nil)(in))

      // in.sum()(n)
      case Apply(Apply(TypeApply(select@Select(in, _), List(tpt)), Nil), n :: Nil) if select.symbol == api.sum =>
        val vd_x = q"val x: $tpt = null.asInstanceOf[$tpt]".asInstanceOf[ValDef]
        val vd_y = q"val y: $tpt = null.asInstanceOf[$tpt]".asInstanceOf[ValDef]

        Fold(tpt.tpe, ScalaExpr(Nil, q"$n.zero"), ScalaExpr(vd_x :: Nil, q"x"), ScalaExpr(vd_x :: vd_y :: Nil, q"$n.plus(x, y)"), comprehend(Nil)(in))

      // in.product()(n)
      case Apply(Apply(TypeApply(select@Select(in, _), List(tpt)), Nil), n :: Nil) if select.symbol == api.product =>
        val vd_x = q"val x: $tpt = null.asInstanceOf[$tpt]".asInstanceOf[ValDef]
        val vd_y = q"val y: $tpt = null.asInstanceOf[$tpt]".asInstanceOf[ValDef]

        Fold(tpt.tpe, ScalaExpr(Nil, q"$n.one"), ScalaExpr(vd_x :: Nil, q"x"), ScalaExpr(vd_x :: vd_y :: Nil, q"$n.times(x, y)"), comprehend(Nil)(in))

      // in.count()(n)
      case Apply(select@Select(in, _), Nil) if select.symbol == api.count =>
        val vd_x = q"val x: Long = null.asInstanceOf[Long]".asInstanceOf[ValDef]
        val vd_y = q"val y: Long = null.asInstanceOf[Long]".asInstanceOf[ValDef]

        Fold(c.typeOf[Long], ScalaExpr(Nil, q"0L"), ScalaExpr(vd_x :: Nil, q"1L"), ScalaExpr(vd_x :: vd_y :: Nil, q"x + y"), comprehend(Nil)(in))

      // in.exists()(n)
      case Apply(select@Select(in, _), List(fn@Function(List(arg), body))) if select.symbol == api.exists =>
        val vd_x = q"val x: Boolean = null.asInstanceOf[Long]".asInstanceOf[ValDef]
        val vd_y = q"val y: Boolean = null.asInstanceOf[Long]".asInstanceOf[ValDef]

        Fold(c.typeOf[Boolean], ScalaExpr(Nil, q"false"), ScalaExpr(vd_x :: Nil, substitute(body, arg.toString(), q"x")), ScalaExpr(vd_x :: vd_y :: Nil, q"x || y"), comprehend(Nil)(in))

      // in.forall()(n)
      case Apply(select@Select(in, _), List(fn@Function(List(arg), body))) if select.symbol == api.forall =>
        val vd_x = q"val x: Boolean = null.asInstanceOf[Long]".asInstanceOf[ValDef]
        val vd_y = q"val y: Boolean = null.asInstanceOf[Long]".asInstanceOf[ValDef]

        Fold(c.typeOf[Boolean], ScalaExpr(Nil, q"true"), ScalaExpr(vd_x :: Nil, substitute(body, arg.toString(), q"x")), ScalaExpr(vd_x :: vd_y :: Nil, q"x && y"), comprehend(Nil)(in))

      // in.empty()(n)
      case Apply(select@Select(in, _), Nil) if select.symbol == api.empty =>
        val vd_x = q"val x: Boolean = null.asInstanceOf[Long]".asInstanceOf[ValDef]
        val vd_y = q"val y: Boolean = null.asInstanceOf[Long]".asInstanceOf[ValDef]

        Fold(c.typeOf[Boolean], ScalaExpr(Nil, q"false"), ScalaExpr(vd_x :: Nil, q"true"), ScalaExpr(vd_x :: vd_y :: Nil, q"x || y"), comprehend(Nil)(in))

      // ----------------------------------------------------------------------
      // Environment & Host Language Connectors
      // ----------------------------------------------------------------------

      // write[T](location, ofmt)(in)
      case Apply(Apply(TypeApply(method, List(_)), location :: ofmt :: Nil), List(in: Tree)) if method.symbol == api.write =>
        Write(location, ofmt, comprehend(env)(in))

      // read[T](location, ifmt)
      case Apply(TypeApply(method, List(tpt)), location :: ifmt :: Nil) if method.symbol == api.read =>
        Read(tpt.tpe, location, ifmt)

      // interpret as black box Scala expression (default)
      case _ =>
        ScalaExpr(env, t)
    }
  }

}
