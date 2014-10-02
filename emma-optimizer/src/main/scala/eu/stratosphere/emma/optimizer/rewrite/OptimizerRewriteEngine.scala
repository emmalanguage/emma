package eu.stratosphere.emma.optimizer.rewrite

import _root_.eu.stratosphere.emma.ir._
import _root_.eu.stratosphere.emma.rewrite.RewriteEngine
import eu.stratosphere.emma.ir.monad.Monad

import scala.collection.mutable

object OptimizerRewriteEngine {

  def apply() = new OptimizerRewriteEngine()
}

class OptimizerRewriteEngine extends RewriteEngine {

  import scala.reflect.runtime.universe._

  override type ExpressionRoot = eu.stratosphere.emma.ir.ExpressionRoot

  override val rules: List[Rule] = List(MatchFilter, MatchJoin, MatchCross, MatchMap)

  /**
   * [ e | qs, x ← xs, qs1, p x, qs2 ] →
   * [ e | qs, x ← σ p xs, qs1, qs2 ]
   */
  object MatchFilter extends Rule {

    case class RuleMatch(parent: Comprehension[Any], generator: Generator, filter: Filter)

    override protected def bind(r: ExpressionRoot) = new Traversable[RuleMatch] {
      override def foreach[U](f: (RuleMatch) => U) = {
        for (x <- globalSeq(r.expr)) x match {
          case parent@Comprehension(_, _) =>
            for (q <- parent.qualifiers) q match {
              case filter@Filter(expr) =>
                for (q <- parent.qualifiers.span(_ != filter)._1) q match {
                  case generator: Generator =>
                    f(RuleMatch(parent, generator, filter))
                  case _ =>
                    Unit
                }

              case _ =>
                Unit
            }
          case _ =>
            Unit
        }
      }
    }

    /*
     * Checks:
     * - Filter has only 1 free var
     * - Generator binds the free var of the filter
     */
    override protected def guard(r: ExpressionRoot, m: RuleMatch) = {
      m.filter.expr match {
        case ScalaExpr(env, _) if env.size == 1 => env.head._1 == m.generator.lhs
//        case quantifier@Comprehension(h, qs) if getType(t) =:= getType(monad.All) || getType(t) =:= getType(monad.Exists) =>
//          // get free vars
//          val fv = getFreeVars(quantifier).keySet
//          fv.size == 1 && fv.head == m.generator.lhs
        case _ => false
      }
    }

    override protected def fire(r: ExpressionRoot, m: RuleMatch) = {
      val (prefix, suffix) = m.parent.qualifiers.span(_ != m.generator)
      m.parent.qualifiers = prefix ++ List(FilterCombinator(m.filter, m.generator)) ++ suffix.tail.filter(_ != m.filter)
    }
  }

  //----------------------------------------------------------------------------
  // JOINS
  //----------------------------------------------------------------------------

  /**
   * [ e | qs, x ← xs, y ← ys, qs1, p x y, qs2 ] →
   * [ e[v.x/x][v.y/y] | qs, v ← xs ⋈ ys, qs1[v.x/x][v.y/y], qs2[v.x/x][v.y/y] ]
   */
  object MatchJoin extends Rule {

    case class RuleMatch(parent: Comprehension[Any], g1: Generator, g2: Generator, filter: Filter)

    override protected def bind(r: ExpressionRoot) = new Traversable[RuleMatch] {
      override def foreach[U](f: (RuleMatch) => U) = {
        for (x <- globalSeq(r.expr)) x match {
          case parent@Comprehension(_, _) =>
            for (q <- parent.qualifiers) q match {
              case filter@Filter(expr) =>
                for (List(q1, q2) <- parent.qualifiers.span(_ != filter)._1.sliding(2)) (q1, q2) match {
                  case (a: Generator, b: Generator) =>
                    f(RuleMatch(parent, a, b, filter))
                  case _ =>
                    Unit
                }

              case _ =>
                Unit
            }
          case _ =>
            Unit
        }
      }
    }

    override protected def guard(r: ExpressionRoot, m: RuleMatch) = {
      m.filter.expr match {
        case se@ScalaExpr(env, _) if env.size == 2 && isEquiJoin(se) => (m.g1.lhs == env.head && m.g2.lhs == env.tail.head) || (m.g1.lhs == env.tail.head && m.g2.lhs == env.head)
        case _ => false
      }
    }

    def isEquiJoin(expr: ScalaExpr[Any]): Boolean = {
      class RelationTraverser() extends Traverser {

        import scala.collection._

        var localVars = mutable.Map[String, Tree]()

        var equiJoin: Boolean = false

        override def traverse(tree: Tree): Unit = tree match {
          case Apply(Select(_, name), _) =>
            if (name.toString == "$eq$eq") equiJoin = true
          // else theta join
          //        case Block(stats, Apply(Select(_, name), _)) if (name.toString == "$eq$eq") =>
          //          // build val defs
          //          for (s <- stats) traverse(s)
          //        case Block(stats, Ident(name)) =>
          //          for (s <- stats) traverse(s)
          //
          //        case ValDef(_, name, _, rhs) => localVars += (name, rhs)
          case _ => // TODO: should not be false, but no join at all
        }
      }

      val res = new RelationTraverser()
      res.traverse(expr.expr.tree)
      res.equiJoin
    }

    override protected def fire(r: ExpressionRoot, m: RuleMatch) = {
      val freeVars = m.filter.expr match {
        case ScalaExpr(env, _) => env
        case _ => Map() // this can not happen as we only allow scala expr in the guard
      }

      val (prefix, suffix) = m.parent.qualifiers.span(_ != m.g1)
      val join = EquiJoinCombinator(freeVars.head._1, m.filter, m.g1, m.g2)
      m.parent.qualifiers = prefix ++ List(join) ++ suffix.tail.tail.filter(_ != m.filter)

      val toSubstitute = globalSeq(m.parent).span(_ != join)._2.tail.span(_ != m.parent)._1

      // replace var
      substituteVars(freeVars.keySet.toList, toSubstitute)
    }
  }

  //  /**
  //   * [ e | qs, x ← xs, qs1, [ p x y | y ← ys](exists), qs2 ] →
  //   * [ e | qs, x ← xs ⋉ ys, qs1, qs2 ]
  //   */
  //  object MatchSemiJoin extends Rule {}
  //
  //  /**
  //   * [ e | qs, x ← xs, qs1, [ ¬ p x y | y ← ys](all), qs2 ] →
  //   * [ e | qs, x ← xs ⋉ ys, qs1, qs2 ]
  //   */
  //  object MatchAntiJoin extends Rule {}

  //----------------------------------------------------------------------------
  // CROSS
  //----------------------------------------------------------------------------

  /**
   * [ e | qs, x ← xs, y ← ys, qs1 ] →
   * [ e[v.x/x][v.y/y] | qs, v ← xs ⨯ ys, qs1[v.x/x][v.y/y] ]
   *
   * for (x <- X; y <- x) yield y
   */
  object MatchCross extends Rule {

    case class RuleMatch(parent: Comprehension[Any], generators: List[Generator])

    override protected def bind(r: ExpressionRoot) = new Traversable[RuleMatch] {
      override def foreach[U](f: (RuleMatch) => U) = {
        for (x <- globalSeq(r.expr)) x match {
          case parent@Comprehension(_, qs) =>
            val builder = mutable.LinkedHashSet[Generator]()
            for (List(q1, q2) <- qs.sliding(2)) (q1, q2) match {
              case (a: Generator, b: Generator) =>
                builder += a

                val contained = builder.foldLeft(false)((contained, q) => containsFreeVars(q.lhs, b) || contained)
                if (contained) {
                  if (builder.size >= 2) f(RuleMatch(parent, builder.toList))
                  builder.clear()
                }
                builder += b
              case (_, b: Generator) =>
                builder += b
              case (a: Generator, _) =>
                // as we have been in the fist case if we have a cross we do not have to check or add here
                if (builder.size >= 2) f(RuleMatch(parent, builder.toList))
                builder.clear()
              case _ =>
                // should never happen
                throw new IllegalStateException()
            }
            if (builder.size >= 2) f(RuleMatch(parent, builder.toList))
          case _ =>
            Unit
        }
      }
    }

    // prevent matching of source and sink
    override protected def guard(r: ExpressionRoot, m: RuleMatch) = true

    def fuseTuple(ss: List[String]) = ???

    override protected def fire(r: ExpressionRoot, m: RuleMatch) = {
      val identifier = m.generators.head.lhs
      val cross = CrossCombinator(identifier, m.generators)
      val heads: Map[String, Type] = {
        val builder = Map.newBuilder[String, Type]
        for (g <- m.generators) {
          val tpe: Type = g match {
            case c@ComprehensionGenerator(l, Comprehension(_, _)) => c.tag.tpe
            case ScalaExprGenerator(l, ScalaExpr(f, e)) => e.staticType
            case _ => throw new IllegalStateException()
          }
          builder += Tuple2(g.lhs, tpe)
        }
        builder.result()
      }

      val (prefix, suffix) = m.parent.qualifiers.span(_ != m.generators.head)
      m.parent.qualifiers = prefix ++ List(cross) ++ suffix.drop(m.generators.size)

      val toSubstitute = globalSeq(m.parent).span(_ != cross)._2.tail.span(_ != m.parent)._1

      // replace var
      substituteVars(heads.keySet.toList, toSubstitute)
    }
  }

  /**
   * [ f e | qs ] → map f [ e | qs ]
   *
   * We expect that aggregations are matched beforehand, as we do not check here!
   * Currently only maps with one input are supported!
   */
  object MatchMap extends Rule {

    //TODO: at this point no single filter should be left, so list generator should be better? do this?
    case class RuleMatch(parent: Option[ComprehensionGenerator[Any]], body: ScalaExpr[Any], generators: List[Qualifier], flatmap: Boolean = false)

    override protected def bind(r: ExpressionRoot) = new Traversable[RuleMatch] {
      override def foreach[U](f: (RuleMatch) => U) = {
        for (x <- globalSeq(r.expr)) x match {
          case parent@ComprehensionGenerator(_, c@Comprehension(e: ScalaExpr[Any], q)) =>
            f(RuleMatch(Some(parent), e, q))
          case Comprehension(e: ScalaExpr[Any], q) =>
            f(RuleMatch(None, e, q))
          case parent@ComprehensionGenerator(_, MonadJoin(Comprehension(e: ScalaExpr[Any], q))) =>
            f(RuleMatch(Some(parent), e, q, flatmap = true))
          case MonadJoin(Comprehension(e: ScalaExpr[Any], q)) =>
            f(RuleMatch(None, e, q, flatmap = true))
          case _ =>
            Unit
        }
      }
    }

    // prevent matching of source and sink
    override protected def guard(r: ExpressionRoot, m: RuleMatch) = true // m.generators.size == 1

    override protected def fire(r: ExpressionRoot, m: RuleMatch) = {
      val operator =
        if (m.flatmap)
          FlatMapCombinator(m.body, m.generators.head)
        else
          MapCombinator(m.body, m.generators.head)

      m.parent match {
        case Some(g) => g.rhs = operator
        case None => r.expr = operator
      }
    }
  }

}
