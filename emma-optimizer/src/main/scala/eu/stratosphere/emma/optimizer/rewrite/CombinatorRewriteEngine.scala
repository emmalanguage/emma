package eu.stratosphere.emma.optimizer.rewrite

import eu.stratosphere.emma.ir._
import eu.stratosphere.emma.rewrite.RewriteEngine

import scala.collection.mutable

//class CombinatorRewriteEngine extends RewriteEngine {
//
//  type Expression = eu.stratosphere.emma.ir.Expression
//
//  import scala.reflect.runtime.universe._
//
//  def rewrite(root: ExpressionRoot) = {
//    /*MatchFilter, MatchMap, MatchJoin, MatchCross,*/
//    exhaust(MatchFilter, MatchMap)(root)
//    root
//  }
//
//  /**
//   * Exhaustively applies a set to a tree and all its subtrees.
//   *
//   * @param rules A set of rules to be applied.
//   * @param root The root of the expression tree to be transformed.
//   * @return The root of the transformed expression.
//   */
//  private def exhaust(rules: Rule*)(root: ExpressionRoot) = {
//    object apply extends (ExpressionRoot => Boolean) with ExpressionTransformer {
//      var changed = false
//
//      override def transform(e: Expression): Expression = apply(rules, e) match {
//        case Some(x) => changed = true; x
//        case _ => super.transform(e)
//      }
//
//      private def apply(rules: Seq[Rule], e: Expression) = rules.foldLeft(Option.empty[Expression])((x: Option[Expression], r: Rule) => x match {
//        case Some(_) => x
//        case _ => r.apply(e)
//      })
//
//      override def apply(root: ExpressionRoot) = {
//        changed = false
//        root.expr = transform(root.expr)
//        changed
//      }
//    }
//
//    var changed = false
//    do {
//      changed = apply(root)
//    } while (changed)
//  }
//
//  /**
//   * Creates a filter combinator.
//   *
//   * ==Rule Description==
//   *
//   * '''Matching Pattern''':
//   * {{{ [[ e | qs, x ← xs, qs1, p x, qs2 ]] }}}
//   *
//   * '''Rewrite''':
//   * {{{ [[ e | qs, x ← filter p xs, qs1, qs2 ]] }}}
//   */
//  object MatchFilter extends Rule {
//
//    case class RuleMatch(root: Comprehension[Any], generator: Generator[Any], filter: Filter)
//
//    override protected def bind(r: Expression) = {
//      var m = Option.empty[RuleMatch]
//
//      r match {
//        case parent@Comprehension(_, _) =>
//          for (q <- parent.qualifiers) q match {
//            case filter@Filter(expr) =>
//              for (q <- parent.qualifiers.span(_ != filter)._1) q match {
//                case generator: Generator[Any] =>
//                  m = Some(RuleMatch(parent, generator, filter))
//                case _ =>
//              }
//            case _ =>
//          }
//        case _ =>
//      }
//
//      m
//    }
//
//    /**
//     * Checks:
//     *
//     * - Filter has only 1 free var
//     * - Generator binds the free var of the filter
//     */
//    override protected def guard(r: Expression, m: RuleMatch) = {
//      // TODO: add support for comprehension filter expressions
//      m.filter.expr match {
//        case ScalaExpr(env, _) if env.size == 1 => env.head._1 == m.generator.lhs
//        case _ => false
//      }
//    }
//
//    override protected def fire(r: Expression, m: RuleMatch) = {
//      // TODO: add support for comprehension filter expressions
//      m.filter.expr match {
//        case ScalaExpr(env, expr) =>
//          val p = q"(${TermName(env.head._1)}: ${env.head._2.actualType}) => ${expr.tree}"
//          m.generator.rhs = FilterCombinator(p, m.generator.rhs)
//          m.root.qualifiers = m.root.qualifiers.filter(_ != m.filter)
//          // return new parent
//          m.root
//        case _ =>
//          throw new RuntimeException("Unexpected filter expression type")
//      }
//    }
//  }
//
//  //  //----------------------------------------------------------------------------
//  //  // JOINS
//  //  //----------------------------------------------------------------------------
//  //
//  //  /**
//  //   * [ e | qs, x ← xs, y ← ys, qs1, p x y, qs2 ] →
//  //   * [ e[v.x/x][v.y/y] | qs, v ← xs ⋈ ys, qs1[v.x/x][v.y/y], qs2[v.x/x][v.y/y] ]
//  //   */
//  //  object MatchJoin extends Rule {
//  //
//  //    case class RuleMatch(parent: Comprehension[Any], g1: Generator[Any], g2: Generator[Any], filter: Filter)
//  //
//  //    override protected def bind(r: ExpressionRoot) = new Traversable[RuleMatch] {
//  //      override def foreach[U](f: (RuleMatch) => U) = {
//  //        for (x <- globalSeq(r.expr)) x match {
//  //          case parent@Comprehension(_, _) =>
//  //            for (q <- parent.qualifiers) q match {
//  //              case filter@Filter(expr) =>
//  //                for (List(q1, q2) <- parent.qualifiers.span(_ != filter)._1.sliding(2)) (q1, q2) match {
//  //                  case (a: Generator[Any], b: Generator[Any]) =>
//  //                    f(RuleMatch(parent, a, b, filter))
//  //                  case _ =>
//  //                    Unit
//  //                }
//  //
//  //              case _ =>
//  //                Unit
//  //            }
//  //          case _ =>
//  //            Unit
//  //        }
//  //      }
//  //    }
//  //
//  //    override protected def guard(r: ExpressionRoot, m: RuleMatch) = {
//  //      m.filter.expr match {
//  //        case se@ScalaExpr(env, _) if env.size == 2 && isEquiJoin(se) => (m.g1.lhs == env.head && m.g2.lhs == env.tail.head) || (m.g1.lhs == env.tail.head && m.g2.lhs == env.head)
//  //        case _ => false
//  //      }
//  //    }
//  //
//  //    def isEquiJoin(expr: ScalaExpr[Any]): Boolean = {
//  //      class RelationTraverser() extends Traverser {
//  //
//  //        import scala.collection._
//  //
//  //        var localVars = mutable.Map[String, Tree]()
//  //
//  //        var equiJoin: Boolean = false
//  //
//  //        override def traverse(tree: Tree): Unit = tree match {
//  //          case Apply(Select(_, name), _) =>
//  //            if (name.toString == "$eq$eq") equiJoin = true
//  //          // else theta join
//  //          //        case Block(stats, Apply(Select(_, name), _)) if (name.toString == "$eq$eq") =>
//  //          //          // build val defs
//  //          //          for (s <- stats) traverse(s)
//  //          //        case Block(stats, Ident(name)) =>
//  //          //          for (s <- stats) traverse(s)
//  //          //
//  //          //        case ValDef(_, name, _, rhs) => localVars += (name, rhs)
//  //          case _ => // TODO: should not be false, but no join at all
//  //        }
//  //      }
//  //
//  //      val res = new RelationTraverser()
//  //      res.traverse(expr.expr.tree)
//  //      res.equiJoin
//  //    }
//  //
//  //    override protected def fire(r: ExpressionRoot, m: RuleMatch) = {
//  //      val freeVars = m.filter.expr match {
//  //        case ScalaExpr(env, _) => env
//  //        case _ => Map() // this can not happen as we only allow scala expr in the guard
//  //      }
//  //
//  //      val (prefix, suffix) = m.parent.qualifiers.span(_ != m.g1)
//  //      val join = EquiJoinCombinator(freeVars.head._1, m.filter, m.g1, m.g2)
//  //      m.parent.qualifiers = prefix ++ List(join) ++ suffix.tail.tail.filter(_ != m.filter)
//  //
//  //      val toSubstitute = globalSeq(m.parent).span(_ != join)._2.tail.span(_ != m.parent)._1
//  //
//  //      // replace var
//  //      substituteVars(freeVars.keySet.toList, toSubstitute)
//  //    }
//  //  }
//  //
//  //  //  /**
//  //  //   * [ e | qs, x ← xs, qs1, [ p x y | y ← ys](exists), qs2 ] →
//  //  //   * [ e | qs, x ← xs ⋉ ys, qs1, qs2 ]
//  //  //   */
//  //  //  object MatchSemiJoin extends Rule {}
//  //  //
//  //  //  /**
//  //  //   * [ e | qs, x ← xs, qs1, [ ¬ p x y | y ← ys](all), qs2 ] →
//  //  //   * [ e | qs, x ← xs ⋉ ys, qs1, qs2 ]
//  //  //   */
//  //  //  object MatchAntiJoin extends Rule {}
//  //
//  //  //----------------------------------------------------------------------------
//  //  // CROSS
//  //  //----------------------------------------------------------------------------
//  //
//  //  /**
//  //   * [ e | qs, x ← xs, y ← ys, qs1 ] →
//  //   * [ e[v.x/x][v.y/y] | qs, v ← xs ⨯ ys, qs1[v.x/x][v.y/y] ]
//  //   *
//  //   * for (x <- X; y <- x) yield y
//  //   */
//  //  object MatchCross extends Rule {
//  //
//  //    case class RuleMatch(parent: Comprehension[Any], generators: List[Generator[Any]])
//  //
//  //    override protected def bind(r: ExpressionRoot) = new Traversable[RuleMatch] {
//  //      override def foreach[U](f: (RuleMatch) => U) = {
//  //        for (x <- globalSeq(r.expr)) x match {
//  //          case parent@Comprehension(_, qs) =>
//  //            val builder = mutable.LinkedHashSet[Generator[Any]]()
//  //            for (List(q1, q2) <- qs.sliding(2)) (q1, q2) match {
//  //              case (a: Generator[Any], b: Generator[Any]) =>
//  //                builder += a
//  //
//  //                val contained = builder.foldLeft(false)((contained, q) => containsFreeVars(q.lhs, b) || contained)
//  //                if (contained) {
//  //                  if (builder.size >= 2) f(RuleMatch(parent, builder.toList))
//  //                  builder.clear()
//  //                }
//  //                builder += b
//  //              case (_, b: Generator[Any]) =>
//  //                builder += b
//  //              case (a: Generator[Any], _) =>
//  //                // as we have been in the fist case if we have a cross we do not have to check or add here
//  //                if (builder.size >= 2) f(RuleMatch(parent, builder.toList))
//  //                builder.clear()
//  //              case _ =>
//  //                // should never happen
//  //                throw new IllegalStateException()
//  //            }
//  //            if (builder.size >= 2) f(RuleMatch(parent, builder.toList))
//  //          case _ =>
//  //            Unit
//  //        }
//  //      }
//  //    }
//  //
//  //    // prevent matching of source and sink
//  //    override protected def guard(r: ExpressionRoot, m: RuleMatch) = true
//  //
//  //    def fuseTuple(ss: List[String]) = ???
//  //
//  //    override protected def fire(r: ExpressionRoot, m: RuleMatch) = {
//  //      val identifier = m.generators.head.lhs
//  //      val cross = CrossCombinator(identifier, m.generators)
//  //      val heads: Map[String, Type] = {
//  //        val builder = Map.newBuilder[String, Type]
//  //        for (g <- m.generators) {
//  //          builder += Tuple2(g.lhs, g.tag.tpe)
//  //        }
//  //        builder.result()
//  //      }
//  //
//  //      val (prefix, suffix) = m.parent.qualifiers.span(_ != m.generators.head)
//  //      m.parent.qualifiers = prefix ++ List(cross) ++ suffix.drop(m.generators.size)
//  //
//  //      val toSubstitute = globalSeq(m.parent).span(_ != cross)._2.tail.span(_ != m.parent)._1
//  //
//  //      // replace var
//  //      substituteVars(heads.keySet.toList, toSubstitute)
//  //    }
//  //  }
//
//  /**
//   * [ f e | qs ] → map f [ e | qs ]
//   *
//   * We expect that aggregations are matched beforehand, as we do not check here!
//   * Currently only maps with one input are supported!
//   */
//
//  /**
//   * Creates a map combinator. Assumes that aggregations are matched beforehand.
//   *
//   * ==Rule Description==
//   *
//   * '''Matching Pattern''':
//   * {{{ [[ f x  | x ← xs ]] }}}
//   *
//   * '''Rewrite''':
//   * {{{ map f xs }}}
//   */
//  object MatchMap extends Rule {
//
//    case class RuleMatch(head: ScalaExpr[Any], child: Generator[Any])
//
//    override protected def bind(r: Expression) = r match {
//      case Comprehension(head: ScalaExpr[Any], List(child: Generator[Any])) =>
//        Some(RuleMatch(head, child))
//      case _ =>
//        Option.empty[RuleMatch]
//    }
//
//    override protected def guard(r: Expression, m: RuleMatch) = true
//
//    override protected def fire(r: Expression, m: RuleMatch) = {
//      val f = q"(${TermName(m.head.env.head._1)}: ${m.head.env.head._2.actualType}) => ${m.head.expr.tree}"
//      MapCombinator(f, m.child.rhs)
//    }
//  }
//
//  /**
//   * Creates a flatMap combinator. Assumes that aggregations are matched beforehand.
//   *
//   * ==Rule Description==
//   *
//   * '''Matching Pattern''':
//   * {{{ [[ f x  | x ← xs ]] }}}
//   *
//   * '''Rewrite''':
//   * {{{ map f xs }}}
//   */
//  object MatchFlatMap extends Rule {
//
//    //TODO: at this point no single filter should be left, so list generator should be better? do this?
//    case class RuleMatch(head: ScalaExpr[Any], generators: List[Qualifier])
//
//    override protected def bind(r: Expression) = r match {
//      case MonadJoin(Comprehension(head: ScalaExpr[Any], generators)) =>
//        Some(RuleMatch(head, generators))
//      case _ =>
//        Option.empty[RuleMatch]
//    }
//
//    // prevent matching of source and sink
//    override protected def guard(r: Expression, m: RuleMatch) = true // m.generators.size == 1
//
//    override protected def fire(r: Expression, m: RuleMatch) = {
//      val f = q"(${TermName(m.head.env.head._1)}: ${m.head.env.head._2.actualType}) => ${m.head.expr.tree}"
//      FlatMapCombinator(f, m.generators.head.asInstanceOf[Generator[Any]].rhs)
//    }
//  }
//
//}
