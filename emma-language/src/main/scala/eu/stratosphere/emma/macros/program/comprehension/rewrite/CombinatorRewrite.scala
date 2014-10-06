package eu.stratosphere.emma.macros.program.comprehension.rewrite

import eu.stratosphere.emma.macros.program.ContextHolder
import eu.stratosphere.emma.macros.program.comprehension.ComprehensionModel
import eu.stratosphere.emma.macros.program.util.ProgramUtils
import eu.stratosphere.emma.rewrite.RewriteEngine

import scala.reflect.macros.blackbox

trait CombinatorRewrite[C <: blackbox.Context]
  extends ContextHolder[C]
  with ComprehensionModel[C]
  with ProgramUtils[C]
  with RewriteEngine {

  import c.universe._

  def rewrite(root: ExpressionRoot) = {
    exhaust(MatchJoin, MatchFilter)(root) // combine qualifiers
    exhaust(MatchMap, MatchFlatMap)(root) // combine single qualifier heads
    root
  }

  /**
   * Exhaustively applies a set to a tree and all its subtrees.
   *
   * @param rules A set of rules to be applied.
   * @param root The root of the expression tree to be transformed.
   * @return The root of the transformed expression.
   */
  private def exhaust(rules: Rule*)(root: ExpressionRoot) = {
    object apply extends (ExpressionRoot => Boolean) with ExpressionTransformer {
      var changed = false

      override def transform(e: Expression): Expression = apply(rules, e) match {
        case Some(x) => changed = true; x
        case _ => super.transform(e)
      }

      private def apply(rules: Seq[Rule], e: Expression) = rules.foldLeft(Option.empty[Expression])((x: Option[Expression], r: Rule) => x match {
        case Some(_) => x
        case _ => r.apply(e)
      })

      override def apply(root: ExpressionRoot) = {
        changed = false
        root.expr = transform(root.expr)
        changed
      }
    }

    var changed = false
    do {
      changed = apply(root)
    } while (changed)
  }

  //---------------------------------------------------------------------------
  // MAP, FLATMAP, FILTER
  //---------------------------------------------------------------------------

  /**
   * Creates a filter combinator.
   *
   * ==Rule Description==
   *
   * '''Matching Pattern''':
   * {{{ [[ e | qs, x ← xs, qs1, p x, qs2 ]] }}}
   *
   * '''Rewrite''':
   * {{{ [[ e | qs, x ← filter p xs, qs1, qs2 ]] }}}
   */
  object MatchFilter extends Rule {

    case class RuleMatch(root: Comprehension, generator: Generator, filter: Filter)

    override protected def bind(r: Expression) = {
      var m = Option.empty[RuleMatch]

      r match {
        case parent@Comprehension(_, _) =>
          for (q <- parent.qualifiers) q match {
            case filter@Filter(expr) =>
              for (q <- parent.qualifiers.span(_ != filter)._1) q match {
                case generator: Generator =>
                  m = Some(RuleMatch(parent, generator, filter))
                case _ =>
              }
            case _ =>
          }
        case _ =>
      }

      m
    }

    /**
     * Checks:
     *
     * - Filter has only 1 free var
     * - Generator binds the free var of the filter
     */
    override protected def guard(r: Expression, m: RuleMatch) = {
      // TODO: add support for comprehension filter expressions
      m.filter.expr match {
        case ScalaExpr(vars, _) if vars.size == 1 => vars.head.name == m.generator.lhs
        case _ => false
      }
    }

    override protected def fire(r: Expression, m: RuleMatch) = {
      // TODO: add support for comprehension filter expressions
      m.filter.expr match {
        case ScalaExpr(vars, tree) =>
          val x = vars.head
          val p = c.typecheck(q"(${x.name}: ${x.tpt}) => ${freeEnv(vars)(tree)}")
          m.generator.rhs = combinator.Filter(p, m.generator.rhs)
          m.root.qualifiers = m.root.qualifiers.filter(_ != m.filter)
          // return new parent
          m.root
        case _ =>
          throw new RuntimeException("Unexpected filter expression type")
      }
    }
  }

  /**
   * Creates a map combinator. Assumes that aggregations are matched beforehand.
   *
   * ==Rule Description==
   *
   * '''Matching Pattern''':
   * {{{ [[ f x  | x ← xs ]] }}}
   *
   * '''Rewrite''':
   * {{{ map f xs }}}
   */
  object MatchMap extends Rule {

    case class RuleMatch(head: ScalaExpr, child: Generator)

    override protected def bind(r: Expression) = r match {
      case Comprehension(head: ScalaExpr, List(child: Generator)) =>
        Some(RuleMatch(head, child))
      case _ =>
        Option.empty[RuleMatch]
    }

    override protected def guard(r: Expression, m: RuleMatch) = m.head.vars.size == 1

    override protected def fire(r: Expression, m: RuleMatch) = {
      val f = c.typecheck(q"(..${for (v <- m.head.vars) yield q"val ${v.name}: ${v.tpt}"}) => ${freeEnv(m.head.vars)(m.head.tree)}")
      combinator.Map(f, m.child.rhs)
    }
  }

  /**
   * Creates a flatMap combinator. Assumes that aggregations are matched beforehand.
   *
   * ==Rule Description==
   *
   * '''Matching Pattern''':
   * {{{ join [[ f x  | x ← xs ]] }}}
   *
   * '''Rewrite''':
   * {{{ flatMap f xs }}}
   */
  object MatchFlatMap extends Rule {

    case class RuleMatch(head: ScalaExpr, child: Generator)

    override protected def bind(r: Expression) = r match {
      case MonadJoin(Comprehension(head: ScalaExpr, List(child: Generator))) =>
        Some(RuleMatch(head, child))
      case _ =>
        Option.empty[RuleMatch]
    }

    override protected def guard(r: Expression, m: RuleMatch) = m.head.vars.size == 1

    override protected def fire(r: Expression, m: RuleMatch) = {
      val f = c.typecheck(q"(..${for (v <- m.head.vars) yield q"val ${v.name}: ${v.tpt}"}) => ${freeEnv(m.head.vars)(m.head.tree)}")
      combinator.FlatMap(f, m.child.rhs)
    }
  }

  //---------------------------------------------------------------------------
  // JOINS
  //---------------------------------------------------------------------------

  /**
   * Creates a map combinator. Assumes that aggregations are matched beforehand.
   *
   * ==Rule Description==
   *
   * '''Matching Pattern''':
   * {{{ [[ e | qs, x ← xs, y ← ys, qs1, p x y, qs2 ]] }}}
   *
   * '''Rewrite''':
   * {{{ [[ e[v.x/x][v.y/y] | qs, v ← ⋈ p xs ys, qs1[v.x/x][v.y/y], qs2[v.x/x][v.y/y] ]] }}}
   */
  object MatchJoin extends Rule {

    case class RuleMatch(parent: Comprehension, xs: Generator, ys: Generator, p: ScalaExpr)

    override protected def bind(r: Expression) = r match {
      case parent@Comprehension(_, _) =>
        var m = Option.empty[RuleMatch]
        for (q <- parent.qualifiers) q match {
          case Filter(filter: ScalaExpr) =>
            for (List(q1, q2) <- parent.qualifiers.span(_ != filter)._1.sliding(2)) (q1, q2) match {
              case (a: Generator, b: Generator) => m = Some(RuleMatch(parent, a, b, filter))
              case _ =>
            }
          case _ =>
        }
        m
      case _ => Option.empty[RuleMatch]
    }

    override protected def guard(r: Expression, m: RuleMatch) = {
      m.p.vars.size == 2 && isEquiJoin(m.p) &&
        ((m.xs.lhs == m.p.vars.head.name && m.ys.lhs == m.p.vars.reverse.head.name) ||
          (m.xs.lhs == m.p.vars.reverse.head.name && m.ys.lhs == m.p.vars.head.name))
    }

    override protected def fire(r: Expression, m: RuleMatch) = {
      val (prefix, suffix) = m.parent.qualifiers.span(_ != m.xs)
      val p = c.typecheck(q"(..${for (v <- m.p.vars) yield q"val ${v.name}: ${v.tpt}"}) => ${freeEnv(m.p.vars)(m.p.tree)}")

      //m.parent.qualifiers = prefix ++ List(join) ++ suffix.tail.tail.filter(_ != m.p)
      //val toSubstitute = globalSeq(m.parent).span(_ != join)._2.tail.span(_ != m.parent)._1 FIXME
      // replace var FIXME
      //substituteVars(freeVars.keySet.toList, toSubstitute)

      combinator.EquiJoin(p, m.xs.rhs, m.ys.rhs)
    }

    private def isEquiJoin(expr: ScalaExpr): Boolean = {
      val traverser = new Traverser with (Tree => Boolean) {
        var equiJoin: Boolean = false

        override def traverse(tree: Tree): Unit = tree match {
          case Apply(Select(_, name), _) if name.toString == "$eq$eq" => equiJoin = true
          case _ =>
        }

        override def apply(tree: Tree) = {
          traverse(tree);
          equiJoin
        }
      }
      traverser(expr.tree)
    }
  }

  //----------------------------------------------------------------------------
  // CROSS
  //----------------------------------------------------------------------------

  /**
   * [ e | qs, x ← xs, y ← ys, qs1 ] →
   * [ e[v.x/x][v.y/y] | qs, v ← xs ⨯ ys, qs1[v.x/x][v.y/y] ]
   *
   * for (x <- X; y <- x) yield y
   */
  //  object MatchCross extends Rule {
  //
  //    case class RuleMatch(parent: Comprehension[Any], generators: List[Generator[Any]])
  //
  //    override protected def bind(r: ExpressionRoot) = new Traversable[RuleMatch] {
  //      override def foreach[U](f: (RuleMatch) => U) = {
  //        for (x <- globalSeq(r.expr)) x match {
  //          case parent@Comprehension(_, qs) =>
  //            val builder = mutable.LinkedHashSet[Generator[Any]]()
  //            for (List(q1, q2) <- qs.sliding(2)) (q1, q2) match {
  //              case (a: Generator[Any], b: Generator[Any]) =>
  //                builder += a
  //
  //                val contained = builder.foldLeft(false)((contained, q) => containsFreeVars(q.lhs, b) || contained)
  //                if (contained) {
  //                  if (builder.size >= 2) f(RuleMatch(parent, builder.toList))
  //                  builder.clear()
  //                }
  //                builder += b
  //              case (_, b: Generator[Any]) =>
  //                builder += b
  //              case (a: Generator[Any], _) =>
  //                // as we have been in the fist case if we have a cross we do not have to check or add here
  //                if (builder.size >= 2) f(RuleMatch(parent, builder.toList))
  //                builder.clear()
  //              case _ =>
  //                // should never happen
  //                throw new IllegalStateException()
  //            }
  //            if (builder.size >= 2) f(RuleMatch(parent, builder.toList))
  //          case _ =>
  //            Unit
  //        }
  //      }
  //    }
  //
  //    // prevent matching of source and sink
  //    override protected def guard(r: ExpressionRoot, m: RuleMatch) = true
  //
  //    def fuseTuple(ss: List[String]) = ???
  //
  //    override protected def fire(r: ExpressionRoot, m: RuleMatch) = {
  //      val identifier = m.generators.head.lhs
  //      val cross = CrossCombinator(identifier, m.generators)
  //      val heads: Map[String, Type] = {
  //        val builder = Map.newBuilder[String, Type]
  //        for (g <- m.generators) {
  //          builder += Tuple2(g.lhs, g.tag.tpe)
  //        }
  //        builder.result()
  //      }
  //
  //      val (prefix, suffix) = m.parent.qualifiers.span(_ != m.generators.head)
  //      m.parent.qualifiers = prefix ++ List(cross) ++ suffix.drop(m.generators.size)
  //
  //      val toSubstitute = globalSeq(m.parent).span(_ != cross)._2.tail.span(_ != m.parent)._1
  //
  //      // replace var
  //      substituteVars(heads.keySet.toList, toSubstitute)
  //    }
  //  }

}
