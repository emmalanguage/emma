package eu.stratosphere.emma.macros.program.comprehension.rewrite

import eu.stratosphere.emma.macros.program.ContextHolder
import eu.stratosphere.emma.macros.program.util.ProgramUtils

import scala.reflect.macros.blackbox

trait ComprehensionCombination[C <: blackbox.Context]
  extends ContextHolder[C]
  with ComprehensionRewriteEngine[C]
  with ProgramUtils[C] {

  import c.universe._

  def combine(root: ExpressionRoot) = {
    // states for the state machine
    sealed trait RewriteState {}
    object Start extends RewriteState
    object Filter extends RewriteState
    object Join extends RewriteState
    object Cross extends RewriteState
    object Map extends RewriteState
    object End extends RewriteState

    // state machine for the rewrite process
    def process(state: RewriteState): ExpressionRoot = state match {
      case Start => process(Filter);
      case Filter => applyExhaustively(MatchFilter)(root); process(Join)
      case Join => if (applyAny(MatchEquiJoin)(root)) process(Filter) else process(Cross)
      case Cross => if (applyAny(MatchCross)(root)) process(Filter) else process(Map)
      case Map => applyExhaustively(MatchMap, MatchFlatMap)(root); process(End)
      case End => root
    }

    // (1) run the state machine and (2) simplify UDF parameter names
    process(Start)
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

    override protected def bind(r: Expression) = new Traversable[RuleMatch] {
      def foreach[U](f: RuleMatch => U) = {
        r match {
          case parent@Comprehension(_, _) =>
            for (q <- parent.qualifiers) q match {
              case filter@Filter(expr) =>
                for (q <- parent.qualifiers.span(_ != filter)._1) q match {
                  case generator: Generator => f(RuleMatch(parent, generator, filter))
                  case _ =>
                }
              case _ =>
            }
          case _ =>
        }
      }
    }

    /**
     * Checks:
     *
     * - Filter uses only 1 variable
     * - Generator binds the variable of the filter
     */
    override protected def guard(m: RuleMatch) = {
      // TODO: add support for comprehension filter expressions
      m.filter.expr match {
        case expr: ScalaExpr => expr.usedVars.size == 1 && expr.usedVars.head.name == m.generator.lhs
        case _ => false
      }
    }

    override protected def fire(m: RuleMatch) = {
      // TODO: add support for comprehension filter expressions
      m.filter.expr match {
        case filter@ScalaExpr(vars, tree) =>
          val x = filter.usedVars.head
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
      case Comprehension(head: ScalaExpr, List(child: Generator)) => Some(RuleMatch(head, child))
      case _ => Option.empty[RuleMatch]
    }

    override protected def guard(m: RuleMatch) = m.head.usedVars.size == 1

    override protected def fire(m: RuleMatch) = {
      val f = c.typecheck(q"(..${for (v <- m.head.vars.reverse) yield q"val ${v.name}: ${v.tpt}"}) => ${freeEnv(m.head.vars)(m.head.tree)}")
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
      case MonadJoin(Comprehension(head: ScalaExpr, List(child: Generator))) => Some(RuleMatch(head, child))
      case _ => Option.empty[RuleMatch]
    }

    override protected def guard(m: RuleMatch) = m.head.usedVars.size == 1

    override protected def fire(m: RuleMatch) = {
      val f = c.typecheck(q"(..${for (v <- m.head.vars.reverse) yield q"val ${v.name}: ${v.tpt}"}) => ${freeEnv(m.head.vars)(m.head.tree)}")
      combinator.FlatMap(f, m.child.rhs)
    }
  }

  //---------------------------------------------------------------------------
  // JOINS
  //---------------------------------------------------------------------------

  /**
   * Creates an equi-join combinator.
   *
   * ==Rule Description==
   *
   * '''Matching Pattern''':
   * {{{ [[ e | qs, x ← xs, y ← ys, qs1, k₁ x == k₂ y, qs2 ]] }}}
   *
   * '''Rewrite''':
   * {{{ [[ e[v.x/x][v.y/y] | qs, v ← ⋈ k₁ k₂ xs ys, qs1[v.x/x][v.y/y], qs2[v.x/x][v.y/y] ]] }}}
   */
  object MatchEquiJoin extends Rule {

    case class RuleMatch(parent: Comprehension, xs: Generator, ys: Generator, filter: Filter, keyx: Function, keyy: Function)

    override protected def bind(r: Expression) = new Traversable[RuleMatch] {
      def foreach[U](f: RuleMatch => U) = {
        r match {
          case parent@Comprehension(_, _) =>
            for (q <- parent.qualifiers) q match {
              case filter@Filter(p: ScalaExpr) =>
                // check all generator pairs before the filter
                for (pair <- parent.qualifiers.span(_ != filter)._1.sliding(2)) pair match {
                  case (xs: Generator) :: (ys: Generator) :: Nil =>
                    parseJoinPredicate(xs, ys, p) match {
                      case Some((keyx, keyy)) => f(RuleMatch(parent, xs, ys, filter, keyx, keyy))
                      case _ =>
                    }
                  case _ =>
                }
              case _ =>
            }
          case _ =>
        }
      }
    }

    override protected def guard(m: RuleMatch) = true

    override protected def fire(m: RuleMatch) = {
      val (prefix, suffix) = m.parent.qualifiers.span(_ != m.xs)
      // construct combinator node with input and predicate sides aligned
      val join = combinator.EquiJoin(m.keyx, m.keyy, m.xs.rhs, m.ys.rhs)

      // bind join result to a fresh variable
      val v = Variable(TermName(c.freshName("x")), tq"(${m.keyx.vparams.head.tpt}, ${m.keyy.vparams.head.tpt})")
      m.parent.qualifiers = prefix ++ List(Generator(v.name, join)) ++ suffix.tail.tail.filter(_ != m.filter)

      // substitute [v._1/x] in affected expressions
      for (e <- m.parent.collect({
        case e@ScalaExpr(vars, expr) if vars.map(_.name).contains(m.xs.lhs) => e
      })) substitute(e, m.xs.lhs, ScalaExpr(List(v), q"${v.name}._1"))

      // substitute [v._2/y] in affected expressions
      for (e <- m.parent.collect({
        case e@ScalaExpr(vars, expr) if vars.map(_.name).contains(m.ys.lhs) => e
      })) substitute(e, m.ys.lhs, ScalaExpr(List(v), q"${v.name}._2"))

      // return the modified parent
      m.parent
    }

    private def parseJoinPredicate(xs: Generator, ys: Generator, p: ScalaExpr): Option[(Function, Function)] = {
      p.tree match {
        // check if the predicate expression has the type `lhs == rhs`
        case Apply(Select(lhs, name), List(rhs)) if name.toString == "$eq$eq" =>
          // find expr.vars used in the `lhs`
          val lhsUsedVars = {
            val used = lhs.collect({
              case Ident(n: TermName) => n
            })
            p.vars.filter(v => used.contains(v.name))
          }
          // find expr.vars used in the `rhs`
          val rhsUsedVars = {
            val used = rhs.collect({
              case Ident(n: TermName) => n
            })
            p.vars.filter(v => used.contains(v.name))
          }

          if (lhsUsedVars.size != 1 || rhsUsedVars.size != 1) {
            None // both `lhs` and `rhs` must refer to exactly one variable
          } else if (lhsUsedVars.map(_.name).contains(xs.lhs) && rhsUsedVars.map(_.name).contains(ys.lhs)) {
            // filter expression has the type `f(xs.lhs) == h(ys.lhs)`
            val vx = lhsUsedVars.find(v => v.name == xs.lhs).get
            val vy = rhsUsedVars.find(v => v.name == ys.lhs).get
            val keyx = c.typecheck(q"(${vx.name}: ${vx.tpt}) => ${freeEnv(p.vars)(lhs)}").asInstanceOf[Function]
            val keyy = c.typecheck(q"(${vy.name}: ${vy.tpt}) => ${freeEnv(p.vars)(rhs)}").asInstanceOf[Function]
            Some((keyx, keyy))
          } else if (lhsUsedVars.map(_.name).contains(ys.lhs) && rhsUsedVars.map(_.name).contains(xs.lhs)) {
            // filter expression has the type `f(ys.lhs) == h(xs.lhs)`
            val vx = rhsUsedVars.find(v => v.name == xs.lhs).get
            val vy = lhsUsedVars.find(v => v.name == ys.lhs).get
            val keyx = c.typecheck(q"(${vx.name}: ${vx.tpt}) => ${freeEnv(p.vars)(rhs)}").asInstanceOf[Function]
            val keyy = c.typecheck(q"(${vy.name}: ${vy.tpt}) => ${freeEnv(p.vars)(lhs)}").asInstanceOf[Function]
            Some((keyx, keyy))
          } else {
            None // something else
          }
        case _ =>
          None // something else
      }
    }
  }

  //----------------------------------------------------------------------------
  // CROSS
  //----------------------------------------------------------------------------

  /**
   * Creates a cross combinator.
   *
   * ==Rule Description==
   *
   * '''Matching Pattern''':
   * {{{ [[ e | qs, x ← xs, y ← ys, qs1 ]] }}}
   *
   * '''Rewrite''':
   * {{{ [[ e[v.x/x][v.y/y] | qs, v ← ⨯ xs ys, qs1[v.x/x][v.y/y] ]] }}}
   */
  object MatchCross extends Rule {

    case class RuleMatch(parent: Comprehension, xs: Generator, ys: Generator)

    override protected def bind(r: Expression) = new Traversable[RuleMatch] {
      def foreach[U](f: RuleMatch => U) = {
        r match {
          case parent@Comprehension(_, qs) =>
            for (pair <- qs.sliding(2)) pair match {
              case (xs: Generator) :: (ys: Generator) :: Nil => f(RuleMatch(parent, xs, ys))
              case _ =>
            }
          case _ =>
        }
      }
    }

    override protected def guard(m: RuleMatch) = true

    override protected def fire(m: RuleMatch) = {
      val (prefix, suffix) = m.parent.qualifiers.span(_ != m.xs)

      // construct combinator node with input and predicate sides aligned
      val cross = combinator.Cross(m.xs.rhs, m.ys.rhs)

      // bind join result to a fresh variable
      val v = Variable(TermName(c.freshName("x")), tq"(${m.xs.tpe}, ${m.ys.tpe})")
      m.parent.qualifiers = prefix ++ List(Generator(v.name, cross)) ++ suffix.tail.tail

      // substitute [v._1/x] in affected expressions
      for (e <- m.parent.collect({
        case e@ScalaExpr(vars, expr) if vars.map(_.name).contains(m.xs.lhs) => e
      })) substitute(e, m.xs.lhs, ScalaExpr(List(v), q"${v.name}._1"))

      // substitute [v._2/y] in affected expressions
      for (e <- m.parent.collect({
        case e@ScalaExpr(vars, expr) if vars.map(_.name).contains(m.ys.lhs) => e
      })) substitute(e, m.ys.lhs, ScalaExpr(List(v), q"${v.name}._2"))

      // return the modified parent
      m.parent
    }
  }
}
