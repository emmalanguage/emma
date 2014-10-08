package eu.stratosphere.emma.macros.program.comprehension.rewrite

import eu.stratosphere.emma.macros.program.ContextHolder
import eu.stratosphere.emma.macros.program.util.ProgramUtils

import scala.reflect.macros.blackbox

trait ComprehensionCombination[C <: blackbox.Context]
  extends ContextHolder[C]
  with ComprehensionRewriteEngine[C]
  with ProgramUtils[C] {

  import c.universe._

  def rewrite(root: ExpressionRoot) = {
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

    // run the state machine
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
    override protected def guard(m: RuleMatch) = {
      // TODO: add support for comprehension filter expressions
      m.filter.expr match {
        case ScalaExpr(vars, _) if vars.size == 1 => vars.head.name == m.generator.lhs
        case _ => false
      }
    }

    override protected def fire(m: RuleMatch) = {
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

    override protected def guard(m: RuleMatch) = m.head.vars.size == 1

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
      case MonadJoin(Comprehension(head: ScalaExpr, List(child: Generator))) =>
        Some(RuleMatch(head, child))
      case _ =>
        Option.empty[RuleMatch]
    }

    override protected def guard(m: RuleMatch) = m.head.vars.size == 1

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
   * {{{ [[ e | qs, x ← xs, y ← ys, qs1, p x y, qs2 ]] }}}
   *
   * '''Rewrite''':
   * {{{ [[ e[v.x/x][v.y/y] | qs, v ← ⋈ p xs ys, qs1[v.x/x][v.y/y], qs2[v.x/x][v.y/y] ]] }}}
   */
  object MatchEquiJoin extends Rule {

    case class RuleMatch(parent: Comprehension, xs: Generator, ys: Generator, filter: Filter, p: ScalaExpr)

    override protected def bind(r: Expression) = r match {
      case parent@Comprehension(_, _) =>
        var m = Option.empty[RuleMatch]
        for (q <- parent.qualifiers) q match {
          case filter@Filter(p: ScalaExpr) =>
            for (List(q1, q2) <- parent.qualifiers.span(_ != filter)._1.sliding(2)) (q1, q2) match {
              case (xs: Generator, ys: Generator) => m = Some(RuleMatch(parent, xs, ys, filter, p))
              case _ =>
            }
          case _ =>
        }
        m
      case _ => Option.empty[RuleMatch]
    }

    override protected def guard(m: RuleMatch) = {
      m.p.vars.size == 2 && isEquiJoin(m.p) &&
        ((m.xs.lhs == m.p.vars.head.name && m.ys.lhs == m.p.vars.reverse.head.name) ||
          (m.xs.lhs == m.p.vars.reverse.head.name && m.ys.lhs == m.p.vars.head.name))
    }

    override protected def fire(m: RuleMatch) = {
      val (prefix, suffix) = m.parent.qualifiers.span(_ != m.xs)
      val p = c.typecheck(q"(..${for (v <- m.p.vars.reverse) yield q"val ${v.name}: ${v.tpt}"}) => ${freeEnv(m.p.vars)(m.p.tree)}")

      // construct combinator node with input and predicate sides aligned
      val join =
        if (m.xs.lhs == m.p.vars.head.name && m.ys.lhs == m.p.vars.reverse.head.name)
          combinator.EquiJoin(p, m.ys.rhs, m.xs.rhs)
        else // (m.xs.lhs == m.p.vars.reverse.head.name && m.ys.lhs == m.p.vars.head.name)
          combinator.EquiJoin(p, m.xs.rhs, m.ys.rhs)

      // bind join result to a fresh variable
      val v = Variable(TermName(c.freshName("x")), tq"(..${for (v <- m.p.vars.reverse) yield v.tpt})")
      m.parent.qualifiers = prefix ++ List(Generator(v.name, join)) ++ suffix.tail.tail.filter(_ != m.filter)

      // substitute [v._1/x] in affected expressions
      for (e <- m.parent.collect({
        case e@ScalaExpr(vars, expr) if vars.contains(m.p.vars.reverse.head) => e
      })) substitute(e, m.p.vars.reverse.head.name, ScalaExpr(List(v), q"${v.name}._1"))

      // substitute [v._2/y] in affected expressions
      for (e <- m.parent.collect({
        case e@ScalaExpr(vars, expr) if vars.contains(m.p.vars.head) => e
      })) substitute(e, m.p.vars.head.name, ScalaExpr(List(v), q"${v.name}._2"))

      // return the modified parent
      m.parent
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

    override protected def bind(r: Expression) = r match {
      case parent@Comprehension(_, qs) =>
        qs.sliding(2).collectFirst({
          case (xs: Generator) :: (ys: Generator) :: Nil if !qs.exists(equiJoinPredicate(xs, ys)) => (xs, ys)
        }) match {
          case Some((xs: Generator, ys: Generator)) => Some(RuleMatch(parent, xs, ys))
          case None => Option.empty[RuleMatch]
        }
      case _ =>
        Option.empty[RuleMatch]
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

    private def equiJoinPredicate(xs: Generator, ys: Generator)(q: Qualifier): Boolean = q match {
      // match a filter node holding a scala expression `e` of the form `lhs` == `rhs`
      case Filter(ScalaExpr(_, Apply(Select(lhs, TermName("$eq$eq")), rhs))) =>
        // terms used in the lhs
        val lhsTermNames = lhs.collect({
          case Ident(name@TermName(_)) => name
        })
        // terms used in the rhs
        val rhsTermNames = lhs.collect({
          case Ident(name@TermName(_)) => name
        })
        // check condition
        (lhsTermNames.size == 1 && rhsTermNames.size == 1) && // singleton sets
          ((lhsTermNames.contains(xs.lhs) && rhsTermNames.contains(xs.rhs)) || // f(xs.lhs) == g(ys.lhs)
            (lhsTermNames.contains(xs.rhs) && rhsTermNames.contains(xs.lhs))) // f(ys.lhs) == g(xs.lhs)
      case _ =>
        false
    }
  }

}
