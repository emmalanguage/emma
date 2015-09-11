package eu.stratosphere.emma.macros.program.comprehension.rewrite

trait ComprehensionCombination extends ComprehensionRewriteEngine {
  import universe._
  import c.internal._

  def combine(root: ExpressionRoot) = {
    // states for the state machine
    sealed trait RewriteState
    object Start  extends RewriteState
    object Filter extends RewriteState
    object Join   extends RewriteState
    object Cross  extends RewriteState
    object Map    extends RewriteState
    object End    extends RewriteState

    // state machine for the rewrite process
    def process(state: RewriteState): ExpressionRoot = state match {
      case Start  => process(Filter);
      case Filter => applyExhaustively(MatchFilter)(root); process(Join)
      case Join   => if (applyAny(MatchEquiJoin)(root)) process(Filter) else process(Cross)
      case Cross  => if (applyAny(MatchCross)(root))    process(Filter) else process(Map)
      case Map    => applyExhaustively(MatchMap, MatchFlatMap)(root); process(End)
      case End    => root
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

    case class RuleMatch(root: Comprehension, gen: Generator, filter: Filter)

    def bind(expr: Expression) = new Traversable[RuleMatch] {
      def foreach[U](f: RuleMatch => U) = expr match {
        case parent @ Comprehension(_, qualifiers) => for {
          filter @ Filter(_)    <- qualifiers
          gen @ Generator(_, _) <- qualifiers takeWhile { _ != filter }
        } f(RuleMatch(parent, gen, filter))
          
        case _ =>
      }
    }

    /**
     * Checks:
     *
     * - Filter uses only 1 variable
     * - Generator binds the variable of the filter
     */
    // TODO: add support for comprehension filter expressions
    def guard(rm: RuleMatch) = rm.filter.expr match {
      case expr: ScalaExpr => expr.usedVars match {
        case List(v) => v.name == rm.gen.lhs.name
        case _ => false
      }
        
      case _ => false
    }

    // TODO: add support for comprehension filter expressions
    def fire(rm: RuleMatch) = rm.filter.expr match {
      case expr @ ScalaExpr(vars, tree) =>
        val RuleMatch(root, gen, filter) = rm
        val arg  = expr.usedVars.head
        val body = tree.freeEnv(vars: _*)
        val p    = q"($arg) => $body".typeChecked
        gen.rhs         = combinator.Filter(p, gen.rhs)
        root.qualifiers = root.qualifiers diff List(filter)
        root // return new parent

      case filter => c.abort(c.enclosingPosition,
        s"Unexpected filter expression type: ${filter.getClass}")
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

    def bind(expr: Expression) = expr match {
      case Comprehension(head: ScalaExpr, List(child: Generator)) =>
        Some(RuleMatch(head, child))
      
      case _ => None
    }

    def guard(rm: RuleMatch) = 
      rm.head.usedVars.size == 1

    def fire(rm: RuleMatch) = {
      val RuleMatch(ScalaExpr(vars, tree), child) = rm
      val args = vars.reverse
      val body = tree.freeEnv(vars: _*)
      val f    = q"(..$args) => $body".typeChecked
      combinator.Map(f, child.rhs)
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

    def bind(expr: Expression) = expr match {
      case MonadJoin(Comprehension(head: ScalaExpr, List(child: Generator))) =>
        Some(RuleMatch(head, child))
      
      case _ => None
    }

    def guard(rm: RuleMatch) =
      rm.head.usedVars.size == 1

    def fire(rm: RuleMatch) = {
      val RuleMatch(ScalaExpr(vars, tree), child) = rm
      val args = vars.reverse
      val body = tree.freeEnv(vars: _*)
      val f    = q"(..$args) => $body".typeChecked
      combinator.FlatMap(f, child.rhs)
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

    case class RuleMatch(
      parent: Comprehension,
      xs:     Generator,
      ys:     Generator,
      filter: Filter,
      kx:     Function,
      ky:     Function)

    def bind(expr: Expression) = new Traversable[RuleMatch] {
      def foreach[U](f: RuleMatch => U) = expr match {
        case parent @ Comprehension(_, qualifiers) => for {
          filter @ Filter(p: ScalaExpr) <- qualifiers
          pairs = qualifiers takeWhile { _ != filter } sliding 2
          (xs: Generator) :: (ys: Generator) :: Nil <- pairs
          (kx, ky) <- parseJoinPredicate(xs, ys, p)
        } f(RuleMatch(parent, xs, ys, filter, kx, ky))

        case _ =>
      }
    }

    def guard(rm: RuleMatch) = true

    def fire(rm: RuleMatch) = {
      val RuleMatch(parent, xs, ys, filter, kx, ky) = rm
      val (prefix, suffix) = parent.qualifiers span { _ != xs }
      // construct combinator node with input and predicate sides aligned
      val join = combinator.EquiJoin(kx, ky, xs.rhs, ys.rhs)

      // bind join result to a fresh variable
      val tpt = tq"(${kx.vparams.head.tpt}, ${ky.vparams.head.tpt})"
      val vd  = valDef(freshName("x$"), tpt)
      val sym = newFreeTerm(vd.name.toString, ())
      val qs  = suffix drop 2 filter { _ != filter }
      parent.qualifiers = prefix ::: Generator(sym, join) :: qs

      // substitute [v._1/x] in affected expressions
      for (expr @ ScalaExpr(vars, _) <- parent if vars exists { _.name == xs.lhs.name })
        expr.substitute(xs.lhs.name, ScalaExpr(vd :: Nil, q"${vd.name}._1"))

      // substitute [v._2/y] in affected expressions
      for (expr @ ScalaExpr(vars, _) <- parent if vars exists { _.name == ys.lhs.name })
        expr.substitute(ys.lhs.name, ScalaExpr(vd :: Nil, q"${vd.name}._2"))

      // return the modified parent
      parent
    }

    private def parseJoinPredicate(xs: Generator, ys: Generator, p: ScalaExpr):
      Option[(Function, Function)] = p.tree match {
        // check if the predicate expression has the type `lhs == rhs`
        case Apply(Select(lhs, name), List(rhs)) if name.toString == "$eq$eq" =>
          val lhsVars = { // find expr.vars used in the `lhs`
            val names = lhs.collect { case Ident(n: TermName) => n }.toSet
            p.vars filter { v => names(v.name) }
          }

          val rhsVars = { // find expr.vars used in the `rhs`
            val names = rhs.collect { case Ident(n: TermName) => n }.toSet
            p.vars filter { v => names(v.name) }
          }

          if (lhsVars.size != 1 || rhsVars.size != 1) {
            None // both `lhs` and `rhs` must refer to exactly one variable
          } else if (lhsVars.exists { _.name == xs.lhs.name } &&
                     rhsVars.exists { _.name == ys.lhs.name }) {
            // filter expression has the type `f(xs.lhs) == h(ys.lhs)`
            val vx = lhsVars.find { _.name == xs.lhs.name }.get
            val vy = rhsVars.find { _.name == ys.lhs.name }.get
            val kx = q"($vx) => ${lhs.freeEnv(p.vars: _*)}".typeChecked.as[Function]
            val ky = q"($vy) => ${rhs.freeEnv(p.vars: _*)}".typeChecked.as[Function]
            Some(kx, ky)
          } else if (lhsVars.exists { _.name == ys.lhs.name } &&
                     rhsVars.exists { _.name == xs.lhs.name }) {
            // filter expression has the type `f(ys.lhs) == h(xs.lhs)`
            val vx = rhsVars.find { _.name == xs.lhs.name }.get
            val vy = lhsVars.find { _.name == ys.lhs.name }.get
            val kx = q"($vx) => ${rhs.freeEnv(p.vars: _*)}".typeChecked.as[Function]
            val ky = q"($vy) => ${lhs.freeEnv(p.vars: _*)}".typeChecked.as[Function]
            Some(kx, ky)
          } else None  // something else

        case _ => None // something else
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

    def bind(expr: Expression) = new Traversable[RuleMatch] {
      def foreach[U](f: RuleMatch => U) = expr match {
        case parent @ Comprehension(_, qs) =>
          for ((xs: Generator) :: (ys: Generator) :: Nil <- qs sliding 2)
            f(RuleMatch(parent, xs, ys))

        case _ =>
      }
    }

    def guard(rm: RuleMatch) = true

    def fire(rm: RuleMatch) = {
      val RuleMatch(parent, xs, ys) = rm
      val (prefix, suffix) = parent.qualifiers span { _ != xs }

      // construct combinator node with input and predicate sides aligned
      val cross = combinator.Cross(xs.rhs, ys.rhs)

      // bind join result to a fresh variable
      val vd  = valDef(freshName("x$"), tq"(${rm.xs.tpe}, ${rm.ys.tpe})")
      val sym = newFreeTerm(vd.name.toString, ())
      parent.qualifiers = prefix ::: Generator(sym, cross) :: suffix.drop(2)

      // substitute [v._1/x] in affected expressions
      for (expr @ ScalaExpr(vars, _) <- parent if vars exists { _.name == xs.lhs.name })
        expr.substitute(xs.lhs.name, ScalaExpr(vd :: Nil, q"${vd.name}._1"))

      // substitute [v._2/y] in affected expressions
      for (expr @ ScalaExpr(vars, _) <- parent if vars exists { _.name == ys.lhs.name })
        expr.substitute(ys.lhs.name, ScalaExpr(vd :: Nil, q"${vd.name}._2"))

      // return the modified parent
      rm.parent
    }
  }
}
