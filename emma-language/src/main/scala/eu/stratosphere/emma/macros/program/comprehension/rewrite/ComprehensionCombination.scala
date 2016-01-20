package eu.stratosphere.emma.macros.program.comprehension.rewrite

import language.postfixOps

trait ComprehensionCombination extends ComprehensionRewriteEngine {
  import universe._
  import syntax._

  def combine(root: ExpressionRoot) = {
    // states for the state machine
    sealed trait RewriteState
    object Start     extends RewriteState
    object Filter    extends RewriteState
    object FlatMap   extends RewriteState
    object Join      extends RewriteState
    object Cross     extends RewriteState
    object Residuals extends RewriteState
    object End       extends RewriteState

    // state machine for the rewrite process
    def process(state: RewriteState): ExpressionRoot = state match {
      case Start     => process(Filter)
      case Filter    => applyExhaustively(MatchFilter)(root); process(FlatMap)
      case FlatMap   => if (applyAny(MatchFlatMap)(root))  process(Filter) else process(Join)
      case Join      => if (applyAny(MatchEquiJoin)(root)) process(Filter) else process(Cross)
      case Cross     => if (applyAny(MatchCross)(root))    process(Filter) else process(Residuals)
      case Residuals => applyExhaustively(MatchResidualComprehension)(root); process(End)
      case End       => root
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
   * ==Matching Pattern==
   * {{{ [[ e | qs, x ← xs, qs1, p x, qs2 ]] }}}
   *
   * ==Guard==
   * The filter expression `p x` must refer to at most one generator variable and this variable should be `x`.
   *
   * ==Rewrite==
   * {{{ [[ e | qs, x ← filter p xs, qs1, qs2 ]] }}}
   */
  object MatchFilter extends Rule {

    case class RuleMatch(root: Expression, parent: Comprehension, gen: Generator, filter: Filter)

    def bind(expr: Expression, root: Expression) = new Traversable[RuleMatch] {
      def foreach[U](f: RuleMatch => U) = expr match {
        case parent @ Comprehension(_, qualifiers) => for {
          filter @ Filter(_) <- qualifiers
          gen @ Generator(_, _) <- qualifiers takeWhile { _ != filter }
        } f(RuleMatch(root, parent, gen, filter))
          
        case _ =>
      }
    }

    def guard(rm: RuleMatch) =
      rm.filter.usedVars(rm.root) subsetOf Set(rm.gen.lhs)

    def fire(rm: RuleMatch) = {
      val RuleMatch(_, parent, gen, filter) = rm
      val p = lambda(gen.lhs) { unComprehend(filter.expr) }
      gen.rhs = combinator.Filter(p, gen.rhs)
      parent.qualifiers = parent.qualifiers diff List(filter)
      parent // return new parent
    }
  }

  /**
   * Creates a map combinator.
   *
   * ==Matching Pattern==
   * {{{ [[ f x  | x ← xs ]] }}}
   *
   * ==Guard==
   * The head expression `f x` must refer to at most one generator variable and this variable should be `x`.
   *
   * ==Rewrite==
   * {{{ map f xs }}}
   */
  object MatchMap extends Rule {

    case class RuleMatch(root: Expression, head: ScalaExpr, child: Generator)

    def bind(expr: Expression, root: Expression) = expr match {
      case comp @ Comprehension(head: ScalaExpr, List(child: Generator)) =>
        Some(RuleMatch(root, head, child))
      
      case _ => None
    }

    def guard(rm: RuleMatch) =
      rm.head.usedVars(rm.root) subsetOf Set(rm.child.lhs)

    def fire(rm: RuleMatch) = {
      val RuleMatch(_, ScalaExpr(tree), child) = rm
      combinator.Map(lambda(child.lhs) { tree }, child.rhs)
    }
  }

  /**
   * Creates a flatMap combinator.
   *
   * ===Matching Pattern===
   * {{{ [[ e | x ← xs, qs, y ← f x, qs' ]] }}}
   *
   * ==Guard==
   * 1. The rhs `f x` must refer to exactly one generator variable and this variable should be `x`.
   * 2. The remaining generators and guards, as well as the head should not refer to `x`.
   *
   * ==Rewrite==
   * {{{ [[ e | qs, y ← flatMap f xs, qs'' ]]  }}}
   */
  object MatchFlatMap extends Rule {

    case class RuleMatch(root: Expression, parent: Comprehension, g1: Generator, g2: Generator)

    def bind(expr: Expression, root: Expression) = new Traversable[RuleMatch] {
      def foreach[U](f: RuleMatch => U) = expr match {
        case parent @ Comprehension(_, qualifiers) =>
          val generators = qualifiers collect { case g: Generator => g }
          for {
            g1 <- generators
            g2 <- generators
            if g1 != g2
          } f(RuleMatch(root, parent, g1, g2))

        case _ =>
      }
    }

    def guard(rm: RuleMatch) = {
      val RuleMatch(_, parent, g1, g2) = rm
      parent.qualifiers :+ parent.hd forall { expr =>
        if (expr == g2) expr.usedVars(parent) == Set(g1.lhs)
        else !expr.usedVars(parent).contains(g1.lhs)
      }
    }

    def fire(rm: RuleMatch) = {
      val RuleMatch(_, parent, g1, g2 @ Generator(_, rhs)) = rm
      val f = lambda(g1.lhs) { unComprehend(rhs) }
      parent.qualifiers = for (q <- parent.qualifiers if q != g1)
        yield if (q == g2) Generator(g2.lhs, combinator.FlatMap(f, g1.rhs)) else q

      parent
    }
  }

  //---------------------------------------------------------------------------
  // JOINS
  //---------------------------------------------------------------------------

  /**
   * Creates an equi-join combinator.
   *
   * ==Matching Pattern==
   * {{{ [[ e | qs, x ← xs, y ← ys, qs1, k₁ x == k₂ y, qs2 ]] }}}
   *
   * ==Guard==
   * The generators should not be correlated, i.e. `xs` and `ys` should not refer to `y` and `x` correspondingly.
   *
   * ==Rewrite==
   * {{{ [[ e[v.x/x][v.y/y] | qs, v ← ⋈ k₁ k₂ xs ys, qs1[v.x/x][v.y/y], qs2[v.x/x][v.y/y] ]] }}}
   */
  object MatchEquiJoin extends Rule {

    case class RuleMatch(root: Expression, parent: Comprehension,
      xs: Generator, ys: Generator, filter: Filter, kx: Function, ky: Function)

    def bind(expr: Expression, root: Expression) = new Traversable[RuleMatch] {
      def foreach[U](f: RuleMatch => U) = expr match {
        case parent @ Comprehension(_, qualifiers) => for {
          filter @ Filter(p: ScalaExpr) <- qualifiers
          pairs = qualifiers takeWhile { _ != filter } sliding 2
          (xs: Generator) :: (ys: Generator) :: Nil <- pairs
          (kx, ky) <- parseJoinPredicate(root, xs, ys, p)
        } f(RuleMatch(root, parent, xs, ys, filter, kx, ky))

        case _ =>
      }
    }

    def guard(rm: RuleMatch) =
      !((rm.xs.usedVars(rm.root) contains rm.ys.lhs) ||
        (rm.ys.usedVars(rm.root) contains rm.xs.lhs))

    def fire(rm: RuleMatch) = {
      val RuleMatch(root, parent, xs, ys, filter, kx, ky) = rm
      val (prefix, suffix) = parent.qualifiers span { _ != xs }
      // construct combinator node with input and predicate sides aligned
      val join = combinator.EquiJoin(kx, ky, xs.rhs, ys.rhs)

      // bind join result to a fresh variable
      val xType = kx.vparams.head.preciseType
      val yType = ky.vparams.head.preciseType
      val tpe = PAIR(xType, yType)
      val term = mk.freeTerm($"join".toString, tpe)
      val qs = suffix drop 2 filter { _ != filter }

      for { // substitute [v._1/x] in affected expressions
        expr @ ScalaExpr(tree) <- parent
        if expr.usedVars(root) contains xs.lhs
        body = q"$term._1" :: xType
      } expr.tree = let (xs.lhs -> body) in tree

      for { // substitute [v._2/y] in affected expressions
        expr @ ScalaExpr(tree) <- parent
        if expr.usedVars(root) contains ys.lhs
        body = q"$term._2" :: yType
      } expr.tree = let (ys.lhs -> body) in tree

      // modify parent qualifier list
      parent.qualifiers = prefix ::: Generator(term, join) :: qs

      // return the modified parent
      parent
    }

    private def parseJoinPredicate(root: Expression, xs: Generator, ys: Generator, p: ScalaExpr):
      Option[(Function, Function)] = p.tree match {
        case q"${lhs: Tree} == ${rhs: Tree}" =>
          val defVars = p.definedVars(root)
          val lhsVars = defVars & lhs.closure
          val rhsVars = defVars & rhs.closure
          if (lhsVars.size != 1 || rhsVars.size != 1) {
            None // Both `lhs` and `rhs` must refer to exactly one variable
          } else if (lhsVars(xs.lhs) && rhsVars(ys.lhs)) {
            // Filter expression has the type `f(xs.lhs) == h(ys.lhs)`
            Some(lambda(xs.lhs) { lhs }, lambda(ys.lhs) { rhs })
          } else if (lhsVars(ys.lhs) && rhsVars(xs.lhs)) {
            // Filter expression has the type `f(ys.lhs) == h(xs.lhs)`
            Some(lambda(xs.lhs) { rhs }, lambda(ys.lhs) { lhs })
          } else None // Something else

        case _ => None // Something else
      }
  }

  //----------------------------------------------------------------------------
  // CROSS
  //----------------------------------------------------------------------------

  /**
   * Creates a cross combinator.
   *
   * ==Matching Pattern==
   * {{{ [[ e | qs, x ← xs, y ← ys, qs1 ]] }}}
   *
   * ==Guard==
   * The generators should not be be correlated, i.e. `xs` and `ys` should not refer to `y` and `x` correspondingly.
   *
   * ==Rewrite==
   * {{{ [[ e[v.x/x][v.y/y] | qs, v ← ⨯ xs ys, qs1[v.x/x][v.y/y] ]] }}}
   */
  object MatchCross extends Rule {

    case class RuleMatch(root: Expression, parent: Comprehension, xs: Generator, ys: Generator)

    def bind(expr: Expression, root: Expression) = new Traversable[RuleMatch] {
      def foreach[U](f: RuleMatch => U) = expr match {
        case parent @ Comprehension(_, qs) =>
          for ((xs: Generator) :: (ys: Generator) :: Nil <- qs sliding 2)
            f(RuleMatch(root, parent, xs, ys))

        case _ =>
      }
    }

    def guard(rm: RuleMatch) =
      !((rm.xs.usedVars(rm.root) contains rm.ys.lhs) ||
        (rm.ys.usedVars(rm.root) contains rm.xs.lhs))

    def fire(rm: RuleMatch) = {
      val RuleMatch(root, parent, xs, ys) = rm
      val (prefix, suffix) = parent.qualifiers span { _ != xs }

      // construct combinator node with input and predicate sides aligned
      val cross = combinator.Cross(xs.rhs, ys.rhs)

      // bind join result to a fresh variable
      val tpe = PAIR(xs.tpe, ys.tpe)
      val term = mk.freeTerm($"cross".toString, tpe)

      for { // substitute [v._1/x] in affected expressions
        expr @ ScalaExpr(tree) <- parent
        if expr.usedVars(root) contains xs.lhs
        body = q"$term._1" :: xs.tpe
      } expr.tree = let (xs.lhs -> body) in tree

      for { // substitute [v._2/y] in affected expressions
        expr @ ScalaExpr(tree) <- parent
        if expr.usedVars(root) contains ys.lhs
        body = q"$term._2" :: ys.tpe
      } expr.tree = let (ys.lhs -> body) in tree

      // modify parent qualifier list
      parent.qualifiers = prefix ::: Generator(term, cross) :: suffix.drop(2)

      // return the modified parent
      parent
    }
  }

  //---------------------------------------------------------------------------
  // Residual comprehensions
  //---------------------------------------------------------------------------

  /**
   * Matches a residual comprehension.
   *
   * ==Matching Pattern==
   * {{{ [[ e | qs ]] }}}
   *
   * ==Guard==
   * None.
   *
   * ==Rewrite==
   * {{{ MC [[ e | qs ]] }}}
   *
   * where the top node is represented as an IR combinator node and the inner nodes are part of the UDF.
   */
  object MatchResidualComprehension extends Rule {

    case class RuleMatch(root: Expression, parent: Comprehension)

    def bind(expr: Expression, root: Expression) = new Traversable[RuleMatch] {
      def foreach[U](f: RuleMatch => U) = expr match {
        case comp: Comprehension =>
          f(RuleMatch(root, comp))

        case _ =>
      }
    }

    def guard(rm: RuleMatch) = true

    def fire(rm: RuleMatch) = {
      val comp = rm.parent
      // the rhs of the first generator will be the input
      val in = comp.qualifiers collectFirst { case Generator(_, rhs) => rhs }
      assert(in.isDefined, "Undefined input for residual comprehension")

      unComprehend(comp) match {
        // xs.map(f)
        case q"${map: Select}[$_]($f)"
          if map.symbol == api.map =>
            combinator.Map(f, in.get)

        // xs.flatMap(f)
        case q"${flatMap: Select}[$_]($f)"
          if flatMap.symbol == api.flatMap =>
            combinator.FlatMap(f, in.get)

        // xs.withFilter(f)
        case q"${withFilter: Select}($f)"
          if withFilter.symbol == api.withFilter =>
            combinator.Filter(f, in.get)
      }
    }
  }
}

