package eu.stratosphere.emma.macros.program.comprehension.rewrite

trait ComprehensionNormalization extends ComprehensionRewriteEngine {
  import universe._
  import syntax._

  def normalize(root: ExpressionRoot) = {
    applyExhaustively(UnnestHead, UnnestGenerator, SimplifyTupleProjection)(root)
    applyExhaustively(FoldFusion)(root)
    root
  }

  /**
   * Un-nests a comprehended head in its parent.
   *
   * ==Rule Description==
   *
   * '''Matching Pattern''':
   * {{{ [[ e | qs, x ← [[ e' | qs' ]], qs'' ]] }}}
   *
   * '''Rewrite''':
   * {{{ [[ e[e'\x] | qs,  qs', qs''[e'\x] ]] }}}
   */
  object UnnestGenerator extends Rule {

    case class RuleMatch(parent: Comprehension, gen: Generator, child: Comprehension)

    def bind(expr: Expression, root: Expression) = expr match {
      case parent @ Comprehension(_, qualifiers) => qualifiers collectFirst {
        case gen @ Generator(_, child @ Comprehension(_: ScalaExpr, _)) =>
          RuleMatch(parent, gen, child)
      }
      
      case _ => None
    }

    def guard(rm: RuleMatch) = true

    def fire(rm: RuleMatch) = {
      val RuleMatch(parent, gen, child) = rm
      val rest = parent
        .dropWhile { _ != gen }
        .tail // trim prefix
        .takeWhile {
          case Generator(lhs, _) => lhs.name.toString != gen.toString
          case _ => true
        } // trim suffix

      val (xs, ys) = parent.qualifiers span { _ != gen }
      parent.qualifiers = xs ::: child.qualifiers ::: ys.tail

      for (expr @ ScalaExpr(tree) <- rest)
        expr.tree = let (gen.lhs -> child.hd.as[ScalaExpr].tree) in tree

      // new parent
      rm.parent
    }
  }

  /**
   * Unnests a comprehended head in its parent.
   *
   * ==Rule Description==
   *
   * '''Matching Pattern''':
   * {{{ join [[ [[ e | qs' ]] | qs ]] }}}
   *
   * '''Rewrite''':
   * {{{ [[ e | qs, qs' ]] }}}
   */
  object UnnestHead extends Rule {

    case class RuleMatch(parent: Comprehension, child: Expression)

    def bind(expr: Expression, root: Expression) = expr match {
      case MonadJoin(parent @ Comprehension(child, _)) =>
        Some(RuleMatch(parent, child))
      case _ => None
    }

    def guard(rm: RuleMatch) = true //FIXME

    def fire(rm: RuleMatch) = rm match {
      case RuleMatch(parent, child: Comprehension) =>
        parent.qualifiers ++= child.qualifiers
        parent.hd = child.hd
        parent // return new root

      case RuleMatch(parent, child: Expression) =>
        val term = mk.freeTerm($"head".toString, child.elementType)
        parent.qualifiers ++= Generator(term, child) :: Nil
        parent.hd = ScalaExpr(&(term))
        parent // return new root
    }
  }

  object SimplifyTupleProjection extends Rule {

    val offset = "_([1-9]+)".r

    case class RuleMatch(expr: ScalaExpr)

    def bind(expr: Expression, root: Expression) = expr match {
      case expr: ScalaExpr => Some(RuleMatch(expr))
      case _ => None
    }

    def guard(rm: RuleMatch) = rm.expr.tree exists {
      case q"(..${List(_, _, _*)}).${TermName(offset(_))}" => true
      case _ => false
    }

    def fire(rm: RuleMatch) = {
      val expr @ ScalaExpr(tree) = rm.expr
      expr.tree = transform(tree) {
        case q"(..${args: List[Tree]}).${TermName(offset(i))}"
          if args.size > 1 => args(i.toInt - 1)
      }

      expr
    }
  }

  /**
   * Fuses a fold with a child comprehension consisting of a single generator.
   *
   * ==Rule Description==
   *
   * '''Matching Pattern''':
   * {{{ fold( empty, sng, union, [[ e | x ← e' ]] ) }}}
   *
   * '''Rewrite''':
   * {{{ fold( empty, sng[e[x\z]\x], union[e[x\z]\x, e[y\z]\y], e' ]] ) }}}
   */
  object FoldFusion extends Rule {
    val c = combinator

    case class RuleMatch(fold: c.Fold, map: Comprehension, child: Generator)

    def bind(expr: Expression, root: Expression) = expr match {
      case fold @ c.Fold(_, _, _, map @ Comprehension(_: ScalaExpr, List(child: Generator)), _) =>
        Some(RuleMatch(fold, map, child))
      
      case _ => None
    }

    def guard(rm: RuleMatch) = true // FIXME

    def fire(rm: RuleMatch) = {
      val RuleMatch(fold, map, child) = rm
      val tpe = fold.empty.preciseType
      val head = map.hd.as[ScalaExpr].tree
      val body = q"${fold.sng}($head)" :: tpe
      fold.sng = lambda(child.lhs) { body }
      fold.xs = child.rhs
      fold // return new root
    }
  }
}
