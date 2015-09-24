package eu.stratosphere.emma.macros.program.comprehension.rewrite

trait ComprehensionNormalization extends ComprehensionRewriteEngine {
  import universe._

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
        .dropWhile { _ != gen }.tail // trim prefix
        .takeWhile { expr => !expr.isInstanceOf[Generator] ||
          expr.as[Generator].lhs.fullName != gen.toString
        } // trim suffix

      val (xs, ys) = parent.qualifiers span { _ != gen }
      parent.qualifiers = xs ::: child.qualifiers ::: ys.tail

      for (expr <- rest if expr.isInstanceOf[ScalaExpr])
        expr.as[ScalaExpr].substitute(gen.lhs.name, child.hd.as[ScalaExpr])

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

    case class RuleMatch(parent: Comprehension, child: Comprehension)

    def bind(expr: Expression, root: Expression) = expr match {
      case MonadJoin(parent @ Comprehension(child: Comprehension, _)) =>
        Some(RuleMatch(parent, child))
      
      case _ => None
    }

    def guard(rm: RuleMatch) = true //FIXME

    def fire(rm: RuleMatch) = {
      val RuleMatch(parent, child) = rm
      parent.qualifiers ++= child.qualifiers
      parent.hd = child.hd
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
      val expr  = rm.expr
      expr.tree = expr.tree transform {
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
      val x    = freshName("x")
      val head = map.hd.as[ScalaExpr].tree.rename(child.lhs.name, x)
      val sng  = fold.sng.as[Function]
      val body = sng.body.substitute(sng.vparams.head.name, head)
      fold.sng = q"($x: ${child.tpe}) => $body".typeChecked
      fold.xs  = child.rhs
      fold // return new root
    }
  }
}
