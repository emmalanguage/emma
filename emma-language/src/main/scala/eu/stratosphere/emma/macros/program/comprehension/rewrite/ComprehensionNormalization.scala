/*
 * Copyright © 2014 TU Berlin (emma@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
      case parent @ Comprehension(qs, _) => qs collectFirst {
        case gen @ Generator(_, child @ Comprehension(_, _: ScalaExpr)) =>
          RuleMatch(parent, gen, child)
      }
      
      case _ => None
    }

    def guard(rm: RuleMatch) = true

    def fire(rm: RuleMatch) = {
      val RuleMatch(parent, gen, child) = rm
      val (xs, ys) = parent.qs span { _ != gen }

      // construct new parent components
      val hd = parent.hd
      val qs = xs ::: child.qs ::: ys.tail
      // return new parent
      substituteExpr (gen.lhs -> child.hd.as[ScalaExpr].tree) in Comprehension(qs, hd)
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
      case MonadJoin(parent @ Comprehension(_, child)) =>
        Some(RuleMatch(parent, child))
      case _ => None
    }

    def guard(rm: RuleMatch) = true //FIXME

    def fire(rm: RuleMatch) = rm match {
      case RuleMatch(parent, child: Comprehension) =>
        // construct new parent components
        val hd = child.hd
        val qs = parent.qs ++ child.qs
        // return new parent
        Comprehension(qs, hd)

      case RuleMatch(parent, child: Expression) =>
        val term = mk.freeTerm($"head".toString, child.elementType)
        // construct new parent components
        val hd = ScalaExpr(&(term))
        val qs = parent.qs :+ Generator(term, child)
        // return new parent
        Comprehension(qs, hd)
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
      val ScalaExpr(tree) = rm.expr
      ScalaExpr(transform(tree) {
        case q"(..${args: List[Tree]}).${TermName(offset(i))}"
          if args.size > 1 => args(i.toInt - 1)
      })
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
      case fold @ c.Fold(_, _, _, map @ Comprehension(List(child: Generator), _: ScalaExpr), _) =>
        Some(RuleMatch(fold, map, child))
      
      case _ => None
    }

    def guard(rm: RuleMatch) = true // FIXME

    def fire(rm: RuleMatch) = {
      val RuleMatch(fold, map, child) = rm
      val tpe = fold.empty.preciseType
      val head = map.hd.as[ScalaExpr].tree
      val body = q"${fold.sng}($head)" :: tpe
      // construct new parent components
      val sng = lambda(child.lhs) { body }
      val xs = child.rhs
      // return new parent
      fold.copy(sng = sng, xs = xs)
    }
  }
}
