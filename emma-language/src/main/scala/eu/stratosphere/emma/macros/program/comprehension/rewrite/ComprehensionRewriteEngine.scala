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

import eu.stratosphere.emma.macros.program.comprehension.ComprehensionModel
import eu.stratosphere.emma.rewrite.RewriteEngine

trait ComprehensionRewriteEngine extends ComprehensionModel with RewriteEngine {

  /**
   * Construct a rule application function that breaks after one successful application.
   *
   * @param rules A sequence of rules to be applied
   * @return A one-off rule application function
   */
  protected def applyAny(rules: Rule*): ExpressionRoot => Boolean =
    new ExpressionTransformer with (ExpressionRoot => Boolean) {
      
      var changed: Boolean     = false
      var root: ExpressionRoot = null
  
      override def transform(expr: Expression) = {
        if (!changed) applyFirst(rules: _*)(expr, root.expr) match {
          case Some(x) => changed = true; x
          case None    => super.transform(expr)
        } else expr
      }
  
      /**
       * Exhaustively apply the given set of `rules` to a tree and all its subtrees.
       *
       * @param root The root of the expression tree to be transformed
       * @return A [[Boolean]] indicating whether some rule was applied at all
       */
      def apply(root: ExpressionRoot) = {
        this.changed   = false
        this.root      = root

        root.expr = transform(root.expr)
        changed
      }
    }

  /**
   * Construct an exhaustive application function for a given set of transformation rules.
   *
   * @param rules A sequence of rules to be applied
   * @return An exhaustive rule application function
   */
  protected def applyExhaustively(rules: Rule*): ExpressionRoot => Boolean =
    new ExpressionTransformer with (ExpressionRoot => Boolean) {

      var changed: Boolean     = false
      var root: ExpressionRoot = null
  
      override def transform(expr: Expression) =
        applyFirst(rules: _*)(expr, root.expr) match {
          case Some(x) => changed = true; x
          case None    => super.transform(expr)
        }
  
      /**
       * Exhaustively apply the given set of `rules` to a tree and all its subtrees.
       *
       * @param root The root of the expression tree to be transformed
       * @return A [[Boolean]] indicating whether some rule was applied at all
       */
      def apply(root: ExpressionRoot) = {
        var i = -1
        this.root = root

        do {
          i += 1
          changed   = false
          root.expr = transform(root.expr)
        } while (changed)
  
        i > 0
      }
    }

  /**
   * Construct a break-on-success application function for a given sequence of transformation rules.
   *
   * @param rules A sequence of rules to be applied
   * @return A one-off rule application function
   */
  protected def applyFirst(rules: Rule*): (Expression, Expression) => Option[Expression] =
    (expr, root) => rules.foldLeft(Option.empty[Expression]) {
      case (Some(x), _) => Some(x)
      case (None, rule) => rule apply (expr, root)
    }
}
