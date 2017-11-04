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
package org.emmalanguage
package compiler

import ast.AST

import org.scalactic.Accumulation._
import org.scalactic._

/** Common IR tools. */
trait Common extends AST with API {

  import u._

  // --------------------------------------------------------------------------
  // Transformation pipelines API
  // --------------------------------------------------------------------------

  /** Standard pipeline prefix. Brings a tree into a form convenient for transformation. */
  def preProcess: TreeTransform

  /** Standard pipeline suffix. Brings a tree into a form acceptable for `scalac` after being transformed. */
  def postProcess: TreeTransform

  /** Combines a sequence of `transformations` into a pipeline with pre- and post-processing. */
  def pipeline(
    typeCheck: Boolean = false, withPre: Boolean = true, withPost: Boolean = true
  )(
    transformations: TreeTransform*
  ): TreeTransform

  /** The identity transformation with pre- and post-processing. */
  def identity(typeCheck: Boolean = false): u.Tree => u.Tree =
    pipeline(typeCheck)()

  /** Common validation helpers. */
  object Validation {

    val ok = ()
    val pass = Good(ok)

    type Valid = Unit
    type Invalid = Every[Error]
    type Verdict = Valid Or Invalid
    type Validator = Tree =?> Verdict

    def validateAs(expected: Validator, tree: Tree,
      violation: => String = "Unexpected tree"
    ): Verdict = expected.applyOrElse(tree, (unexpected: Tree) => {
      Bad(One(Error(unexpected, violation)))
    })

    def oneOf(allowed: Validator*): Validator =
      allowed.reduceLeft(_ orElse _)

    case class Error(at: Tree, violation: String) {
      override def toString = s"$violation:\n${api.Tree.show(at)}"
    }

    case class all(trees: Seq[Tree]) {
      case class are(expected: Validator) {
        def otherwise(violation: => String): Verdict =
          if (trees.isEmpty) pass
          else trees validatedBy expected.orElse {
            case unexpected => Bad(One(Error(unexpected, violation)))
          } map (_.head)
      }
    }

    object all {
      def apply(tree: Tree, trees: Tree*): all =
        apply(tree +: trees)
    }

    implicit class And(verdict: Verdict) {
      def and(other: Verdict): Verdict =
        withGood(verdict, other)((_, _) => ok)
    }

  }
}
