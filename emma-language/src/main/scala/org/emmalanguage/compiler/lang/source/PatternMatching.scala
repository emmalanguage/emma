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
package compiler.lang.source

import compiler.Common

import shapeless._

/** Pattern matching destructuring for the Core language. */
private[source] trait PatternMatching extends Common {
  self: Source =>

  import Source.{Lang => src}
  import UniverseImplicits._

  private[source] object PatternMatching {

    /**
     * Eliminates irrefutable pattern matches by replacing them with value definitions corresponding
     * to bindings and field accesses corresponding to case class extractors.
     *
     * == Assumptions ==
     * - The selector of the pattern match is non-null;
     * - The first pattern match case is irrefutable;
     * - No guards are used;
     *
     * == Example ==
     * {{{
     *   ("life", 42) match {
     *     case (s: String, i: Int) =>
     *       s + i
     *   } \\ <=>
     *   {
     *     val x$1 = ("life", 42)
     *     val s = x$1._1: String
     *     val i = x$1._2: Int
     *     val x$2 = s + i
     *     x$2
     *   }
     * }}}
     */
    lazy val destruct = TreeTransform("PatternMatching.destruct",
      api.BottomUp.withOwner.transformWith {
        case Attr.inh(
          mat @ src.PatMat(target, Seq(src.PatCase(pat, src.Empty(_), body), _*)),
          owner :: _) =>

          // remove potential type ascriptions from the target
          val (unascr, tpe) = target match {
            // of the form `(expr: T@unchecked)`, if the type of `expr` is `T`
            case src.TypeAscr(expr, u.AnnotatedType(Seq(_), underlying))
              if expr.tpe =:= underlying => (expr, expr.tpe)
            case other => (other, other.tpe)
          }

          unascr match {
            case src.Ref(lhs) =>
              val vals = irrefutable(src.Ref(lhs), pat)
              assert(vals.isDefined, s"Unsupported refutable pattern matching:\n${api.Tree.show(mat)}")
              src.Block(vals.get, body)
            case _ =>
              val nme = api.TermName.fresh("x")
              val lhs = api.ValSym(owner, nme, tpe)
              val vals = irrefutable(src.Ref(lhs), pat)
              assert(vals.isDefined, s"Unsupported refutable pattern matching:\n${api.Tree.show(mat)}")
              src.Block(src.ValDef(lhs, unascr) +: vals.get, body)
          }
      }.andThen(_.tree))

    /**
     * Tests if `pattern` is irrefutable for the given selector, i.e. if it always matches. If it
     * is, returns a sequence of value definitions equivalent to the bindings in `pattern`.
     * Otherwise returns [[scala.None]].
     *
     * A pattern `p` is irrefutable for type `T` when:
     * - `p` is the wildcard pattern (_);
     * - `p` is a variable pattern;
     * - `p` is a typed pattern `x: U` and `T` is a subtype of `U`;
     * - `p` is an alternative pattern `p1 | ... | pn` and at least one `pi` is irrefutable;
     * - `p` is a case class pattern `c(p1, ..., pn)`, `T` is an instance of `c`, the primary
     * constructor of `c` has argument types `T1, ..., Tn` and each `pi` is irrefutable for `Ti`.
     *
     * Caution: Does not consider `null` refutable in contrast to the standard Scala compiler. This
     * might cause [[java.lang.NullPointerException]]s.
     */
    private def irrefutable(target: u.Tree, pattern: u.Tree): Option[Seq[u.ValDef]] =
      pattern match {
        // Alternative patterns don't allow binding
        case api.PatAlt(alternatives) =>
          alternatives.flatMap(irrefutable(target, _)).headOption

        case api.PatAny(_) =>
          Some(Seq.empty)

        case api.PatAscr(pat, tpe) if target.tpe <:< tpe =>
          irrefutable(src.TypeAscr(target, tpe), pat)

        case api.PatAt(lhs, pat) =>
          lazy val value = src.ValDef(lhs, target)
          irrefutable(src.ValRef(lhs), pat).map(value +: _)

        case extr @ api.PatExtr(_, args)
          if is.caseClass(extr.tpe) && extr.tpe.typeSymbol == target.tpe.typeSymbol =>
            val init = extr.tpe.typeSymbol.asClass.primaryConstructor
            val getters = for {
              param <- init.infoIn(target.tpe).paramLists.head
              api.DefSym(accessor) <- target.tpe.member(param.name).alternatives
              if accessor.isGetter
            } yield src.DefCall(Some(target), accessor)
            val patterns = getters zip args map (irrefutable _).tupled
            if (patterns.exists(_.isEmpty)) None
            else Some(patterns.flatMap(_.get))

        case _ =>
          None
      }
  }
}
