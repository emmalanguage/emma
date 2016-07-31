package eu.stratosphere.emma
package compiler.lang.comprehension

import compiler.lang.core.Core
import compiler.{Common, Rewrite}

private[comprehension] trait Normalize extends Common
  with Rewrite {
  self: Core with Comprehension =>

  import UniverseImplicits._
  import Comprehension.{Syntax, asLet}
  import Core.{Lang => core}

  private[comprehension] object Normalize {

    /**
     * Normalizes nested mock-comprehension syntax.
     *
     * @param monad The [[Symbol]] of the monad syntax to be normalized.
     * @param tree  The [[Tree]] to be resugared.
     * @return The input [[Tree]] with resugared comprehensions.
     */
    def normalize(monad: u.Symbol)(tree: u.Tree): u.Tree = {
      // construct comprehension syntax helper for the given monad
      val cs = new Syntax(monad: u.Symbol) with NormalizationRules

      ({
        // apply UnnestHead and UnnestGenerator rules exhaustively
        Engine.bottomUp(List(cs.UnnestHead, cs.UnnestGenerator))
      } andThen {
        // elminiate dead code produced by normalization
        Core.dce
      } andThen {
        // elminiate trivial guards produced by normalization
        tree => api.BottomUp.transform {
          case cs.Comprehension(qs, hd) =>
            cs.Comprehension(
              qs filterNot {
                case t@cs.Guard(core.Let(_, _, core.Lit(true))) => true
                case t => false
              }, hd)
        }(tree).tree
      }) (tree)
    }
  }

  protected trait NormalizationRules {
    self: Syntax =>

    // -------------------------------------------------------------------------
    // Comprehension normalization rules
    // -------------------------------------------------------------------------

    /**
     * Unnests a comprehended head in its parent.
     *
     * ==Matching Pattern==
     *
     * (1) A `flatten` expression occurring in the `expr` position in the enclosing `let` block.
     *
     * {{{
     * {
     *   $vals1
     *   flatten {
     *     $vals2
     *     comprehension {
     *       $qs1
     *       head {
     *         $vals3
     *         comprehension {
     *           $qs2
     *           head $hd2
     *         } // child comprehension
     *       }
     *     } // outer comprehension
     *   } // parent expression
     * } // enclosing block
     * }}}
     *
     * (2) A `flatten` expression occurring in the `rhs` opsition of some binding in the enclosing `let` block.
     *
     * {{{
     * {
     *   $vals1a
     *   val x = flatten {
     *     $vals2
     *     comprehension {
     *       $qs1
     *       head {
     *         $vals3
     *         comprehension {
     *           $qs2
     *           head $hd2
     *         } // child comprehension
     *       }
     *     } // outer comprehension
     *   } // parent valdef
     *   $vals1b
     *   $expr1 // parent expression
     * } // enclosing block
     * }}}
     *
     * ==Rewrite==
     *
     * Let $vals3 decompose into the following two subsets:
     * - $vals3i (transitively) depends on symbols defined in $qs1, and
     * - $vals3o is the independent complement $vals3 \ $vals3o.
     *
     * For a match of type (1):
     *
     * {{{
     * {
     *   $vals1
     *   $vals2
     *   $vals3o
     *   comprehension {
     *     $qs1
     *     $qs2' // where let blocks are prefixed with $vals3i
     *     head $hd' // where let blocks are prefixed with $vals3i
     *   } // flattened result comprehension
     * } // enclosing block
     * }}}
     *
     * For a match of type (2):
     *
     * {{{
     * {
     *   $vals1a
     *   $vals2
     *   $vals3o
     *   val x = comprehension {
     *     $qs1
     *     $qs2' // where let blocks are prefixed with $vals3i
     *     head $hd' // where let blocks are prefixed with $vals3i
     *   } // flattened result comprehension
     *   $vals1b
     *   $expr1
     * } // enclosing block
     * }}}
     */
    object UnnestHead extends Rule {

      override def apply(root: u.Tree): Option[u.Tree] = root match {
        //@formatter:off
        case core.Let(
          vals1,
          defs1,
          expr1) =>
          //@formatter:on

          vals1.map(x => x -> x.rhs) :+ (expr1 -> expr1) collectFirst {
            //@formatter:off
            case (encl, Flatten(
              core.Let(
                vals2,
                Nil,
                expr2@Comprehension(
                  qs1,
                  Head(
                    core.Let(
                      vals3,
                      Nil,
                      expr3@Comprehension(
                        qs2,
                        Head(hd2)
                      ))))))) =>
              //@formatter:on

              val (vals3i, vals3o) = split(vals3, qs1)

              val qs2p = qs2 map {
                case Generator(sym, rhs) =>
                  Generator(sym, prepend(vals3i, rhs))
                case Guard(pred) =>
                  Guard(prepend(vals3i, pred))
              }

              val hd2p = prepend(vals3i, hd2)

              val (vals, expr) = encl match {
                case encl@core.ValDef(xsym, xrhs, xflags) =>
                  val (vals1a, vals1b) = splitAt(encl)(vals1)
                  val val_ = core.ValDef(xsym, Comprehension(qs1 ++ qs2p, Head(hd2p)), xflags)
                  (vals1a ++ vals2 ++ vals3o ++ Seq(val_) ++ vals1b, expr1)
                case _ =>
                  (vals1 ++ vals2 ++ vals3o, Comprehension(qs1 ++ qs2p, Head(hd2p)))
              }

              // API: cumbersome syntax of Let.apply
              core.Let(vals: _*)(defs1: _*)(expr)
          }

        case _ =>
          // subtree's root does not match, don't modify the subtree
          None
      }

      /** Splits `vals` in two subsequences: vals dependent on generators bound in `qs`, and complement. */
      private def split(vals: Seq[u.ValDef], qs: Seq[u.Tree]): (Seq[u.ValDef], Seq[u.ValDef]) = {
        // symbols referenced in vals
        val vals3refs = (for {
          core.ValDef(sym, rhs, _) <- vals
        } yield sym -> api.Tree.refs(rhs)).toMap

        // symbols defined in qs
        val qs1Syms = (for {
          Generator(sym, _) <- qs
        } yield sym).toSet

        // symbols defined in vals3 which directly depend on symbols from qs1
        var vasDepSyms = (for {
          (sym, refs) <- vals3refs
          if (refs intersect qs1Syms).nonEmpty
        } yield sym).toSet

        // compute the transitive closure of vals3iSyms, i.e. extend with indirect dependencies
        var delta = Set.empty[u.TermSymbol]
        do {
          vasDepSyms = vasDepSyms union delta
          delta = (for {
            (sym, refs) <- vals3refs
            if (refs intersect vasDepSyms).nonEmpty
          } yield sym).toSet diff vasDepSyms
        } while (delta.nonEmpty)

        // symbols defined in vals3 which do not depend on symbols from qs1
        val valsIndSyms = vals3refs.keySet diff vasDepSyms

        val vasDep = for {
          vd@core.ValDef(sym, rhs, _) <- vals
          if vasDepSyms contains sym
        } yield vd

        val valsInd = for {
          vd@core.ValDef(sym, rhs, _) <- vals
          if valsIndSyms contains sym
        } yield vd

        (vasDep, valsInd)
      }

      private def prepend(prefix: Seq[u.ValDef], blck: u.Block): u.Block = blck match {
        case core.Let(vals, defs, expr) =>
          val fresh = for (core.ValDef(sym, rhs, flags) <- prefix) yield {
            core.ValDef(api.TermSym.fresh(sym), rhs, flags)
          }

          val aliases = for {
            (core.ValDef(from, _, _), core.ValDef(to, _, _)) <- prefix zip fresh
          } yield from -> to

          (api.Tree.rename(aliases: _*) andThen { Core.simplify _ })(
            core.Let(fresh ++ vals: _*)(defs: _*)(expr)
          ).asInstanceOf[u.Block]
      }
    }

    /**
     * Un-nests a comprehended generator in its parent.
     *
     * ==Matching Pattern==
     *
     * A `parent` comprehension with a generator that binds to `x` and references a `child` comprehension
     * that occurs in one of the previous value bindings within the enclosing `let` block.
     *
     * (1) The `parent` comprehension is an `expr` position in the enclosing `let` block.
     *
     * {{{
     * {
     *   $vals1a
     *   val y = comprehension {
     *     $qs2
     *     head $hd2
     *   } // child comprehension
     *   $vals1b
     *   comprehension {
     *     $qs1a
     *     val x = generator(y) // gen
     *     $qs1b
     *     head $hd1
     *   } // parent comprehension
     * } // enclosing block
     * }}}
     *
     * (2) The `parent` comprehension is an `rhs` position for some value definitions in the enclosing `let` block.
     *
     * {{{
     * {
     *   $vals1a
     *   val y = comprehension {
     *     $qs2
     *     head $hd2
     *   } // child comprehension
     *   $vals1b
     *   val z = comprehension {
     *     $qs1a
     *     val x = generator(y) // gen
     *     $qs1b
     *     head $hd1
     *   } // parent comprehension
     *   $vals1c
     *   $expr1
     * } // enclosing block
     * }}}
     *
     * ==Rewrite==
     *
     * For a match of type (1):
     *
     * {{{
     * {
     *   $vals1a
     *   $vals1b
     *   comprehension {
     *     $qs1a
     *     $qs2
     *     $qs1b [ $hd2 \ x ]
     *     head $hd1 [ $hd2 \ x ]
     *   } // unnested result comprehension
     * } // enclosing block
     * }}}
     *
     * For a match of type (2):
     *
     * {{{
     * {
     *   $vals1a
     *   $vals1b
     *   val z = comprehension {
     *     $qs1a
     *     $qs2
     *     $qs1b [ $hd2 \ x ]
     *     head $hd1 [ $hd2 \ x ]
     *   } // unnested result comprehension
     *   $vals1c
     *   $expr1
     * } // enclosing block
     * }}}
     */
    object UnnestGenerator extends Rule {

      override def apply(root: u.Tree): Option[u.Tree] = root match {
        //@formatter:off
        case core.Let(
          vals1,
          defs1,
          expr1) =>
          //@formatter:on

          (vals1 collectFirst {
            case vd@core.ValDef(y, Comprehension(qs2, Head(hd2)), _) =>
              val (vals1a, vals1r) = splitAt(vd)(vals1)
              val encls = vals1r.map(x => x -> x.rhs) :+ (expr1 -> expr1)
              encls collectFirst {
                case (encl, Comprehension(qs1, Head(hd1))) =>
                  qs1 collectFirst {
                    case gen@Generator(x, core.Let(Nil, Nil, core.ValRef(`y`))) =>

                      // define a substitution function `· [ $hd2 \ x ]`
                      val subst = hd2 match {
                        case core.Let(Nil, Nil, expr2) => api.Tree.subst(x -> expr2)
                        case _ => api.Tree.subst(x -> hd2)
                      }

                      // compute prefix and suffix for qs1 and vals1
                      val (qs1a, qs1b) = splitAt[u.Tree](gen)(qs1)

                      val comp = Comprehension(
                        qs1a ++ qs2 ++ (qs1b map subst),
                        Head(asLet(subst(hd1))))

                      val (vals, expr) = encl match {
                        case encl@core.ValDef(zsym, zrhs, zflags) =>
                          val (vals1b, vals1c) = splitAt(encl)(vals1r)
                          val val_ = core.ValDef(zsym, comp, zflags)
                          (vals1a ++ vals1b ++ Seq(val_) ++ vals1c, expr1)
                        case _ =>
                          (vals1a ++ vals1r, comp)
                      }

                      // API: cumbersome syntax of Let.apply
                      core.Let(vals: _*)(defs1: _*)(expr)
                  }
              }
          }).flatten.flatten

        case _ =>
          None
      }
    }

    /* Splits a `Seq[A]` into a prefix and suffix. */
    private def splitAt[A](e: A): Seq[A] => (Seq[A], Seq[A]) = {
      (_: Seq[A]).span(_ != e)
    } andThen {
      case (pre, _ :: suf) => (pre, suf)
    }
  }

}
