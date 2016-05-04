package eu.stratosphere.emma.compiler.lang.comprehension

import eu.stratosphere.emma.compiler.lang.core.Core
import eu.stratosphere.emma.compiler.{Common, Rewrite}

private[comprehension] trait Normalize extends Common
  with Rewrite {
  self: Core with Comprehension =>

  import universe._
  import Comprehension.{Syntax, asBlock}
  import Tree._

  private[comprehension] object Normalize {

    /**
     * Normalizes nested mock-comprehension syntax.
     *
     * @param monad The [[Symbol]] of the monad syntax to be normalized.
     * @param tree  The [[Tree]] to be resugared.
     * @return The input [[Tree]] with resugared comprehensions.
     */
    def normalize(monad: Symbol)(tree: Tree): Tree = {
      // construct comprehension syntax helper for the given monad
      val cs = new Syntax(monad: Symbol) with NormalizationRules

      ({
        // apply UnnestHead and UnnestGenerator rules exhaustively
        Engine.postWalk(List(cs.UnnestHead, cs.UnnestGenerator))
      } andThen {
        // elminiate dead code produced by normalization
        Core.dce
      } andThen {
        // elminiate trivial guards produced by normalization
        postWalk {
          case cs.comprehension(qs, hd) =>
            cs.comprehension(
              qs filterNot {
                case t@cs.guard(block(_, Literal(Constant(true)))) => true
                case t => false
              }, hd)
        }
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
     * {{{
     * {
     *   $bs1
     *   val c = flatten {
     *     $bs2
     *     comprehension {
     *       $qs1
     *       head {
     *         $bs3
     *         comprehension {
     *           $qs2
     *           head $hd
     *         } // child comprehension
     *       }
     *     } // outer comprehension
     *   } // parent expression
     *   $bs4
     * } // enclosing block
     * }}}
     *
     * ==Guard==
     * None.
     *
     * ==Rewrite==
     *
     * Let $bs3 decompose into the following two subsets:
     * - $bs3i (transitively) depends on symbols defined in $qs1, and
     * - $bs3o is the independent complement $bs3 \ $bs3o.
     *
     * {{{
     * {
     *   $bs1
     *   $bs2
     *   $bs3o
     *   val c = comprehension {
     *     $qs1
     *     $qs2' // where let blocks are prefixed with $bs3i
     *     head $hd' // where let blocks are prefixed with $bs3i
     *   } // flattened result comprehension
     *   $bs4
     * } // enclosing block
     * }}}
     */
    object UnnestHead extends Rule {

      case class RuleMatch(enclosing: Block, parent: Tree, child: Block) {
        lazy val bs1 = enclosing.children.takeWhile(_ != parent)
        lazy val bs2 = parent match {
          case val_(_, flatten(block(bs, comprehension(_, _))), _) => bs
          case flatten(block(bs, comprehension(_, _))) => bs
        }
        lazy val bs3 = child.stats
        lazy val bs4 = enclosing.children.reverse.takeWhile(_ != parent).reverse
        lazy val qs1 = parent match {
          case val_(_, flatten(block(Nil, comprehension(qs, _))), _) => qs
          case flatten(block(Nil, comprehension(qs, _))) => qs
        }
        lazy val (qs2, hd) = child.expr match {
          case comprehension(_qs2, head(_hd)) => (_qs2, _hd)
        }
        lazy val (bs3i, bs3o) = {
          val surrogateSym = (t: Tree) =>
            Term.sym.free(Term.name.fresh("x"), t.tpe)

          val bs3refs = (bs3 map {
            case val_(sym, rhs, _) => sym -> Tree.refs(rhs)
            case stmt => surrogateSym(stmt) -> Tree.refs(stmt)
          }).toMap

          val qs1defs = (qs1 flatMap {
            case generator(sym, _) => List(sym)
            case guard(_) => List.empty[TermSymbol]
          }).toSet

          var bs3iSym = for {
            sym <- bs3refs.keySet
            if (bs3refs(sym) intersect qs1defs).nonEmpty
          } yield sym

          var delta = Set.empty[TermSymbol]
          do {
            bs3iSym = bs3iSym union delta
            delta = (for {
              sym <- bs3refs.keySet
              if (bs3refs(sym) intersect bs3iSym).nonEmpty
            } yield sym) diff bs3iSym
          } while (delta.nonEmpty)

          val bs3oSym = bs3refs.keySet diff bs3iSym

          val bs3i = bs3 flatMap {
            case vd@val_(sym, _, _) if bs3iSym contains sym => List(vd)
            case _ => Nil
          }
          val bs3o = bs3 flatMap {
            case vd@val_(sym, _, _) if bs3oSym contains sym => List(vd)
            case _ => Nil
          }

          (bs3i, bs3o)
        }
      }

      override def bind(root: Tree): Traversable[RuleMatch] = root match {
        case enclosing: Block => new Traversable[RuleMatch] {
          override def foreach[U](f: (RuleMatch) => U): Unit = {
            val parents = (enclosing.stats collect {
              // a ValDef statement with a `flatten(comprehension(...))` rhs
              case parent@val_(_, flatten(block(_, outer@comprehension(_, _))), _) => (parent, outer)
            }) ++ (enclosing.expr match {
              // an expr with a `flatten(comprehension(...))`
              case parent@flatten(block(_, outer@comprehension(_, _))) => List((parent, outer))
              case _ => Nil
            })

            for ((parent, outer) <- parents) outer match {
              case comprehension(_, head(child@Block(_, comprehension(_, _)))) =>
                f(RuleMatch(enclosing, parent, child))
            }
          }
        }
        case _ => Traversable.empty[RuleMatch]
      }

      override def guard(rm: RuleMatch): Boolean =
        true

      override def fire(rm: RuleMatch): Tree = {
        // construct a flattened version of the parent comprehension
        val flattened = rm.parent match {
          case val_(vsym, flatten(block(_, comprehension(_, _))), _) =>
            val_(vsym,
              comprehension(
                rm.qs1 ++ rm.qs2 collect {
                  case generator(sym, rhs) =>
                    NormalizationRules.this.generator(sym, prepend(rm.bs3i, rhs))
                  case guard(expr) =>
                    NormalizationRules.this.guard(prepend(rm.bs3i, expr))
                },
                head(prepend(rm.bs3i, rm.hd))))
          case flatten(block(_, comprehension(_, _))) =>
            comprehension(
              rm.qs1 ++ rm.qs2 collect {
                case generator(sym, rhs) =>
                  NormalizationRules.this.generator(sym, prepend(rm.bs3i, rhs))
                case guard(expr) =>
                  NormalizationRules.this.guard(prepend(rm.bs3i, expr))
              },
              head(prepend(rm.bs3i, rm.hd)))
        }

        // construct and return a new enclosing block
        block(rm.bs1 ++ rm.bs2 ++ rm.bs3o ++ List(flattened) ++ rm.bs4)
      }

      private def prepend(prefix: List[ValDef], blck: Block): Block =
        Tree.refresh(block(prefix, blck.children: _*), prefix collect {
          case val_(sym, _, _) => sym
        }: _*).asInstanceOf[Block]
    }

    /**
     * Un-nests a comprehended generator in its parent.
     *
     * ==Matching Pattern==
     * {{{
     * {
     *   $bs1
     *   val y = comprehension {
     *     $qs2
     *     head $hd2
     *   } // child comprehension
     *   $bs2
     *   comprehension {
     *     $qs1
     *     val x = generator(y) // gen
     *     $qs3
     *     head $hd1
     *   } // parent comprehension
     * } // enclosing block
     * }}}
     *
     * ==Guard==
     * None.
     *
     * ==Rewrite==
     * {{{
     * {
     *   $bs1
     *   $bs2
     *   comprehension {
     *     $qs1
     *     $qs2
     *     $qs3 [ $hd2 \ x ]
     *     head $hd1 [ $hd2 \ x ]
     *   } // unnested result comprehension
     * } // enclosing block
     * }}}
     */
    object UnnestGenerator extends Rule {

      case class RuleMatch(enclosing: Block, parent: Tree, gen: ValDef, child: ValDef) {
        lazy val bs1 = enclosing.stats.takeWhile(_ != child)
        lazy val bs2 = enclosing.stats.reverse.takeWhile(_ != child).reverse
        lazy val qs1 = parent match {
          case comprehension(qs, _) => qs.takeWhile(_ != gen)
        }
        lazy val qs2 = child match {
          case val_(_, comprehension(qs, _), _) => qs
        }
        lazy val qs3 = parent match {
          case comprehension(qs, _) => qs.reverse.takeWhile(_ != gen).reverse
        }
        lazy val hd1 = parent match {
          case comprehension(_, head(hd)) => hd match {
            case block(Nil, expr) => expr
            case other => other
          }
        }
        lazy val hd2 = child match {
          case val_(_, comprehension(_, head(hd)), _) => hd match {
            case block(Nil, expr) => expr
            case other => other
          }
        }
      }

      def bind(root: Tree): Traversable[RuleMatch] = root match {
        case enclosing: Block => new Traversable[RuleMatch] {
          override def foreach[U](f: (RuleMatch) => U): Unit = {
            // compute a lookup table for comprehensions
            // defined in the current enclosing bock
            val lookup = (enclosing.stats collect {
              case vd@val_(sym, rhs@comprehension(_, _), _) => sym -> vd
            }).toMap

            if (lookup.nonEmpty) enclosing.expr match {
              case parent@comprehension(qs, hd) => qs collect {
                case gen@generator(sym, block(_, rhs: Ident)) =>
                  for (child <- lookup.get(Term sym rhs))
                    f(RuleMatch(enclosing, parent, gen, child))
              }
              case _ => Unit
            }
          }
        }
        case _ => Traversable.empty[RuleMatch]
      }

      def guard(rm: RuleMatch): Boolean =
        true

      def fire(rm: RuleMatch): Tree = {
        // define a substitution function `· [ $hd2 \ x ]`
        val subst: Tree => Tree = Tree.subst(_, Map(rm.gen.symbol -> rm.hd2))

        // construct an unnested version of the parent comprehension
        val unnested = comprehension(
          rm.qs1 ++ rm.qs2 ++ (rm.qs3 map subst),
          head(asBlock(subst(rm.hd1)))
        )

        // construct and return a new enclosing block
        block(rm.bs1 ++ rm.bs2 ++ List(unnested))
      }
    }

  }

}
