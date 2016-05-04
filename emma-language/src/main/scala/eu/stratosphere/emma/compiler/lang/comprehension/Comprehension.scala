package eu.stratosphere.emma.compiler.lang.comprehension

import eu.stratosphere.emma.compiler.Rewrite
import eu.stratosphere.emma.compiler.lang.core.Core

trait Comprehension extends Rewrite {
  self: Core =>

  import Term._
  import universe._
  import Tree._

  private[emma] object Comprehension {

    /**
     * Resugars monad ops into mock-comprehension syntax.
     *
     * == Preconditions ==
     *
     * - The input [[Tree]] is in ANF.
     *
     * == Postconditions ==
     *
     * - A [[Tree]] where monad operations are resugared into one-generator mock-comprehensions.
     *
     * @param monad The [[Symbol]] of the monad to be resugared.
     * @param tree  The [[Tree]] to be resugared.
     * @return The input [[Tree]] with resugared comprehensions.
     */
    def resugar(monad: Symbol)(tree: Tree): Tree = {
      // construct comprehension syntax helper for the given monad
      val cs = new Comprehension.Syntax(monad: Symbol)

      object fn {
        val meta = new Core.Meta(tree)

        def unapply(tree: Tree): Option[(ValDef, Tree)] = tree match {
          case q"(${arg: ValDef}) => ${body: Tree}" =>
            Some(arg, body)
          case Term.ref(sym) =>
            meta.valdef(sym) map {
              case ValDef(_, _, _, Function(arg :: Nil, body)) =>
                (arg, body)
            }
        }
      }

      val transform: Tree => Tree = preWalk {
        case cs.map(xs, fn(arg, body)) =>
          cs.comprehension(
            List(
              cs.generator(Term sym arg, asBlock(xs))),
            cs.head(asBlock(body)))

        case cs.flatMap(xs, fn(arg, body)) =>
          cs.flatten(
            block(
              cs.comprehension(
                List(
                  cs.generator(Term sym arg, asBlock(xs))),
                cs.head(asBlock(body)))))

        case cs.withFilter(xs, fn(arg, body)) =>
          cs.comprehension(
            List(
              cs.generator(Term sym arg, asBlock(xs)),
              cs.guard(asBlock(body))),
            cs.head(block(ref(Term sym arg))))
      }

      (transform andThen Core.dce) (tree)
    }

    /**
     * Desugars mock-comprehension syntax into monad ops.
     *
     * == Preconditions ==
     *
     * - An ANF [[Tree]] with mock-comprehensions.
     *
     * == Postconditions ==
     *
     * - A [[Tree]] where mock-comprehensions are desugared into comprehension operator calls.
     *
     * @param monad The [[Symbol]] of the monad syntax to be desugared.
     * @param tree  The [[Tree]] to be desugared.
     * @return The input [[Tree]] with desugared comprehensions.
     */
    def desugar(monad: Symbol)(tree: Tree): Tree = {
      // construct comprehension syntax helper for the given monad
      val cs = new Syntax(monad: Symbol)

      def hasNestedBlocks(tree: Block): Boolean = {
        val inStats = tree.stats exists {
          case ValDef(_, _, _, _: Block) => true
          case _ => false
        }
        val inExpr = tree.expr match {
          case _: Block => true
          case _ => false
        }
        inStats || inExpr
      }

      val transform: Tree => Tree = postWalk {

        case parent: Block if hasNestedBlocks(parent) =>
          // flatten (potentially) nested block stats
          val flatStats = parent.stats.flatMap {
            case val_(sym, Block(stats, expr), flags) =>
              stats :+ val_(sym, expr, flags)
            case stat =>
              stat :: Nil
          }
          // flatten (potentially) nested block expr
          val flatExpr = parent.expr match {
            case Block(stats, expr) =>
              stats :+ expr
            case expr =>
              expr :: Nil
          }

          block(flatStats ++ flatExpr)

        case t@cs.comprehension(cs.generator(sym, block(_, rhs)) :: qs, cs.head(expr)) =>

          val (guards, rest) = qs span {
            case cs.guard(_) => true
            case _ => false
          }

          // accumulate filters in a prefix of valdefs and keep track of the tail valdef symbol
          val (tail, prefix) = {
            ({
              // step (1) accumulate the result
              (_: List[Tree]).foldLeft((Term sym rhs, List.empty[Tree]))((res, guard) => {
                val (currSym, currPfx) = res
                val cs.guard(expr) = guard

                val funcRhs = lambda(sym)(expr)
                val funcNme = Term.name.fresh("anonfun")
                val funcSym = Term.sym.free(funcNme, funcRhs.tpe)
                val funcVal = val_(funcSym, funcRhs)

                val nextNme = Term.name.fresh(cs.withFilter.symbol.name)
                val nextSym = Term.sym.free(nextNme, currSym.info)
                val nextVal = val_(nextSym, cs.withFilter(ref(currSym))(ref(funcSym)))

                (nextSym, nextVal :: funcVal :: currPfx)
              })
            } andThen {
              // step (2) reverse the prefix
              case (currSym, currPfx) => (currSym, currPfx.reverse)
            }) (guards)
          }

          expr match {
            case block(Nil, id: Ident) if id.symbol == sym =>
              // trivial head expression consisting of the matched sym 'x'
              // omit the resulting trivial mapper

              Core.simplify(block(
                prefix,
                ref(tail)))

            case _ =>
              // append a map or a flatMap to the result depending on
              // the size of the residual qualifier sequence 'qs'

              val (op: MonadOp, body: Tree) = rest match {
                case Nil => (
                  cs.map,
                  expr)
                case _ => (
                  cs.flatMap,
                  cs.comprehension(
                    rest,
                    cs.head(expr)))
              }

              val func = lambda(sym)(body)
              val term = Term.sym.free(Term.name.lambda, func.tpe)

              block(
                prefix,
                val_(term, func),
                op(ref(tail))(ref(term)))
          }

        case t@cs.flatten(block(stats, expr@cs.map(xs, fn))) =>

          block(
            stats,
            cs.flatMap(xs)(fn))
      }

      transform(tree)
    }

    /**
     * Normalizes nested mock-comprehension syntax.
     *
     * @param monad The [[Symbol]] of the monad syntax to be normalized.
     * @param tree  The [[Tree]] to be resugared.
     * @return The input [[Tree]] with resugared comprehensions.
     */
    def normalize(monad: Symbol)(tree: Tree): Tree = {
      // construct comprehension syntax helper for the given monad
      val cs = new Syntax(monad: Symbol)

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

    // -------------------------------------------------------------------------
    // General helpers
    // -------------------------------------------------------------------------

    def asBlock(tree: Tree): Block = tree match {
      case block: Block => block
      case other => block(other)
    }

    // -------------------------------------------------------------------------
    // Helpers for mock comprehension syntax construction / destruction
    // -------------------------------------------------------------------------

    trait MonadOp {
      val symbol: TermSymbol

      def apply(xs: Tree)(fn: Tree): Tree

      def unapply(tree: Tree): Option[(Tree, Tree)]
    }

    class Syntax(val monad: Symbol) {

      val monadTpe /*  */ = monad.asType.toType.typeConstructor
      val moduleSel /* */ = resolve(IR.module)

      // -----------------------------------------------------------------------
      // Monad Ops
      // -----------------------------------------------------------------------

      object map extends MonadOp {

        override val symbol =
          Term.member(monad, Term name "map")

        override def apply(xs: Tree)(f: Tree): Tree =
          Method.call(xs, symbol, Type.arg(2, f))(f :: Nil)

        override def unapply(apply: Tree): Option[(Tree, Tree)] = apply match {
          case Method.call(xs, `symbol`, _, Seq(f)) => Some(xs, f)
          case _ => None
        }
      }

      object flatMap extends MonadOp {

        override val symbol =
          Term.member(monad, Term name "flatMap")

        override def apply(xs: Tree)(f: Tree): Tree =
          Method.call(xs, symbol, Type.arg(1, Type.arg(2, f)))(f :: Nil)

        override def unapply(tree: Tree): Option[(Tree, Tree)] = tree match {
          case Method.call(xs, `symbol`, _, Seq(f)) => Some(xs, f)
          case _ => None
        }
      }

      object withFilter extends MonadOp {

        override val symbol =
          Term.member(monad, Term name "withFilter")

        override def apply(xs: Tree)(p: Tree): Tree =
          Method.call(xs, symbol)(p :: Nil)

        override def unapply(tree: Tree): Option[(Tree, Tree)] = tree match {
          case Method.call(xs, `symbol`, _, Seq(p)) => Some(xs, p)
          case _ => None
        }
      }

      // -----------------------------------------------------------------------
      // Mock Comprehension Ops
      // -----------------------------------------------------------------------

      /** Con- and destructs a comprehension from/to a list of qualifiers `qs` and a head expression `hd`. */
      object comprehension {
        val symbol = IR.comprehension

        def apply(qs: List[Tree], hd: Tree): Tree =
          Method.call(moduleSel, symbol, Type of hd, monadTpe)(block(qs, hd) :: Nil)

        def unapply(tree: Tree): Option[(List[Tree], Tree)] = tree match {
          case Method.call(_, `symbol`, _, block(qs, hd) :: Nil) =>
            Some(qs, hd)
          case _ =>
            None
        }
      }

      /** Con- and destructs a generator from/to a [[Tree]]. */
      object generator {
        val symbol = IR.generator

        def apply(lhs: TermSymbol, rhs: Block): Tree =
          val_(lhs, Method.call(moduleSel, symbol, Type.arg(1, rhs), monadTpe)(rhs :: Nil))

        def unapply(tree: ValDef): Option[(TermSymbol, Block)] = tree match {
          case val_(lhs, Method.call(_, `symbol`, _, (arg: Block) :: Nil), _) =>
            Some(lhs, arg)
          case _ =>
            None
        }
      }

      /** Con- and destructs a guard from/to a [[Tree]]. */
      object guard {
        val symbol = IR.guard

        def apply(expr: Block): Tree =
          Method.call(moduleSel, symbol)(expr :: Nil)

        def unapply(tree: Tree): Option[Block] = tree match {
          case Method.call(_, `symbol`, _, (expr: Block) :: Nil) =>
            Some(expr)
          case _ =>
            None
        }
      }

      /** Con- and destructs a head from/to a [[Tree]]. */
      object head {
        val symbol = IR.head

        def apply(expr: Block): Tree =
          Method.call(moduleSel, symbol, Type of expr)(expr :: Nil)

        def unapply(tree: Tree): Option[Block] = tree match {
          case Method.call(_, `symbol`, _, (expr: Block) :: Nil) =>
            Some(expr)
          case _ =>
            None
        }
      }

      /** Con- and destructs a flatten from/to a [[Tree]]. */
      object flatten {
        val symbol = IR.flatten

        def apply(expr: Block): Tree =
          Method.call(moduleSel, symbol, Type.arg(1, Type.arg(1, expr)), monadTpe)(expr :: Nil)

        def unapply(tree: Tree): Option[Block] = tree match {
          case Apply(fun, (expr: Block) :: Nil)
            if Term.sym(fun) == symbol => Some(expr)
          case _ =>
            None
        }
      }

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
                      Syntax.this.generator(sym, prepend(rm.bs3i, rhs))
                    case guard(expr) =>
                      Syntax.this.guard(prepend(rm.bs3i, expr))
                  },
                  head(prepend(rm.bs3i, rm.hd))))
            case flatten(block(_, comprehension(_, _))) =>
              comprehension(
                rm.qs1 ++ rm.qs2 collect {
                  case generator(sym, rhs) =>
                    Syntax.this.generator(sym, prepend(rm.bs3i, rhs))
                  case guard(expr) =>
                    Syntax.this.guard(prepend(rm.bs3i, expr))
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

}
