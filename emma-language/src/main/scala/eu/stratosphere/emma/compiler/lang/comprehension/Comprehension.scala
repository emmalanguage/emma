package eu.stratosphere.emma.compiler.lang.comprehension

import eu.stratosphere.emma.compiler.Common
import eu.stratosphere.emma.compiler.lang.core.Core

trait Comprehension extends Common
  with ReDeSugar
  with Normalize {
  self: Core =>

  import universe._
  import Tree._

  private[emma] object Comprehension {

    // -------------------------------------------------------------------------
    // Mock comprehension syntax language
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

    }

    // -------------------------------------------------------------------------
    // ReDeSugar API
    // -------------------------------------------------------------------------

    /** Delegates to [[ReDeSugar.resugar()]]. */
    def resugar(monad: Symbol)(tree: Tree): Tree =
      ReDeSugar.resugar(monad)(tree)

    /** Delegates to [[ReDeSugar.desugar()]]. */
    def desugar(monad: Symbol)(tree: Tree): Tree =
      ReDeSugar.desugar(monad)(tree)

    // -------------------------------------------------------------------------
    // Normalize API
    // -------------------------------------------------------------------------

    /** Delegates to [[Normalize.normalize()]]. */
    def normalize(monad: Symbol)(tree: Tree): Tree =
      Normalize.normalize(monad)(tree)

    // -------------------------------------------------------------------------
    // General helpers
    // -------------------------------------------------------------------------

    def asBlock(tree: Tree): Block = tree match {
      case block: Block => block
      case other => block(other)
    }
  }

}
