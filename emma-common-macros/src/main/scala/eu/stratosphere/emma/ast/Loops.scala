package eu.stratosphere
package emma.ast

/** Loops (`while` and `do-while`). */
trait Loops { this: AST =>

  /** Loops (`while` and `do-while`). */
  trait LoopAPI { this: API =>

    import universe._

    /** Converts `block` to a statement (having `Unit` as an expression). */
    private def asStat(block: u.Block) = block match {
      case u.Block(_, Lit(())) => block
      case u.Block(stats, stat) => Block(stats :+ stat: _*)()
    }

    /** Extractor for loops (`while` and `do-while`). */
    object Loop extends Node {
      def unapply(loop: u.LabelDef): Option[(u.Tree, u.Block)] = loop match {
        case While(cond, body) => Some(cond, body)
        case DoWhile(cond, body) => Some(cond, body)
        case _ => None
      }
    }

    /** `while` loops. */
    object While extends Node {

      /**
       * Creates a type-checked `while` loop.
       * @param cond The loop condition (must be a boolean term).
       * @param body The loop body block.
       * @return `while (cond) { body }`.
       */
      def apply(cond: u.Tree, body: u.Block): u.LabelDef = {
        assert(is.defined(cond), s"$this condition is not defined: $cond")
        assert(is.defined(body), s"$this body is not defined: $body")
        assert(has.tpe(cond), s"$this condition has no type:\n${Tree.showTypes(cond)}")
        lazy val Cond = Type.of(cond)
        assert(Cond =:= Type.bool, s"$this condition is not boolean:\n${Tree.showTypes(cond)}")

        val name = TermName.While()
        val sym = DefSym(u.NoSymbol, name)()(Seq.empty)(Type.unit)
        val goto = DefCall()(sym)(Nil).asInstanceOf[Apply]
        val rhs = WhileBody(cond, asStat(body), goto)
        val label = u.LabelDef(name, Nil, rhs)
        set(label, sym = sym, tpe = Type.unit)
        label
      }

      def unapply(loop: u.LabelDef): Option[(u.Tree, u.Block)] = loop match {
        case u.LabelDef(_, Nil, WhileBody(cond, body: Block, _)) => Some(cond, body)
        case _ => None
      }
    }

    /** `do-while` loops. */
    object DoWhile extends Node {

      /**
       * Creates a type-checked `while` loop.
       * @param cond The loop condition (must be a boolean term).
       * @param body The loop body block.
       * @return `do { body } while (cond)`.
       */
      def apply(cond: u.Tree, body: u.Block): u.LabelDef = {
        assert(is.defined(cond), s"$this condition is not defined: $cond")
        assert(is.defined(body), s"$this body is not defined: $body")
        assert(has.tpe(cond), s"$this condition has no type:\n${Tree.showTypes(cond)}")
        lazy val Cond = Type.of(cond)
        assert(Cond =:= Type.bool, s"$this condition is not boolean:\n${Tree.showTypes(cond)}")

        val name = TermName.DoWhile()
        val sym = DefSym(u.NoSymbol, name)()(Seq.empty)(Type.unit)
        val goto = DefCall()(sym)(Nil)
        val rhs = DoWhileBody(asStat(body), Branch(cond, goto))
        val label = u.LabelDef(name, Nil, rhs)
        set(label, sym = sym, tpe = Type.unit)
        label
      }

      def unapply(loop: u.LabelDef): Option[(u.Tree, u.Block)] = loop match {
        case u.LabelDef(_, Nil, DoWhileBody(body: Block, Branch(cond, _, _))) => Some(cond, body)
        case _ => None
      }
    }

    /** Helper object for `while` loop body blocks. */
    private[ast] object WhileBody extends Node {

      /**
       * Creates a type-checked `while` body.
       * @param cond The loop condition (must be a boolean term).
       * @param stat The loop statement.
       * @param goto Jump back to the loop label definition.
       * @return `if (cond) { stat; while$n() } else ()`.
       */
      def apply(cond: u.Tree, stat: u.Tree, goto: u.Apply): u.If =
        Branch(cond, Block(stat)(goto))

      def unapply(body: u.If): Option[(u.Tree, u.Tree, u.Apply)] = body match {
        case Branch(cond, Block(Seq(stat), goto: Apply), Lit(())) => Some(cond, stat, goto)
        case _ => None
      }
    }

    /** Helper object for `do-while` loop body blocks. */
    private[ast] object DoWhileBody extends Node {

      /**
       * Creates a type-checked `do-while` body.
       * @param stat The loop statement.
       * @param goto Jump back to the loop label definition if the condition is met.
       * @return `{ stat; if (cond) while$n() else () }`.
       */
      def apply(stat: u.Tree, goto: u.If): u.Block =
        Block(stat)(goto)

      def unapply(body: u.Block): Option[(u.Tree, u.If)] = body match {
        case Block(Seq(stat), goto: If) => Some(stat, goto)
        case _ => None
      }
    }
  }
}
