package eu.stratosphere
package emma.ast

/** Loops (`while` and `do-while`). */
trait Loops { this: AST =>

  /** Loops (`while` and `do-while`). */
  trait LoopAPI { this: API =>

    import universe._
    import u.Flag._
    import internal.newMethodSymbol

    /** Converts `tree` to a statement (block with `Unit` expression). */
    private def asStat(tree: u.Tree) = tree match {
      case Block(_, Lit(())) => tree
      case u.Block(stats, stat) => Block(stats :+ stat: _*)()
      case _ => Block(tree)()
    }

    /** Label (loop) symbols. */
    object LabelSym extends Node {

      /**
       * Creates a new label symbol.
       * @param owner The enclosing named entity where this label is defined.
       * @param name The name of this label (will be encoded).
       * @param flags Any additional modifiers.
       * @param pos The (optional) source code position where this label is defined.
       * @return A new label symbol.
       */
      def apply(owner: u.Symbol, name: u.TermName,
        flags: u.FlagSet = u.NoFlags,
        pos: u.Position = u.NoPosition)
        : u.MethodSymbol = {

        val label = newMethodSymbol(owner, TermName(name), pos, flags | CONTRAVARIANT)
        set.tpe(label, Type.loop)
        label
      }

      def unapply(sym: u.MethodSymbol): Option[u.MethodSymbol] =
        Option(sym).filter(is.label)
    }

    /** Extractor for loops (`while` and `do-while`). */
    object Loop extends Node {
      def unapply(loop: u.LabelDef): Option[(u.Tree, u.Tree)] = loop match {
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
      def apply(cond: u.Tree, body: u.Tree): u.LabelDef = {
        assert(is.defined(cond), s"$this condition is not defined: $cond")
        assert(is.defined(body), s"$this body is not defined: $body")
        assert(has.tpe(cond), s"$this condition has no type:\n${Tree.showTypes(cond)}")
        lazy val Cond = Type.of(cond)
        assert(Cond =:= Type.bool, s"$this condition is not boolean:\n${Tree.showTypes(cond)}")

        val nme = TermName.While()
        val lbl = LabelSym(u.NoSymbol, nme)
        val rhs = WhileBody(lbl, cond, asStat(body))
        val dfn = u.LabelDef(nme, Nil, rhs)
        set(dfn, sym = lbl, tpe = Type.unit)
        dfn
      }

      def unapply(loop: u.LabelDef): Option[(u.Tree, u.Tree)] = loop match {
        case u.LabelDef(_, Nil, WhileBody(_, cond, body)) => Some(cond, body)
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
      def apply(cond: u.Tree, body: u.Tree): u.LabelDef = {
        assert(is.defined(cond), s"$this condition is not defined: $cond")
        assert(is.defined(body), s"$this body is not defined: $body")
        assert(has.tpe(cond), s"$this condition has no type:\n${Tree.showTypes(cond)}")
        lazy val Cond = Type.of(cond)
        assert(Cond =:= Type.bool, s"$this condition is not boolean:\n${Tree.showTypes(cond)}")

        val nme = TermName.DoWhile()
        val lbl = LabelSym(u.NoSymbol, nme)
        val rhs = DoWhileBody(lbl, cond, asStat(body))
        val dfn = u.LabelDef(nme, Nil, rhs)
        set(dfn, sym = lbl, tpe = Type.unit)
        dfn
      }

      def unapply(loop: u.LabelDef): Option[(u.Tree, u.Tree)] = loop match {
        case u.LabelDef(_, Nil, DoWhileBody(_, cond, body)) => Some(cond, body)
        case _ => None
      }
    }

    /** Helper object for `while` loop body blocks. */
    private[ast] object WhileBody extends Node {

      /**
       * Creates a type-checked `while` body.
       * @param label The label symbol.
       * @param cond The loop condition (must be a boolean term).
       * @param stat The loop statement.
       * @return `if (cond) { stat; while$n() } else ()`.
       */
      def apply(label: u.MethodSymbol, cond: u.Tree, stat: u.Tree): u.If = {
        assert(is.defined(label), s"$this label `$label` is not defined")
        assert(is.label(label), s"$this label `$label` is not a label")
        Branch(cond, Block(stat)(TermApp(Id(label), Seq.empty)))
      }

      def unapply(body: u.If): Option[(u.MethodSymbol, u.Tree, u.Tree)] = body match {
        case u.If(Term(cond), u.Block(stat :: Nil,
          TermApp(Id(LabelSym(label)), Seq(), Seq())), Lit(()))
          => Some(label, cond, stat)
        case _
          => None
      }
    }

    /** Helper object for `do-while` loop body blocks. */
    private[ast] object DoWhileBody extends Node {

      /**
       * Creates a type-checked `do-while` body.
       * @param label The label symbol.
       * @param cond The loop condition (must be a boolean term).
       * @param stat The loop statement.
       * @return `{ stat; if (cond) while$n() else () }`.
       */
      def apply(label: u.MethodSymbol, cond: u.Tree, stat: u.Tree): u.Block = {
        assert(is.defined(label), s"$this label `$label` is not defined")
        assert(is.label(label), s"$this label `$label` is not a label")
        Block(stat)(Branch(cond, TermApp(Id(label), Seq.empty)))
      }

      def unapply(body: u.Block): Option[(u.MethodSymbol, u.Tree, u.Tree)] = body match {
        case u.Block(stat :: Nil, u.If(Term(cond),
          TermApp(Id(LabelSym(label)), Seq(), Seq()), Lit(())))
          => Some(label, cond, stat)
        case _
          => None
      }
    }
  }
}
