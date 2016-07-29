package eu.stratosphere
package emma.compiler
package lang.core

import emma.util.Monoids
import lang.source.Source

import shapeless._

import scala.annotation.tailrec

/** Administrative Normal Form (ANF) bypassing control-flow and for-comprehensions. */
private[core] trait ANF extends Common {
  this: Source with Core =>

  import universe._
  import Core.{Lang => core}
  import Source.{Lang => src}

  /** Administrative Normal Form (ANF) bypassing control-flow and for-comprehensions. */
  private[core] object ANF {

    /** Ensures that all definitions within `tree` have unique names. */
    def resolveNameClashes(tree: u.Tree): u.Tree =
      api.Tree.refresh(nameClashes(tree): _*)(tree)

    /** The ANF transformation. */
    private lazy val anfTransform: u.Tree => u.Tree =
      api.BottomUp.inherit {
        case tree => is.tpe(tree)
      } (Monoids.disj).withOwner.transformWith {
        // Bypass type-trees
        case Attr.inh(tree, _ :: true :: _) =>
          tree

        // Wrap atomics
        case Attr.none(atom @ src.Atomic(_)) =>
          src.Block()(atom)

        // Bypass parameters
        case Attr.none(par @ src.ParDef(_, _, _)) =>
          par

        // Bypass loops
        case Attr.none(loop: u.LabelDef) =>
          loop

        // Simplify RHS
        case Attr.none(src.VarMut(lhs, src.Block(stats, rhs))) =>
          val mut = src.VarMut(lhs, rhs)
          src.Block(stats :+ mut: _*)()

        // All lambdas on the RHS
        case Attr.none(lambda @ src.Lambda(fun, _, _)) =>
          val lhs = api.TermSym.fresh(fun)
          val dfn = core.ValDef(lhs, lambda)
          val ref = core.ValRef(lhs)
          src.Block(dfn)(ref)

        case Attr.inh( // Simplify expression
          src.TypeAscr(src.Block(stats, expr), tpe),
          api.Encl(owner) :: _) =>

          val nme = api.TermName.fresh(nameOf(expr))
          val lhs = api.ValSym(owner, nme, tpe)
          val rhs = core.TypeAscr(expr, tpe)
          val dfn = core.ValDef(lhs, rhs)
          val ref = core.ValRef(lhs)
          src.Block(stats :+ dfn: _*)(ref)

        case Attr.inh( // Simplify target
          src.ModuleAcc(src.Block(stats, target), module) withType tpe,
          api.Encl(owner) :: _) =>

          val nme = api.TermName.fresh(module)
          val lhs = api.ValSym(owner, nme, tpe)
          val rhs = core.ModuleAcc(target, module)
          val dfn = core.ValDef(lhs, rhs)
          val ref = core.ValRef(lhs)
          src.Block(stats :+ dfn: _*)(ref)

        // Bypass comprehensions
        case Attr.none(src.DefCall(Some(src.Block(Seq(), ir)), method, targs, argss@_*))
          if IR.comprehensionOps(method) =>

          val comprehension = api.DefCall(Some(ir))(method, targs: _*)(argss: _*)
          src.Block()(comprehension)

        case Attr.inh( // Simplify target & arguments
          src.DefCall(target, method, targs, argss@_*) withType tpe,
          api.Encl(owner) :: _) =>

          val (tgtStats, tgtAtom) = target match {
            case Some(src.Block(stats, expr)) => (stats, Some(expr))
            case tgt @ Some(_) => (Seq.empty, tgt)
            case None => (Seq.empty, None)
          }

          val flatStats = tgtStats ++ argss.flatten.flatMap {
            case src.Block(stats, _) => stats
            case _ => Seq.empty
          }

          val atomArgss = argss.map(_.map {
            case src.Block(_, arg) => arg
            case arg => arg
          })

          val nme = api.TermName.fresh {
            if (tgtAtom.isDefined && method.name == api.TermName.app) nameOf(tgtAtom.get)
            else method.name.toString
          }

          val lhs = api.ValSym(owner, nme, tpe)
          val rhs = core.DefCall(tgtAtom)(method, targs: _*)(atomArgss: _*)
          val dfn = core.ValDef(lhs, rhs)
          val ref = core.ValRef(lhs)
          src.Block(flatStats :+ dfn: _*)(ref)

        case Attr.inh( // Simplify arguments
          src.Inst(clazz, targs, argss@_*) withType tpe,
          api.Encl(owner) :: _) =>

          val flatStats = argss.flatten.flatMap {
            case src.Block(stats, _) => stats
            case _ => Seq.empty
          }

          val atomArgss = argss.map(_.map {
            case src.Block(_, arg) => arg
            case arg => arg
          })

          val nme = api.TermName.fresh(api.Sym.of(clazz).name)
          val lhs = api.ValSym(owner, nme, tpe)
          val rhs = core.Inst(clazz, targs: _*)(atomArgss: _*)
          val dfn = core.ValDef(lhs, rhs)
          val ref = core.ValRef(lhs)
          src.Block(flatStats :+ dfn: _*)(ref)

        case Attr.inh( // Flatten blocks
          src.Block(outer, src.Block(inner, expr)),
          api.Encl(owner) :: _) =>

          def wrap(stat: u.Tree): u.Tree = stat match {
            case bind @ src.BindingDef(_, _, _) => bind
            case loop: u.LabelDef => loop
            case mut @ src.VarMut(_, _) => mut
            case call @ src.DefCall(_, method, _, _*)
              if IR.comprehensionOps(method) => call
            case _ =>
              val nme = api.TermName.fresh("stat$")
              val tpe = api.Type.of(stat)
              val lhs = api.ValSym(owner, nme, tpe)
              core.ValDef(lhs, stat)
          }

          val flatStats = outer.flatMap {
            case src.Block(nested, src.Lit(())) => nested
            case src.Block(nested, stat) => nested :+ wrap(stat)
            case stat => Seq(wrap(stat))
          } ++ inner

          src.Block(flatStats: _*)(expr)

        // Avoid duplication of intermediate values
        case Attr.none(src.BindingDef(lhs,
          src.Block(stats :+ (core.ValDef(x, rhs, _)), core.ValRef(y)), flags))
          if x == y =>

          val dfn = src.BindingDef(lhs, rhs, flags)
          src.Block(stats :+ dfn: _*)()

        // Cover vars as well
        case Attr.none(src.BindingDef(lhs, src.Block(stats, rhs), flags)) =>
          val dfn = src.BindingDef(lhs, rhs, flags)
          src.Block(stats :+ dfn: _*)()

        case Attr.inh( // All branches on the RHS
          src.Branch(src.Block(stats, cond), thn, els) withType tpe,
          api.Encl(owner) :: _) =>

          def unwrap(tree: u.Tree) = tree match {
            case src.Block(Seq(), core.Atomic(atom)) => atom
            case src.Block(Seq(), call @ core.DefCall(_, _, _, _*)) => call
            case other => other
          }

          val branch = src.Branch(cond, unwrap(thn), unwrap(els))
          val nme = api.TermName.fresh("if")
          val lhs = api.ValSym(owner, nme, tpe)
          val dfn = src.ValDef(lhs, branch)
          val ref = core.ValRef(lhs)
          src.Block(stats :+ dfn: _*)(ref)
      }.andThen(_.tree)

    /**
     * Converts a tree into administrative normal form (ANF).
     *
     * == Preconditions ==
     *
     * - There are no name clashes (can be ensured with `resolveNameClashes`).
     *
     * == Postconditions ==
     *
     * - Introduces dedicated symbols for chains of length greater than one.
     * - Ensures that all function arguments are trivial identifiers.
     *
     * @param tree The tree to be converted.
     * @return An ANF version of the input tree.
     */
    def anf(tree: u.Tree): u.Tree = {
      assert(nameClashes(tree).isEmpty)
      anfTransform(tree)
    }

    /**
     * Inlines `Ident` return expressions in blocks whenever refered symbol is used only once.
     * The resulting tree is said to be in ''simplified ANF'' form.
     *
     * == Preconditions ==
     * - The input `tree` is in ANF (see [[Core.anf()]]).
     *
     * == Postconditions ==
     * - `Ident` return expressions in blocks have been inlined whenever possible.
     */
    lazy val simplify: u.Tree => u.Tree =
      api.BottomUp.withDefs.withUses.transformWith {
        case Attr.syn(core.Let(values, methods, core.Ref(ret)), uses :: defs :: _)
          if defs.contains(ret) && uses(ret) == 1 =>
            val vdf = defs(ret)
            core.Let(values.filter(_ != vdf): _*)(methods: _*)(vdf.rhs)
      }.andThen(_.tree)

    /**
     * Un-nests nested blocks.
     *
     * == Preconditions ==
     * - Except the nested blocks, the input tree is in simplified ANF form (see [[anf()]] and
     * [[simplify()]]).
     *
     * == Postconditions ==
     * - A simplified ANF tree where all nested blocks have been flattened.
     */
    lazy val flatten: u.Tree => u.Tree =
      api.BottomUp.transform {
        case parent @ src.Block(stats, expr) if hasNestedBlocks(parent) =>
          // Flatten (potentially) nested block stats
          val flatStats = stats.flatMap {
            case src.ValDef(lhs, src.Block(nestedStats, nestedExpr), flags) =>
              nestedStats :+ src.ValDef(lhs, nestedExpr, flags)
            case stat =>
              Seq(stat)
          }

          // Flatten (potentially) nested block expr
          val (exprStats, flatExpr) = expr match {
            case src.Block(nestedStats, nestedExpr) =>
              (nestedStats, nestedExpr)
            case _ =>
              (Seq.empty, expr)
          }

          src.Block(flatStats ++ exprStats: _*)(flatExpr)
      }.andThen(_.tree)

    // ---------------
    // Helper methods
    // ---------------

    /** Does `block` contain nested blocks? */
    private def hasNestedBlocks(block: u.Block): Boolean = {
      lazy val inStats = block.stats.exists {
        case src.ValDef(_, _, src.Block(_, _)) => true
        case _ => false
      }
      lazy val inExpr = block.expr match {
        case src.Block(_, _) => true
        case _ => false
      }
      inStats || inExpr
    }

    /** Returns the set of symbols in `tree` that have clashing names. */
    private def nameClashes(tree: u.Tree): Seq[u.TermSymbol] = for {
      (_, defs) <- api.Tree.defs(tree).groupBy(_.name).toSeq
      if defs.size > 1
      dfn <- defs
    } yield dfn


    /** Returns the encoded name associated with this subtree. */
    private def nameOf(tree: u.Tree): String = {
      @tailrec
      def loop(tree: u.Tree): u.Name = tree match {
        case id: u.Ident => id.name
        case value: u.ValDef => value.name
        case method: u.DefDef => method.name
        case u.Select(_, member) => member
        case u.Typed(expr, _) => loop(expr)
        case u.Block(_, expr) => loop(expr)
        case u.Apply(target, _) => loop(target)
        case u.TypeApply(target, _) => loop(target)
        case _: u.Function => api.TermName.lambda
        case _ => api.TermName("x")
      }

      loop(tree).encodedName.toString
    }
  }
}
