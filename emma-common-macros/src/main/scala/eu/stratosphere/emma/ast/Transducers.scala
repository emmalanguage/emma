package eu.stratosphere
package emma.ast

import emma.util._

import cats.Monoid
import cats.implicits._
import shapeless._

import scala.Function.const
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable
import scala.language.higherKinds

/** Utility for traversing and transforming trees. */
trait Transducers { this: AST =>

  import universe._
  import Monoids._

  private def unit[A]: A => Unit =
    const(())

  private def complete[A, R](pf: A =?> R)(args: A)(default: => R): R =
    pf.applyOrElse(args, (_: A) => default)

  /**
   * Placeholder for attribute grammars.
   * @param accumulation Template for accumulated (along the traversal path) attributes.
   * @param inheritance Template for inherited (top-down) attributes.
   * @param synthesis Template for synthesized (bottom-up) attributes.
   * @param doSyn Skip synthesis if not set (saving one pass over the tree).
   * @param MAcc A monoid for accumulated attributes.
   * @param MInh A monoid for inherited attributes.
   * @param MSyn A monoid for synthesized attributes.
   * @tparam Acc The types of accumulated attributes.
   * @tparam Inh The types of inherited attributes.
   * @tparam Syn The types of synthesized attributes.
   */
  case class AttrGrammar[Acc <: HList, Inh <: HList, Syn <: HList](
      accumulation: Attr[Acc,  Inh,  Syn] =?> Acc = PartialFunction.empty,
      inheritance:  Attr[HNil, Inh,  Syn] =?> Inh = PartialFunction.empty,
      synthesis:    Attr[HNil, HNil, Syn] =?> Syn = PartialFunction.empty,
      doSyn: Boolean = false
  )(implicit
    val MAcc: Monoid[Acc],
    val MInh: Monoid[Inh],
    val MSyn: Monoid[Syn]
  ) {

    /** Prepends an accumulated (along the traversal path) attribute. */
    def accumulate[X](acc: Attr[X :: Acc, Inh, Syn] =?> X)(implicit M: Monoid[X]) =
      copy[X :: Acc, Inh, Syn](
        accumulation = { case attr @ Attr(t, _ :: a, i, s)
          if accumulation.isDefinedAt(Attr(t, a, i, s)) || acc.isDefinedAt(attr) =>
            complete(acc)(attr)(M.empty) :: complete(accumulation)(Attr(t, a, i, s))(MAcc.empty)
      })

    /** Prepends an inherited (from parents) attribute. */
    def inherit[X](inh: Attr[HNil, X :: Inh, Syn] =?> X)(implicit M: Monoid[X]) =
      copy[Acc, X :: Inh, Syn](
        accumulation = { case Attr(t, a, _ :: i, s)
          if accumulation.isDefinedAt(Attr(t, a, i, s)) =>
            accumulation(Attr(t, a, i, s))
        }, inheritance = { case attr @ Attr(t, _, _ :: i, s)
          if inheritance.isDefinedAt(Attr(t, i, s)) || inh.isDefinedAt(attr) =>
            complete(inh)(attr)(M.empty) :: complete(inheritance)(Attr(t, i, s))(MInh.empty)
        })

    /** Prepends a synthesized (from children) attribute. */
    def synthesize[X](syn: Attr[HNil, HNil, X :: Syn] =?> X)(implicit M: Monoid[X]) = {
      copy[Acc, Inh, X :: Syn](
        accumulation = { case Attr(t, a, i, _ :: s)
          if accumulation.isDefinedAt(Attr(t, a, i, s)) =>
            accumulation(Attr(t, a, i, s))
        }, inheritance = { case attr @ Attr(t, _, i, _ :: s)
          if inheritance.isDefinedAt(Attr(t, i, s)) =>
            inheritance(Attr(t, i, s))
        }, synthesis = { case attr @ Attr(t, _, _, _ :: s)
          if synthesis.isDefinedAt(Attr(t, s)) || syn.isDefinedAt(attr) =>
            complete(syn)(attr)(M.empty) :: complete(synthesis)(Attr(t, s))(MSyn.empty)
        }, doSyn = true)
    }
  }

  /** Utility for managing attribute grammars. */
  trait ManagedAttr[Acc <: HList, Inh <: HList, Syn <: HList] {

    val grammar: AttrGrammar[Acc, Inh, Syn]
    val callback: Attr[Acc, Inh, Syn] =?> Any  = PartialFunction.empty
    val template: Attr[Acc, Inh, Syn] =?> Tree = PartialFunction.empty

    implicit def MAcc = grammar.MAcc // accumulation monoid
    implicit def MInh = grammar.MInh // inheritance monoid
    implicit def MSyn = grammar.MSyn // synthesis monoid

    /** Accumulated state. */
    protected var state = MAcc.empty

    /** Inherited state. */
    protected var stack = MInh.empty :: Nil

    /** Synthesized state. */
    protected val cache = mutable.Map.empty[Tree, Syn]

    /** Get all accumulated attributes at the current tree. */
    def acc: Acc = state

    /** Get all inherited attributes at the current tree. */
    def inh: Inh = stack.head

    /** Get all synthesized attributes at `tree`. */
    def syn(tree: Tree): Syn =
      if (grammar.doSyn) synMemo(tree)
      else MSyn.empty

    /** Annotates `tree` with all attributes. */
    def ann(tree: Tree): Attr[Acc, Inh, Syn] =
      Attr(tree, acc, inh, syn(tree))

    private lazy val synMemo: Tree => Syn = Memo.recur[Tree, Syn]({ syn => tree =>
      val prev = tree.children.map(syn).foldLeft(MSyn.empty)(MSyn.combine)
      complete(grammar.synthesis)(Attr(tree, prev))(prev)
    }, cache)

    protected lazy val accumulation: Tree =?> Acc = {
      case tree if grammar.accumulation.isDefinedAt(ann(tree)) =>
        grammar.accumulation(ann(tree))
    }

    protected lazy val inheritance: Tree =?> Inh = {
      case tree if grammar.inheritance.isDefinedAt(Attr(tree, inh, syn(tree))) =>
        grammar.inheritance(Attr(tree, inh, syn(tree)))
    }

    protected lazy val traversal: Tree =?> Unit = {
      case tree if callback.isDefinedAt(ann(tree)) =>
        callback(ann(tree))
    }

    protected lazy val transformation: Tree =?> Tree = {
      case tree if template.isDefinedAt(ann(tree)) =>
        template(ann(tree))
    }

    /** Inherit attributes for `tree`. */
    protected def at[A](tree: Tree)(f: => A): A =
      if (inheritance.isDefinedAt(tree)) {
        stack ::= inh |+| inheritance(tree)
        val result = f
        stack = stack.tail
        result
      } else f

    /** Accumulate attributes for `tree`. */
    protected def accumulate(tree: Tree): Unit =
      if (accumulation.isDefinedAt(tree)) {
        state = acc |+| accumulation(tree)
      }

    /** Resets the state so that another tree can be traversed. */
    protected def reset(): Unit = {
      state = MAcc.empty
      stack = MInh.empty :: Nil
      cache.clear()
    }
  }

  /**
   * A traversal / transformation strategy.
   * 4 fundamental strategies are available:
   *
   * 1. Top-down continue left-to-right
   * 2. Top-down break left-to-right
   * 3. Bottom-up continue left-to-right
   * 4. Bottom-up break left-to-right
   *
   * Right-to-left variants are not supported.
   */
  case class Strategy[A <: HList, I <: HList, S <: HList](
      grammar: AttrGrammar[A, I, S], factory: TransFactory) {

    type Acc = A
    type Inh = I
    type Syn = S

    /** Prepends an accumulated attribute based on all attributes. */
    def accumulateWith[X: Monoid](acc: Attr[X :: Acc, Inh, Syn] =?> X) =
      copy(grammar = grammar.accumulate(acc))

    /** Prepends an accumulated attribute based on trees only. */
    def accumulate[X: Monoid](acc: Tree =?> X) =
      copy(grammar = grammar.accumulate[X] {
        case Attr(t, _, _, _) if acc.isDefinedAt(t) => acc(t)
      })

    /** Prepends an inherited attribute based on inherited and synthesized attributes. */
    def inheritWith[X: Monoid](inh: Attr[HNil, X :: Inh, Syn] =?> X) =
      copy(grammar = grammar.inherit(inh))

    /** Prepends an inherited attribute based on trees only. */
    def inherit[X: Monoid](inh: Tree =?> X) =
      copy(grammar = grammar.inherit[X] {
        case Attr(t, _, _, _) if inh.isDefinedAt(t) => inh(t)
      })

    /** Prepends a synthesized attribute based on all synthesized attributes. */
    def synthesizeWith[X: Monoid](syn: Attr[HNil, HNil, X :: Syn] =?> X) =
      copy(grammar = grammar.synthesize(syn))

    /** Prepends a synthesized attribute based on trees only. */
    def synthesize[X: Monoid](syn: Tree =?> X) =
      copy(grammar = grammar.synthesize[X] {
        case Attr(t, _, _, _) if syn.isDefinedAt(t) => syn(t)
      })

    /** Traverses a tree with access to all attributes. */
    def traverseWith(callback: Attr[Acc, Inh, Syn] =?> Any): Traversal[Acc, Inh, Syn] =
      factory.traversal(grammar)(callback)

    /** Traverses a tree without access to attributes. */
    def traverse(callback: Tree =?> Any): Traversal[Acc, Inh, Syn] =
      factory.traversal(grammar) { case Attr(t, _, _, _)
        if callback.isDefinedAt(t) => callback(t)
      }

    /** Shortcut for visiting every node in a tree. */
    def traverseAny: Traversal[Acc, Inh, Syn] =
      factory.traversal(grammar) { case _ => () }

    /** Transforms a tree with access to all attributes. */
    def transformWith(template: Attr[Acc, Inh, Syn] =?> Tree): Transform[Acc, Inh, Syn] =
      factory.transform(grammar)(template)

    /** Transforms a tree without access to attributes. */
    def transform(template: Tree =?> Tree): Transform[Acc, Inh, Syn] =
      factory.transform(grammar) { case Attr(t, _, _, _)
        if template.isDefinedAt(t) => template(t)
      }

    /** Inherits all ancestors of the current node in a vector. */
    def withAncestors = inherit(Attr.collect[Vector, Tree] {
      case ancestor => ancestor
    })

    /** Synthesizes all binding definitions contained in the current node and its children. */
    def withDefs = synthesize(Attr.group {
      case bind @ api.BindingDef(lhs, _, _) => lhs -> bind
    })

    /** Counts all term references contained in the current node and its children. */
    def withUses = synthesize(Attr.group {
      case api.TermRef(target) => target -> 1
    })(Monoids.merge)

    /** Inherits the owner of the current node. */
    def withOwner = inherit {
      case api.Owner(sym) => sym
    } (Monoids.right(NoSymbol))

    /** Inherits the parent of the current node. */
    def withParent = inherit {
      case parent => parent
    } (Monoids.right(EmptyTree))
  }

  /** An abstract transformation (default is top-down break). */
  abstract class Transform[A <: HList, I <: HList, S <: HList]
    (val grammar: AttrGrammar[A, I, S], override val template: Attr[A, I, S] =?> Tree)
    extends Transformer with ManagedAttr[A, I, S] with (Tree => Attr[A, I, S]) {

    override def apply(tree: Tree): Attr[A, I, S] = {
      reset()
      ann(transform(tree))
    }

    override def transform(tree: Tree): Tree = at(tree)(tree match {
      // NOTE: TypeTree.original is not transformed by default
      case tpt: TypeTree if tpt.original != null =>
        val original = transform(tpt.original)
        if (original eq tpt.original) tpt else {
          val copy = treeCopy.TypeTree(tpt)
          set.original(copy, original)
          copy
        }

      case _ =>
        super.transform(tree)
    })
  }

  /** An abstract traversal (default is top-down break). */
  abstract class Traversal[A <: HList, I <: HList, S <: HList]
    (val grammar: AttrGrammar[A, I, S], override val callback: Attr[A, I, S] =?> Any)
    extends Traverser with ManagedAttr[A, I, S] with (Tree => Attr[A, I, S]) {

    override def apply(tree: Tree): Attr[A, I, S] = {
      reset()
      traverse(tree)
      ann(tree)
    }

    override def traverse(tree: Tree): Unit = at(tree)(tree match {
      // NOTE: TypeTree.original is not traversed by default
      case tpt: TypeTree if tpt.original != null => traverse(tpt.original)
      case _ => super.traverse(tree)
    })
  }

  /** A traversal / transformation factory. */
  private[ast] trait TransFactory {

    def traversal[A <: HList, I <: HList, S <: HList]
      (grammar: AttrGrammar[A, I, S])
      (callback: Attr[A, I, S] =?> Any)
      : Traversal[A, I, S]

    def transform[A <: HList, I <: HList, S <: HList]
      (grammar: AttrGrammar[A, I, S])
      (template: Attr[A, I, S] =?> Tree)
      : Transform[A, I, S]
  }

  /** A traversal / transformation strategy factory. */
  private[ast] object TransFactory {

    /** Top-down traversal / transformation. */
    object topDown extends TransFactory {

      /** Top-down continue traversal. */
      override def traversal[A <: HList, I <: HList, S <: HList]
        (grammar: AttrGrammar[A, I, S])
        (callback: Attr[A, I, S] =?> Any)
        : Traversal[A, I, S]
        = Traversal.topDown(grammar)(callback)

      /** Top-down continue transformation. */
      override def transform[A <: HList, I <: HList, S <: HList]
        (grammar: AttrGrammar[A, I, S])
        (template: Attr[A, I, S] =?> Tree)
        : Transform[A, I, S]
        = Transform.topDown(grammar)(template)

      /** Top-down break traversal / transformation. */
      object break extends TransFactory {

        /** Top-down break traversal. */
        override def traversal[A <: HList, I <: HList, S <: HList]
          (grammar: AttrGrammar[A, I, S])
          (callback: Attr[A, I, S] =?> Any)
          : Traversal[A, I, S]
          = Traversal.topDown.break(grammar)(callback)

        /** Top-down break transformation. */
        override def transform[A <: HList, I <: HList, S <: HList]
          (grammar: AttrGrammar[A, I, S])
          (template: Attr[A, I, S] =?> Tree)
          : Transform[A, I, S]
          = Transform.topDown.break(grammar)(template)
      }
    }

    /** Bottom-up traversal / transformation. */
    object bottomUp extends TransFactory {

      /** Bottom-up continue traversal. */
      override def traversal[A <: HList, I <: HList, S <: HList]
        (grammar: AttrGrammar[A, I, S])
        (callback: Attr[A, I, S] =?> Any)
        : Traversal[A, I, S]
        = Traversal.bottomUp(grammar)(callback)

      /** Bottom-up continue transformation. */
      override def transform[A <: HList, I <: HList, S <: HList]
        (grammar: AttrGrammar[A, I, S])
        (template: Attr[A, I, S] =?> Tree)
        : Transform[A, I, S]
        = Transform.bottomUp(grammar)(template)

      /** Bottom-up break traversal / transformation. */
      object break extends TransFactory {

        /** Bottom-up break traversal. */
        override def traversal[A <: HList, I <: HList, S <: HList]
          (grammar: AttrGrammar[A, I, S])
          (callback: Attr[A, I, S] =?> Any)
          : Traversal[A, I, S]
          = Traversal.bottomUp.break(grammar)(callback)

        /** Bottom-up break transformation. */
        override def transform[A <: HList, I <: HList, S <: HList]
          (grammar: AttrGrammar[A, I, S])
          (template: Attr[A, I, S] =?> Tree)
          : Transform[A, I, S]
          = Transform.bottomUp.break(grammar)(template)
      }
    }
  }

  /** Transformations. */
  private[ast] object Transform {

    /** Top-down transformation. */
    object topDown {

      /** Top-down continue transformation. */
      def apply[A <: HList, I <: HList, S <: HList]
        (grammar: AttrGrammar[A, I, S])(template: Attr[A, I, S] =?> Tree)
        : Transform[A, I, S] = new Transform[A, I, S](grammar, template) {
          override final def transform(tree: Tree): Tree = {
            accumulate(tree)
            super.transform(complete(transformation)(tree)(tree))
          }
        }

      /** Top-down break transformation. */
      def break[A <: HList, I <: HList, S <: HList]
        (grammar: AttrGrammar[A, I, S])(template: Attr[A, I, S] =?> Tree)
        : Transform[A, I, S] = new Transform[A, I, S](grammar, template) {
          override final def transform(tree: Tree): Tree = {
            accumulate(tree)
            transformation.applyOrElse(tree, super.transform)
          }
        }
    }

    /** Bottom-up transformation. */
    object bottomUp {

      /** Bottom-up continue transformation. */
      def apply[A <: HList, I <: HList, S <: HList]
        (grammar: AttrGrammar[A, I, S])(template: Attr[A, I, S] =?> Tree)
        : Transform[A, I, S] = new Transform[A, I, S](grammar, template) {
          override final def transform(tree: Tree): Tree = {
            val recur = super.transform(tree)
            accumulate(recur)
            complete(transformation)(recur)(recur)
          }
        }

      /** Bottom-up break transformation. */
      def break[A <: HList, I <: HList, S <: HList]
        (grammar: AttrGrammar[A, I, S])(template: Attr[A, I, S] =?> Tree)
        : Transform[A, I, S] = new Transform[A, I, S](grammar, template) {
          val matches: mutable.Set[Tree] = mutable.Set.empty
          override final def transform(tree: Tree): Tree = {
            val recur = super.transform(tree)
            val children = tree.children
            if (children.exists(matches)) {
              matches --= children
              matches += recur
              recur
            } else {
              accumulate(recur)
              if (transformation.isDefinedAt(recur)) {
                matches += recur
                transformation(recur)
              } else recur
            }
          }
        }
    }
  }

  /** Traversals. */
  private[ast] object Traversal {

    /** Top-down traversal. */
    object topDown {

      /** Top-down continue traversal. */
      def apply[A <: HList, I <: HList, S <: HList]
        (grammar: AttrGrammar[A, I, S])(callback: Attr[A, I, S] =?> Any)
        : Traversal[A, I, S] = new Traversal[A, I, S](grammar, callback) {
          override final def traverse(tree: Tree): Unit = {
            accumulate(tree)
            traversal.applyOrElse(tree, unit[Tree])
            super.traverse(tree)
          }
        }

      /** Top-down break traversal. */
      def break[A <: HList, I <: HList, S <: HList]
        (grammar: AttrGrammar[A, I, S])(callback: Attr[A, I, S] =?> Any)
        : Traversal[A, I, S] = new Traversal[A, I, S](grammar, callback) {
          override final def traverse(tree: Tree): Unit = {
            accumulate(tree)
            traversal.applyOrElse(tree, super.traverse)
          }
        }
    }

    /** Bottom-up traversal. */
    object bottomUp {

      /** Bottom-up continue traversal. */
      def apply[A <: HList, I <: HList, S <: HList]
        (grammar: AttrGrammar[A, I, S])(callback: Attr[A, I, S] =?> Any)
        : Traversal[A, I, S] = new Traversal[A, I, S](grammar, callback) {
          override final def traverse(tree: Tree): Unit = {
            super.traverse(tree)
            accumulate(tree)
            traversal.applyOrElse(tree, unit[Tree])
          }
        }

      /** Bottom-up break traversal. */
      def break[A <: HList, I <: HList, S <: HList]
        (grammar: AttrGrammar[A, I, S])(callback: Attr[A, I, S] =?> Any)
        : Traversal[A, I, S] = new Traversal[A, I, S](grammar, callback) {
          val matches: mutable.Set[Tree] = mutable.Set.empty
          override final def traverse(tree: Tree): Unit = {
            super.traverse(tree)
            val children = tree.children
            if (children.exists(matches)) {
              matches --= children
              matches += tree
            } else {
              accumulate(tree)
              if (traversal.isDefinedAt(tree)) {
                matches += tree
                traversal(tree)
              }
            }
          }
        }
    }
  }

  /**
   * An attributed tree.
   * @param tree The tree being attributed.
   * @param acc Accumulated attributes.
   * @param inh Inherited attributes.
   * @param syn Synthesized attributes.
   */
  case class Attr[Acc <: HList, Inh <: HList, Syn <: HList]
    (tree: u.Tree, acc: Acc, inh: Inh, syn: Syn)

  object Attr {

    /** Constructor that discards accumulated attributes. */
    def apply[Inh <: HList, Syn <: HList]
      (tree: u.Tree, inh: Inh, syn: Syn)
      : Attr[HNil, Inh, Syn]
      = apply(tree, HNil, inh, syn)

    /** Constructor that discards accumulated and inherited attributes. */
    def apply[Syn <: HList]
      (tree: u.Tree, syn: Syn)
      : Attr[HNil, HNil, Syn]
      = apply(tree, HNil, HNil, syn)

    /** Collects attributes in a specified type of collection. */
    def collect[Col[x] <: Traversable[x], El](elem: Tree =?> El)
      (implicit Col: CanBuildFrom[Nothing, El, Col[El]]): Tree =?> Col[El] = {

      case tree if elem.isDefinedAt(tree) =>
        val col = Col()
        col += elem(tree)
        col.result()
    }

    /** Collects key -> value attributes in a Map. */
    def group[K, V](kv: Tree =?> (K, V)): Tree =?> Map[K, V] = {
      case tree if kv.isDefinedAt(tree) => Map(kv(tree))
    }

    /** Extractor that discards the attributes of a tree. */
    object none {
      def unapply[Acc <: HList, Inh <: HList, Syn <: HList]
        (attr: Attr[Acc, Inh, Syn]): Option[u.Tree] = attr match {
          case Attr(tree, _, _, _) => Some(tree)
          case _ => None
        }
    }

    /** Extractor for the accumulated attributes of a tree. */
    object acc {
      def unapply[Acc <: HList, Inh <: HList, Syn <: HList]
        (attr: Attr[Acc, Inh, Syn]): Option[(u.Tree, Acc)] = attr match {
          case Attr(tree, acc, _, _) => Some(tree, acc)
          case _ => None
        }
    }

    /** Extractor for the inherited attributes of a tree. */
    object inh {
      def unapply[Acc <: HList, Inh <: HList, Syn <: HList]
        (attr: Attr[Acc, Inh, Syn]): Option[(u.Tree, Inh)] = attr match {
          case Attr(tree, _, inh, _) => Some(tree, inh)
          case _ => None
        }
    }

    /** Extractor for the synthesized attributes of a tree. */
    object syn {
      def unapply[Acc <: HList, Inh <: HList, Syn <: HList]
        (attr: Attr[Acc, Inh, Syn]): Option[(u.Tree, Syn)] = attr match {
          case Attr(tree, _, _, syn) => Some(tree, syn)
          case _ => None
        }
    }
  }

  /** Fluent tree traversal / transformation APIs. */
  trait TransducerAPI { this: API =>

    /** Top-down traversal / transformation and attribute generation. */
    object TopDown extends Strategy[HNil, HNil, HNil](
        new AttrGrammar[HNil, HNil, HNil](),
        TransFactory.topDown) {

      /** Top-down break traversal / transformation and attribute generation. */
      object break extends Strategy[HNil, HNil, HNil](
          new AttrGrammar[HNil, HNil, HNil](),
          TransFactory.topDown.break)
    }

    /** Bottom-up traversal / transformation and attribute generation. */
    object BottomUp extends Strategy[HNil, HNil, HNil](
        new AttrGrammar[HNil, HNil, HNil](),
        TransFactory.bottomUp) {

      /** Bottom-up break traversal / transformation and attribute generation. */
      object break extends Strategy[HNil, HNil, HNil](
          new AttrGrammar[HNil, HNil, HNil](),
          TransFactory.bottomUp.break)
    }
  }
}
