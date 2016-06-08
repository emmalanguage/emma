package eu.stratosphere
package emma.compiler

import emma.util.Memo
import emma.util.Monoids

import cats._
import cats.implicits._
import shapeless._

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable
import scala.Function.const
import scala.language.higherKinds

trait Transducers extends Util { this: ReflectUtil =>

  import universe._
  import Monoids._

  private def unit[A]: A => Unit =
    const(()) _

  private def complete[A, R](pf: A =?> R)(args: A)(default: => R): R =
    pf.applyOrElse(args, (_: A) => default)

  private def extend
    [A, H, T <: HList]
    (pf: (A, T) =?> T)
    (ext: (A, H :: T) =?> H)
    (implicit Mh: Monoid[H], Mt: Monoid[T])
    : (A, H :: T) =?> (H :: T) = {

    case args @ (a, _ :: t) if pf.isDefinedAt(a, t) || ext.isDefinedAt(args) =>
      complete(ext)(args)(Mh.empty) :: complete(pf)(a, t)(Mt.empty)
  }

  case class Attr[
    Acc <: HList : Monoid,
    Inh <: HList : Monoid,
    Syn <: HList : Monoid](
      accumulation: (Tree, Acc) =?> Acc = PartialFunction.empty,
      inheritance: (Tree, Inh) =?> Inh = PartialFunction.empty,
      synthesis: (Tree, Syn) =?> Syn = PartialFunction.empty,
      synth: Boolean = false /* Skip if not specified */) {

    val MAcc = Monoid[Acc]
    val MInh = Monoid[Inh]
    val MSyn = Monoid[Syn]

    def accumulate[A]
      (acc: (Tree, A :: Acc) =?> A)
      (implicit M: Monoid[A])
      : Attr[A :: Acc, Inh, Syn] = {

      copy(accumulation = extend(accumulation)(acc))
    }

    def inherit[I]
      (inh: (Tree, I :: Inh) =?> I)
      (implicit M: Monoid[I])
      : Attr[Acc, I :: Inh, Syn] = {

      copy(inheritance = extend(inheritance)(inh))
    }

    def synthesize[S]
      (syn: (Tree, S :: Syn) =?> S)
      (implicit M: Monoid[S])
      : Attr[Acc, Inh, S :: Syn] = {

      copy(synthesis = extend(synthesis)(syn), synth = true)
    }
  }

  object Attr {

    object at {

      lazy val any: (Tree, Any, Any, Any) =?> Unit =
        PartialFunction(unit)

      def apply[R](pf: Tree =?> R): (Tree, Any, Any, Any) =?> R =
        tree(pf)

      def tree[R](pf: Tree =?> R): (Tree, Any, Any, Any) =?> R = {
        case (tree, _, _, _) if pf.isDefinedAt(tree) => pf(tree)
      }

      def acc[A, R](pf: (Tree, A) =?> R): (Tree, A, Any, Any) =?> R = {
        case (tree, acc, _, _) if pf.isDefinedAt(tree, acc) => pf(tree, acc)
      }

      def inh[I, R](pf: (Tree, I) =?> R): (Tree, Any, I, Any) =?> R = {
        case (tree, _, inh, _) if pf.isDefinedAt(tree, inh) => pf(tree, inh)
      }

      def syn[S, R](pf: (Tree, S) =?> R): (Tree, Any, Any, S) =?> R = {
        case (tree, _, _, syn) if pf.isDefinedAt(tree, syn) => pf(tree, syn)
      }
    }

    def collect[Col[x] <: Traversable[x], El, T <: HList]
      (elem: (Tree, Col[El] :: T) =?> El)
      (implicit from: CanBuildFrom[Nothing, El, Col[El]])
      : (Tree, Col[El] :: T) =?> Col[El] = {

      case args if elem.isDefinedAt(args) =>
        val builder = from()
        builder += elem(args)
        builder.result()
    }

    def group[K, V, T <: HList]
      (kv: (Tree, Map[K, V] :: T) =?> (K, V))
      : (Tree, Map[K, V] :: T) =?> Map[K, V] = {

      case args if kv.isDefinedAt(args) =>
        Map(kv(args))
    }
  }

  trait ManagedAttr[Acc <: HList, Inh <: HList, Syn <: HList] {

    def attr: Attr[Acc, Inh, Syn]
    def callback: (Tree, Acc, Inh, Syn) =?> Any = PartialFunction.empty
    def template: (Tree, Acc, Inh, Syn) =?> Tree = PartialFunction.empty

    implicit def MAcc = attr.MAcc
    implicit def MInh = attr.MInh
    implicit def MSyn = attr.MSyn

    protected var state = MAcc.empty
    protected var stack = MInh.empty :: Nil
    protected val cache = mutable.Map.empty[Tree, Syn]

    def acc: Acc = state
    def inh: Inh = stack.head

    lazy val syn: Tree => Syn = Memo.recur[Tree, Syn]({ syn => tree =>
      val prev = tree.children.map(syn).foldLeft(MSyn.empty)(MSyn.combine)
      complete(attr.synthesis)(tree, prev)(prev)
    }, cache)

    protected def ann(tree: Tree): (Tree, Acc, Inh, Syn) =
      (tree, acc, inh, if (attr.synth) syn(tree) else MSyn.empty)

    protected lazy val accumulation: Tree =?> Acc = {
      case tree if attr.accumulation.isDefinedAt(tree, acc) =>
        attr.accumulation(tree, acc)
    }

    protected lazy val inheritance: Tree =?> Inh = {
      case tree if attr.inheritance.isDefinedAt(tree, inh) =>
        attr.inheritance(tree, inh)
    }

    protected lazy val traversal: Tree =?> Unit = {
      case tree if callback.isDefinedAt(ann(tree)) =>
        callback(ann(tree))
    }

    protected lazy val transformation: Tree =?> Tree = {
      case tree if template.isDefinedAt(ann(tree)) =>
        template(ann(tree))
    }

    protected def at[A](tree: Tree)(f: => A): A =
      if (inheritance.isDefinedAt(tree)) {
        stack ::= inh |+| inheritance(tree)
        val result = f
        stack = stack.tail
        result
      } else f

    protected def accumulate(tree: Tree): Unit =
      if (accumulation.isDefinedAt(tree)) {
        state = acc |+| accumulation(tree)
      }
  }

  case class Strategy[A <: HList, I <: HList, S <: HList](
      attr: Attr[A, I, S], factory: TransFactory) {

    type Acc = A
    type Inh = I
    type Syn = S

    def accumulate[A: Monoid](acc: (Tree, A :: Acc) =?> A): Strategy[A :: Acc, Inh, Syn] =
      copy(attr = attr.accumulate(acc))

    def inherit[I: Monoid](inh: (Tree, I :: Inh) =?> I): Strategy[Acc, I :: Inh, Syn] =
      copy(attr = attr.inherit(inh))

    def synthesize[S: Monoid](syn: (Tree, S :: Syn) =?> S): Strategy[Acc, Inh, S :: Syn] =
      copy(attr = attr.synthesize(syn))

    def traverse[U](callback: (Tree, Acc, Inh, Syn) =?> U): Tree => (Acc, Syn) =
      factory.traversal(attr)(callback)

    def transform(template: (Tree, Acc, Inh, Syn) =?> Tree): Tree => (Tree, Acc, Syn) =
      factory.transform(attr)(template)

    def withAncestors = inherit[Vector[Tree]] {
      case (tree, _) => Vector(tree)
    }

    def withParent = inherit[Tree] {
      case (tree, _) => tree
    } (Monoids.right(EmptyTree))

    def withOwner = inherit[Symbol] {
      case (tree, _) if isOwner(tree) => tree.symbol
    } (Monoids.right(NoSymbol))

    private def isOwner(tree: Tree) = tree match {
      case _: Function => true
      case tree: Tree => tree.isDef
      case _ => false
    }

    def withValDefs = synthesize[Map[TermSymbol, ValDef]] {
      case (value @ Tree.val_(lhs, _, _), defs :: _) =>
        defs + (lhs -> value)
    }

    def withTermRefs = synthesize[Map[TermSymbol, Int]] {
      case (Term.ref(target), refs :: _) =>
        refs + (target -> (refs(target) + 1))
    } (Monoids.merge)
  }

  abstract class Transform[A <: HList, I <: HList, S <: HList]
    (val attr: Attr[A, I, S], override val template: (Tree, A, I, S) =?> Tree)
    extends Transformer with ManagedAttr[A, I, S] with (Tree => (Tree, A, S)) {

    override def apply(tree: Tree): (Tree, A, S) = {
      state = MAcc.empty
      stack = MInh.empty :: Nil
      cache.clear()
      val (t, a, _, s) = ann(transform(tree))
      (t, a, s)
    }

    override def transform(tree: Tree): Tree = at(tree)(tree match {
      // NOTE: `TypeTree.original` is not transformed by default
      case tpt: TypeTree if tpt.original != null =>
        val original = transform(tpt.original)
        if (original eq tpt.original) tpt else {
          val copy = treeCopy.TypeTree(tpt)
          setOriginal(copy, original)
          copy
        }

      case _ =>
        super.transform(tree)
    })
  }

  abstract class Traversal[A <: HList, I <: HList, S <: HList]
    (val attr: Attr[A, I, S], override val callback: (Tree, A, I, S) =?> Any)
    extends Traverser with ManagedAttr[A, I, S] with (Tree => (A, S)) {

    override def apply(tree: Tree): (A, S) = {
      state = MAcc.empty
      stack = MInh.empty :: Nil
      cache.clear()
      traverse(tree)
      val (_, a, _, s) = ann(tree)
      (a, s)
    }

    override def traverse(tree: Tree): Unit = at(tree)(tree match {
      // NOTE: TypeTree.original is not traversed by default
      case tpt: TypeTree if tpt.original != null => traverse(tpt.original)
      case _ => super.traverse(tree)
    })
  }

  trait TransFactory {

    def traversal[A <: HList, I <: HList, S <: HList, U]
      (attr: Attr[A, I, S])(callback: (Tree, A, I, S) =?> U): Traversal[A, I, S]

    def transform[A <: HList, I <: HList, S <: HList]
      (attr: Attr[A, I, S])(template: (Tree, A, I, S) =?> Tree): Transform[A, I, S]
  }

  object TransFactory {

    object topDown extends TransFactory {

      override def traversal[A <: HList, I <: HList, S <: HList, U]
        (attr: Attr[A, I, S])(callback: (Tree, A, I, S) =?> U): Traversal[A, I, S] =
        Traversal.topDown(attr)(callback)

      override def transform[A <: HList, I <: HList, S <: HList]
        (attr: Attr[A, I, S])(template: (Tree, A, I, S) =?> Tree): Transform[A, I, S] =
        Transform.topDown(attr)(template)

      object break extends TransFactory {

        override def traversal[A <: HList, I <: HList, S <: HList, U]
          (attr: Attr[A, I, S])(callback: (Tree, A, I, S) =?> U): Traversal[A, I, S] =
          Traversal.topDown.break(attr)(callback)

        override def transform[A <: HList, I <: HList, S <: HList]
          (attr: Attr[A, I, S])(template: (Tree, A, I, S) =?> Tree): Transform[A, I, S] =
          Transform.topDown.break(attr)(template)
      }
    }

    object bottomUp extends TransFactory {

      override def traversal[A <: HList, I <: HList, S <: HList, U]
        (attr: Attr[A, I, S])(callback: (Tree, A, I, S) =?> U): Traversal[A, I, S] =
        Traversal.bottomUp(attr)(callback)

      override def transform[A <: HList, I <: HList, S <: HList]
        (attr: Attr[A, I, S])(template: (Tree, A, I, S) =?> Tree): Transform[A, I, S] =
        Transform.bottomUp(attr)(template)

      object break extends TransFactory {

        override def traversal[A <: HList, I <: HList, S <: HList, U]
          (attr: Attr[A, I, S])(callback: (Tree, A, I, S) =?> U): Traversal[A, I, S] =
          Traversal.bottomUp.break(attr)(callback)

        override def transform[A <: HList, I <: HList, S <: HList]
          (attr: Attr[A, I, S])(template: (Tree, A, I, S) =?> Tree): Transform[A, I, S] =
          Transform.bottomUp.break(attr)(template)
      }
    }
  }

  object Transform {

    object topDown {

      def apply[A <: HList, I <: HList, S <: HList]
        (attr: Attr[A, I, S])(template: (Tree, A, I, S) =?> Tree)
        : Transform[A, I, S] = new Transform[A, I, S](attr, template) {
          override final def transform(tree: Tree): Tree = {
            accumulate(tree)
            super.transform(complete(transformation)(tree)(tree))
          }
        }

      def break[A <: HList, I <: HList, S <: HList]
        (attr: Attr[A, I, S])(template: (Tree, A, I, S) =?> Tree)
        : Transform[A, I, S] = new Transform[A, I, S](attr, template) {
          override final def transform(tree: Tree): Tree = {
            accumulate(tree)
            transformation.applyOrElse(tree, super.transform)
          }
        }
    }

    object bottomUp {

      def apply[A <: HList, I <: HList, S <: HList]
        (attr: Attr[A, I, S])(template: (Tree, A, I, S) =?> Tree)
        : Transform[A, I, S] = new Transform[A, I, S](attr, template) {
          override final def transform(tree: Tree): Tree = {
            val recur = super.transform(tree)
            accumulate(recur)
            complete(transformation)(recur)(recur)
          }
        }

      def break[A <: HList, I <: HList, S <: HList]
        (attr: Attr[A, I, S])(template: (Tree, A, I, S) =?> Tree)
        : Transform[A, I, S] = new Transform[A, I, S](attr, template) {
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

  object Traversal {

    object topDown {

      def apply[A <: HList, I <: HList, S <: HList, U]
        (attr: Attr[A, I, S])(callback: (Tree, A, I, S) =?> U)
        : Traversal[A, I, S] = new Traversal[A, I, S](attr, callback) {
          override final def traverse(tree: Tree): Unit = {
            accumulate(tree)
            traversal.applyOrElse(tree, unit[Tree])
            super.traverse(tree)
          }
        }

      def break[A <: HList, I <: HList, S <: HList, U]
        (attr: Attr[A, I, S])(callback: (Tree, A, I, S) =?> U)
        : Traversal[A, I, S] = new Traversal[A, I, S](attr, callback) {
          override final def traverse(tree: Tree): Unit = {
            accumulate(tree)
            traversal.applyOrElse(tree, super.traverse)
          }
        }
    }

    object bottomUp {

      def apply[A <: HList, I <: HList, S <: HList, U]
        (attr: Attr[A, I, S])(callback: (Tree, A, I, S) =?> U)
        : Traversal[A, I, S] = new Traversal[A, I, S](attr, callback) {
          override final def traverse(tree: Tree): Unit = {
            super.traverse(tree)
            accumulate(tree)
            traversal.applyOrElse(tree, unit[Tree])
          }
        }

      def break[A <: HList, I <: HList, S <: HList, U]
        (attr: Attr[A, I, S])(callback: (Tree, A, I, S) =?> U)
        : Traversal[A, I, S] = new Traversal[A, I, S](attr, callback) {
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

  object TopDown extends Strategy[HNil, HNil, HNil](
      Attr[HNil, HNil, HNil](),
      TransFactory.topDown) {

    object break extends Strategy[HNil, HNil, HNil](
        Attr[HNil, HNil, HNil](),
        TransFactory.topDown.break)
  }

  object BottomUp extends Strategy[HNil, HNil, HNil](
      Attr[HNil, HNil, HNil](),
      TransFactory.bottomUp) {

    object break extends Strategy[HNil, HNil, HNil](
        Attr[HNil, HNil, HNil](),
        TransFactory.bottomUp.break)
  }
}
