package eu.stratosphere.emma.macros

import scala.reflect.api.Universe

/**
 * Implements various utility functions that mitigate and/or workaround
 * deficiencies in Scala's macros and runtime reflection APIs, e.g. non-
 * idempotent type checking, lack of hygiene, capture-avoiding substitution,
 * fully-qualified names, fresh name generation, identifying closures, etc.
 * 
 * This trait has to be instantiated with a [[Universe]] type and works both
 * for runtime and compile time reflection.
 */
trait ReflectUtil {
  val universe: Universe
  import universe._
  import ReflectUtil._

  def parse    (s: String): Tree
  def typeCheck(t: Tree  ): Tree

  val freshName   = internal.reificationSupport.freshTermName _
  val freshType   = internal.reificationSupport.freshTypeName _

  // FIXME: Replace with c.untypecheck once https://issues.scala-lang.org/browse/SI-5464 is resolved.
  val unTypeCheck = (t: Tree  ) => showCode(t, printRootPkg = true) ->> parse
  val reTypeCheck = (t: Tree  ) => t ->> unTypeCheck ->> typeCheck
  val  parseCheck = (s: String) => s ->> parse       ->> typeCheck

  def closure(tree: Tree) = {
    // find all term symbols referenced within the comprehended term
    val referenced = tree collect {
      case id@Ident(TermName(_)) if id.symbol != NoSymbol &&
          (id.symbol.asTerm.isVal || id.symbol.asTerm.isVar) =>
        id.symbol.asTerm
    }

    // find all term symbols bound within the comprehended term
    val bound = tree collect {
      case vd: ValDef => vd.symbol.asTerm
      case bind: Bind => bind.symbol.asTerm
    }

    // the closure is the diff between the referenced and the bound symbols
    referenced.toSet diff bound.toSet
  }
  
  def bind(in: Tree, key: TermName, value: Tree): Tree =
    bind(in, key -> value)

  def bind(in: Tree, map: Map[TermName, Tree]): Tree =
    bind(in, map.toSeq: _*)

  def bind(in: Tree, kvs: (TermName, Tree)*): Tree =
    q"{ ..${for ((k, v) <- kvs) yield q"val $k = $v"}; $in }"

  def substitute(in: Tree, key: TermName, value: Tree): Tree =
    substitute(in, Map(key -> value))

  def substitute(in: Tree, map: Map[TermName, Tree]): Tree = new Transformer {
    override def transform(tree: Tree): Tree = tree match {
      case Typed(Ident(name: TermName), _) if map contains name => map(name)
      case       Ident(name: TermName)     if map contains name => map(name)
      case _ => super.transform(tree)
    }
  } transform in

  def inline(in: Tree, valDef: ValDef) = new Transformer {
    override def transform(tree: Tree): Tree = tree match {
      case vd: ValDef if vd.symbol == valDef.symbol => EmptyTree
      case id: Ident  if id.symbol == valDef.symbol => valDef.rhs
      case _ => super.transform(tree)
    }
  } transform in
}

object ReflectUtil {

  /**
   * Syntactic sugar for emulating chaining of function calls similar to
   * Clojure's thread macros (-> and/or ->>).
   *
   * E.g. `x ->> f ->> g =:= g(f(x)) =:= (g compose f)(x)`
   *
   * @param self Initial parameter to thread.
   * @tparam A Type of the initial parameter.
   */
  implicit class ApplyWith[A](val self: A) extends AnyVal {
    def ->>[B](f: A => B) = f(self)
  }
}
