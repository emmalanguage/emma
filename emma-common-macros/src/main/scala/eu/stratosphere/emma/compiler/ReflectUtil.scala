package eu.stratosphere
package emma.compiler

/** Default macro extensions for trees, types and symbols. */
trait ReflectUtil extends Util with Trees with Types with Symbols {

  import universe._
  import Tree._

  /** Emma-specific utility. */
  object Emma {

    // Predefined trees
    lazy val emma = q"$Root.eu.stratosphere.emma"
    lazy val DataBag = Type.check(q"$emma.api.DataBag")
    lazy val Group = Type.check(q"$emma.api.Group")
  }
}
