package eu.stratosphere.emma
package compiler

/** Default macro extensions for trees, types and symbols. */
trait ReflectUtil extends Util
  with Trees with Terms with Types with Symbols {

  import universe._
  import Tree._

  /** Emma-specific utility. */
  object Emma {

    // Predefined trees
    lazy val emma = q"$Root.eu.stratosphere.emma"
    lazy val DataBag = Type.check(q"$emma.api.DataBag")
    lazy val Group = Type.check(q"$emma.api.Group")
  }

  /** Extractor for the last element of a [[Seq]]. */
  // scalastyle:off
  object :+ {
    // scalastyle:on

    def unapply[A](seq: Seq[A]): Option[(Seq[A], A)] =
      if (seq.isEmpty) None
      else Some(seq.init, seq.last)
  }

  /** Print tree for debuging. */
  def printTree(title: String): Tree => Tree = (tree: Tree) => {
    // prefix
    println(title)
    println("-" * 80)
    // tree
    println(showCode(tree)
      .replace("<synthetic> ", "")
      .replace("_root_.", "")
      .replace("eu.stratosphere.emma.api.", "")
      .replace("eu.stratosphere.emma.compiler.ir.`package`.", "")
      .replaceAll("eu\\.stratosphere\\.emma\\.testschema\\.([a-zA-Z]+)\\.?", ""))
    // suffix
    println("-" * 80)
    // don't  break the chain
    tree
  }
}
