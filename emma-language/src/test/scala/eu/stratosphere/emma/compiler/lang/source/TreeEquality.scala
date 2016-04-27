package eu.stratosphere.emma.compiler.lang.source

import eu.stratosphere.emma.compiler.Compiler
import org.scalactic.Equality

/** Implicit equality for trees based on Core language equivalence. */
trait TreeEquality {

  val compiler: Compiler

  import compiler.universe._

  implicit final val coreEq = new Equality[Tree] {
    override def areEqual(a: Tree, b: Any): Boolean = b match {
      case b: Tree => compiler.Source.eq(a, b)
      case _ => false
    }
  }

}
