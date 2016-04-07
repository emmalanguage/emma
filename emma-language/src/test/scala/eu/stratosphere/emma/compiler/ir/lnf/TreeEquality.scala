package eu.stratosphere.emma.compiler.ir.lnf

import eu.stratosphere.emma.compiler.Compiler
import org.scalactic.Equality

/**
 * Provides implicit equality for trees based on ANF language equivalence.
 */
trait TreeEquality {

  val compiler: Compiler

  import compiler.universe._

  implicit final val lnfEq = new Equality[Tree] {
    override def areEqual(a: Tree, b: Any): Boolean = b match {
      case b: Tree => compiler.LNF.eq(a, b)
      case _ => false
    }
  }

}
