package eu.stratosphere.emma.compiler.ir.lnf

import eu.stratosphere.emma.compiler.ir.CommonIR

trait SchemaOptimizations extends CommonIR {
  self: Language =>

  import universe._

  // ---------------------------------------------------------------------------
  // Schema Analysis & Optimizations
  // ---------------------------------------------------------------------------

  object Schema {

    /** Root trait for all fields. */
    sealed trait Field

    /** A simple field representing a ValDef name. */
    case class SimpleField(symbol: Symbol) extends Field

    /** A field representing a member selection. */
    case class MemberField(symbol: Symbol, member: Symbol) extends Field

    /** A class of equivalent fields. */
    type FieldClass = Set[Field]

    /** Fields can be either */
    case class Info(fieldClasses: Set[FieldClass])

    /**
     * Compute the (local) schema information for a tree fragment.
     *
     * @param tree The ANF [[Tree]] to be analyzed.
     * @return The schema information for the input tree.
     */
    private[emma] def global(tree: Tree): Schema.Info = {
      Info(Set.empty[FieldClass])
    }

    /**
     * Compute the (global) schema information for an anonymous function.
     *
     * @param tree The ANF [[Tree]] to be analyzed.
     * @return The schema information for the input tree.
     */
    private[emma] def local(tree: Function): Schema.Info = {
      Info(Set.empty[FieldClass])
    }
  }

}
