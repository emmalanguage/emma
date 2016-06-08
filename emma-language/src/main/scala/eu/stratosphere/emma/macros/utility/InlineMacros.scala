package eu.stratosphere.emma.macros.utility

import eu.stratosphere.emma.api.Inlined
import eu.stratosphere.emma.compiler.MacroUtil
import scala.language.experimental.macros
import scala.reflect.macros.whitebox

/**
  * The @emma.inline annotation macro
  *
  * @param c a whitebox context
  */
class InlineMacros(val c: whitebox.Context) extends MacroUtil {
  import universe._
  private val inlineBlacklist = Set("<init>") // do not inline these functions

  /** The implementation of the @emma.inline macro.
    *
    * 1. If the annottee is an object, all methods on that object will be extended
    * 2. If the annottee is a method, only the single method will be extended
    *
    * @param annottees a list with (currently) exactly one element (only objects and methods can be currently annotated)
    * @return the resulting tree with the replacement
    */
  def inlineImpl(annottees: Expr[Any]*):Expr[Any] = {
    def mkInlinedFunName(name: String) = Term.name.fresh(s"${name}__expr").toString
    def extendFunction: Tree =?> List[Tree] = {
      case df @ q"""def ${tn @ TermName(defName)}[..$types](...${args: List[List[ValDef]]}):$rt = $body"""
        if !inlineBlacklist.contains(defName) && // TODO find solution, does not work /w symbols
          Symbol.annotation[Inlined](df.symbol).isEmpty =>
        val argsAndBody = args.foldRight(body)(Function(_, _))
        val newName = mkInlinedFunName(defName)
        val annot = q"""new Inlined(forwardTo=$newName)"""
        val oldFun = q"@$annot def $tn[..$types](...$args): $rt = $body"
        val uniType = tq"scala.reflect.api.Universe"
        val newFun = q"def ${TermName(newName)}[..$types](u: $uniType) = u.reify({$argsAndBody})"
        oldFun :: newFun :: Nil
    }
    def extendAllFunctions: Tree =?> Tree = {
      case md @ ModuleDef(modifiers, tn, Template(parents, self, body)) =>
        val newBody = body.flatMap(extendFunction orElse { case x => x :: Nil } )
        ModuleDef(modifiers, tn, Template(parents, self, newBody))
    }
    val preamble = q"import eu.stratosphere.emma.api.Inlined"

    val expandFunction: Tree =?> Tree = extendFunction andThen { t =>
      q"""
         ..$preamble
         ..$t
        """
    }
    val extendModule: Tree =?> Tree = extendAllFunctions andThen { t =>
      q"""
         ..$preamble
         $t
       """
    }
    val warn: Tree =?> Tree = { case x: Tree =>
      c.warning(c.enclosingPosition,
        "@emma.inline can only be applied to methods or modules, skipping this")
      x
    }
    val annottee :: _ = annottees
    c.Expr((
      expandFunction orElse
      extendModule orElse
      warn) (annottee.tree))
  }
}
