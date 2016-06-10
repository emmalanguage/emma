package eu.stratosphere.emma.compiler.lang

import eu.stratosphere.emma.compiler.Common
import eu.stratosphere.emma.compiler.lang.core.Core

/** Provides pretty printing Scala ASTs. */
trait PrettyPrint extends Common with Core {

  import universe._
  import Core.Language._

  def prettyPrintParam(param : ValDef) : String = {
    param.name.toString() + " : " + showCode(param.tpt)
  }

  def prettyPrint(tree: Tree, indent: Int = 0): String = {
    val ret = tree match {
      case l@lit(_) => l toString
      case r@ref(_) => r toString
      case qref(qual, termSym) => prettyPrint(qual) + "." + termSym.toString
      case sel(qual, member) => prettyPrint(qual) + "." + member.toString
      case app(name, targs, args@_*) => name.toString +
        targs.map(_ toString).mkString("[", ", ", "]").replace("[]", "") +
        args.map(_.map(prettyPrint(_).mkString("(", ", ", ")"))).mkString("")
      case call(target, name, targs, args@_*) => prettyPrint(target) + "." + name.name +
        targs.map(_ toString).mkString("[", ", ", "]").replace("[]", "") +
        args.map(_.map(prettyPrint(_).mkString("(", ", ", ")"))).mkString("")
      case inst(t, targs, args@_*) => "new " + t.toString +
        targs.map(_ toString).mkString("[", ", ", "]").replace("[]", "") +
        args.map(_.map(prettyPrint(_).mkString("(", ", ", ")"))).mkString("")
      case lambda(_, argSyms, body) => argSyms.map(_ toString).mkString("(", ", ", ")") + " => " +
        prettyPrint(body)
      case typed(expr, t) => prettyPrint(expr) + " : " + t.toString
      case val_(lhs, rhs, _) => "val " + lhs.name + " = " + prettyPrint(rhs)
      case let(valDefs, defDefs, expr) =>
        s"""{
${valDefs.map(prettyPrint(_, indent + 2)).mkString("\n")}
${defDefs.map(prettyPrint(_, indent + 2)).mkString("\n")}
${prettyPrint(expr, indent + 2)}
}"""
      case if_(cond, then$, else$) =>
        s"""if (${prettyPrint(cond)})
${prettyPrint(then$, indent + 2)}
else
${prettyPrint(else$, indent + 2)}
"""
      case def_(name, _, args, body) => "def " + name.toString() +
        args.map(_ toString).mkString("(", ", ", ")") +
        " = " + prettyPrint(body, indent + 2)
    }
    // TODO: prepend imports to ret and replace long names.
    " " * indent + ret
  }
}
