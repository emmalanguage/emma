package eu.stratosphere.emma
package compiler.lang.core

import compiler.Common
import compiler.lang.source.Source


/** Core language pickling. */
private[core] trait Pickling extends Common {
  this: Source with Core =>

  import Core.{Lang => core}
  import UniverseImplicits._

  private[core] object Pickle {

    type D = Int => String // semantic domain (offset => string representation)
    val indent = 2

    val prettyPrint: u.Tree => String  = (tree: u.Tree) => {

      val kws = Set(
        "abstract", "case", "catch", "class", "def",
        "do", "else", "extends", "false", "final", "finally",
        "for", "if", "implicit", "import", "match", "mixin",
        "new", "null", "object", "override", "package",
        "private", "protected", "requires", "return", "sealed",
        "super", "this", "throw", "trait", "true", "try",
        "type", "val", "var", "while", "with", "yield"
      )
      val ops = Set('=', '+', '-', '*', '/', '%', '<', '>', '&', '|', '!', '?', '^', '\\', '@', '#', '~')

      val printSym = (sym: u.Symbol) => {
        val decName = sym.name.decodedName.toString.stripPrefix("unary_")
        val encName = sym.name.encodedName.toString.stripPrefix("unary_")
        if (decName == encName && !kws.contains(decName) || sym.isMethod && decName.forall(ops.contains)) decName
        else s"`$decName`"
      }

      val isApply = (sym: u.MethodSymbol) =>
        sym.name == u.TermName("apply")

      val printMethod = (pre: String, sym: u.MethodSymbol, suf: String) =>
        if (isApply(sym)) ""
        else pre + printSym(sym) + suf

      val isUnary = (sym: u.MethodSymbol) =>
        sym.name.decodedName.toString.startsWith("unary_")

      val printArgss = (argss: Seq[Seq[D]], offset: Int) =>
        (argss map (args => (args map (arg => arg(offset))).mkString("(", ", ", ")"))).mkString

      val printParams = (params: Seq[D], offset: Int) =>
        (params map (param => param(offset))).mkString("(", ", ", ")")

      val printParamss = (paramss: Seq[Seq[D]], offset: Int) =>
        (paramss map (params => printParams(params, offset))).mkString

      def printTpe(tpe: u.Type): String = {
        val tpeCons = tpe.typeConstructor
        val tpeArgs = tpe.typeArgs
        if (api.Sym.tuples contains tpeCons.typeSymbol) /* tuple type */ {
          (tpe.typeArgs map printTpe).mkString("(", ", ", ")")
        } else if (api.Sym.fun(Math.max(0, tpeArgs.size - 1)) == tpeCons.typeSymbol) /* function type */ {
          s"(${(tpe.typeArgs.init map printTpe).mkString(", ")}) => ${printTpe(tpe.typeArgs.last)}"
        } else if (tpeArgs.nonEmpty) /* applied higher-order type */ {
          s"${printSym(tpeCons.typeSymbol)}[${(tpe.typeArgs map printTpe).mkString(", ")}]"
        } else /* simple type */ {
          printSym(tpeCons.typeSymbol)
        }
      }

      val escape = (str: String) => str
        .replace("\b", "\\b")
        .replace("\n","\\n")
        .replace("\t", "\\t")
        .replace("\r","\\r")
        .replace("\f", "\\f")
        .replace("\"","\\\"")
        .replace("\\","\\\\")

      val alg = new Core.Algebra[D] {

        // Empty
        def empty: D = offset => ""

        // Atomics
        def lit(value: Any): D = offset => value match {
          //@formatter:off
          case value: Int    => value.toString
          case value: Long   => s"${value}L"
          case value: Float  => s"${value}F"
          case value: Char   => s"'$value'"
          case value: String => s""""${escape(value)}""""
          case null          => "null"
          case _             => value.toString
          //@formatter:on
        }

        def this_(sym: u.Symbol): D = offset =>
          s"${printSym(sym)}.this"

        def bindingRef(sym: u.TermSymbol): D = offset =>
          printSym(sym)

        def moduleRef(target: u.ModuleSymbol): D = offset =>
          printSym(target)

        // Definitions
        def valDef(lhs: u.TermSymbol, rhs: D, flags: u.FlagSet): D = offset =>
          s"val ${printSym(lhs)} = ${rhs(offset)}"

        def parDef(lhs: u.TermSymbol, rhs: D, flags: u.FlagSet): D = offset =>
          s"${printSym(lhs)}: ${printTpe(lhs.info)}"

        def defDef(sym: u.MethodSymbol, flags: u.FlagSet, tparams: S[u.TypeSymbol], paramss: SS[D], body: D): D =
          offset => {
            val tparamsStr = tparams match {
              case Nil => (tparams map printSym).mkString(", ")
              case _ => ""
            }
            val paramssStr = printParamss(paramss, offset)
            val bodyStr = body(offset)
            s"def ${printSym(sym)}$tparamsStr$paramssStr: ${printTpe(sym.returnType)} = $bodyStr"
          }

        // Other
        def typeAscr(target: D, tpe: u.Type): D = offset =>
          s"${target(offset)}: ${printTpe(tpe)}"

        def defCall(target: Option[D], method: u.MethodSymbol, targs: S[u.Type], argss: SS[D]): D = offset => {
          val s = target
          (target, argss) match {
            case (Some(tgt), ((arg :: Nil) :: Nil)) if isApply(method) =>
              s"${tgt(offset)}(${arg(offset)})"
            case (Some(tgt), ((arg :: Nil) :: Nil)) =>
              s"${tgt(offset)}${printMethod(" ", method, " ")}${arg(offset)}"
            case (Some(tgt), Nil | (Nil :: Nil)) if targs.nonEmpty =>
              s"${tgt(offset)}${printMethod(".", method, "")}[${(targs map printTpe).mkString}]"
            case (Some(tgt), Nil) if isUnary(method) =>
              s"${printSym(method)}${tgt(offset)}"
            case (Some(tgt), _) if isApply(method) && api.Sym.tuples.contains(method.owner.companion) =>
              s"${printArgss(argss, offset)}"
            case (Some(tgt), _) =>
              s"${tgt(offset)}${printMethod(".", method, "")}${printArgss(argss, offset)}"
            case (None, Nil | (Nil :: Nil)) if targs.nonEmpty =>
              s"${printSym(method)}[${(targs map printTpe).mkString}]"
            case (None, _) =>
              s"${printSym(method)}${printArgss(argss, offset)}"
          }
        }

        def inst(target: u.Type, targs: Seq[u.Type], argss: SS[D]): D = offset => argss match {
          case Nil | (Nil :: Nil) if targs.nonEmpty =>
            s"new ${printSym(target.typeSymbol)}[${(targs map printTpe).mkString(", ")}]${printArgss(argss, offset)}"
          case _ =>
            s"new ${printSym(target.typeSymbol)}${printArgss(argss, offset)}"
        }

        def lambda(sym: u.TermSymbol, params: S[D], body: D): D = offset =>
          s"${printParams(params, offset + indent)} => ${body(offset)}"

        def branch(cond: D, thn: D, els: D): D = offset =>
          s"if (${cond(offset + indent)}) ${thn(offset + indent)} else ${els(offset + indent)}"

        def let(vals: S[D], defs: S[D], expr: D): D = offset => {
          s"""{
              |${(vals map (val_ => (" " * (offset + indent)) + val_(offset + indent))).mkString("\n")}
              |${" " * (offset + indent)}${expr(offset + indent)}
              |${" " * offset}}
           """.stripMargin.trim

          val valsStr = (vals map (val_ => (" " * (offset + indent)) + val_(offset + indent))).mkString("\n")
          val defsStr = (defs map (def_ => (" " * (offset + indent)) + def_(offset + indent))).mkString("\n")
          Seq(
            "{",
            valsStr,
            defsStr,
            " " * (offset + indent) + expr(offset + indent),
            " " * offset + "}").filter(_.trim != "").mkString("\n")
        }

        // Comprehensions
        def comprehend(qs: S[D], hd: D): D = offset =>
          s"""for {
             |${(qs map (q => (" " * (offset + indent)) + q(offset + indent))).mkString("\n")}
             |${" " * offset}} yield ${hd(offset)}
           """.stripMargin.trim

        def generator(lhs: u.TermSymbol, rhs: D): D = offset =>
          s"${printSym(lhs)} <- ${rhs(offset)}"

        def guard(expr: D): D = offset =>
          s"if ${expr(offset)}"

        def head(expr: D): D = offset =>
          expr(offset)

        def flatten(expr: D): D = offset =>
          s"(${expr(offset)}).flatten"
      }

      Core.fold(alg)(tree)(0)
    }
  }

}
