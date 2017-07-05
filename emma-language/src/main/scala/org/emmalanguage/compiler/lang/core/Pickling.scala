/*
 * Copyright © 2014 TU Berlin (emma@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.emmalanguage
package compiler.lang.core

import compiler.Common
import compiler.lang.source.Source

import scala.Function.const

/** Core language pickling. */
private[core] trait Pickling extends Common {
  self: Core with Source =>

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
        val empty: D = const("")

        // Atomics

        def lit(value: Any) = const(value match {
          //@formatter:off
          case value: Int    => value.toString
          case value: Long   => s"${value}L"
          case value: Float  => s"${value}F"
          case value: Char   => s"'$value'"
          case value: String => s""""${escape(value)}""""
          case null          => "null"
          case _             => value.toString
          //@formatter:on
        })

        def this_(sym: u.Symbol) =
          const(s"${printSym(sym)}.this")

        def ref(target: u.TermSymbol) =
          const(printSym(target))

        // Definitions

        def bindingDef(lhs: u.TermSymbol, rhs: D) = ???

        override def valDef(lhs: u.TermSymbol, rhs: D) = offset =>
          s"val ${printSym(lhs)} = ${rhs(offset)}"

        override def parDef(lhs: u.TermSymbol, rhs: D) =
          const(s"${printSym(lhs)}: ${printTpe(lhs.info)}")

        def defDef(sym: u.MethodSymbol,
          tparams: Seq[u.TypeSymbol], paramss: Seq[Seq[D]], body: D
        ) = offset => {
          val tparamsStr = tparams match {
            case Nil => (tparams map printSym).mkString(", ")
            case _ => ""
          }
          val paramssStr = printParamss(paramss, offset)
          val bodyStr = body(offset)
          s"def ${printSym(sym)}$tparamsStr$paramssStr: ${printTpe(sym.returnType)} = $bodyStr"
        }

        // Other

        def typeAscr(target: D, tpe: u.Type) = offset =>
          s"${target(offset)}: ${printTpe(tpe)}"

        def termAcc(target: D, member: u.TermSymbol) = offset =>
          s"${target(offset)}.${printSym(member)}"

        def defCall(target: Option[D], method: u.MethodSymbol,
          targs: Seq[u.Type], argss: Seq[Seq[D]]
        ) = offset => (target, argss) match {
          case (Some(tgt), ((arg :: Nil) :: Nil)) if isApply(method) =>
            s"${tgt(offset)}(${arg(offset)})"
          case (Some(tgt), ((arg :: Nil) :: Nil)) =>
            s"${tgt(offset)}${printMethod(" ", method, " ")}${arg(offset)}"
          case (Some(tgt), Nil | (Nil :: Nil)) if targs.nonEmpty =>
            s"${tgt(offset)}${printMethod(".", method, "")}[${(targs map printTpe).mkString}]"
          case (Some(tgt), Nil) if isUnary(method) =>
            s"${printSym(method)}${tgt(offset)}"
          case (Some(_), _) if isApply(method) && api.Sym.tuples.contains(method.owner.companion) =>
            s"${printArgss(argss, offset)}"
          case (Some(tgt), _) =>
            s"${tgt(offset)}${printMethod(".", method, "")}${printArgss(argss, offset)}"
          case (None, Nil | (Nil :: Nil)) if targs.nonEmpty =>
            s"${printSym(method)}[${(targs map printTpe).mkString}]"
          case (None, _) =>
            s"${printSym(method)}${printArgss(argss, offset)}"
        }

        def inst(target: u.Type, targs: Seq[u.Type], argss: Seq[Seq[D]]) = offset => argss match {
          case Nil | (Nil :: Nil) if targs.nonEmpty =>
            s"new ${printSym(target.typeSymbol)}[${(targs map printTpe).mkString(", ")}]${printArgss(argss, offset)}"
          case _ =>
            s"new ${printSym(target.typeSymbol)}${printArgss(argss, offset)}"
        }

        def lambda(sym: u.TermSymbol, params: Seq[D], body: D) = offset =>
          s"${printParams(params, offset + indent)} => ${body(offset)}"

        def branch(cond: D, thn: D, els: D) = offset =>
          s"if (${cond(offset + indent)}) ${thn(offset + indent)} else ${els(offset + indent)}"

        def let(vals: Seq[D], defs: Seq[D], expr: D) = offset => {
          s"""{
            |${(vals map (val_ => (" " * (offset + indent)) + val_(offset + indent))).mkString("\n")}
            |${" " * (offset + indent)}${expr(offset + indent)}
            |${" " * offset}}
            |""".stripMargin.trim

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
        def comprehend(qs: Seq[D], hd: D) = offset =>
          s"""for {
            |${(qs map (q => (" " * (offset + indent)) + q(offset + indent))).mkString("\n")}
            |${" " * offset}} yield ${hd(offset)}
            |""".stripMargin.trim

        def generator(lhs: u.TermSymbol, rhs: D) = offset =>
          s"${printSym(lhs)} <- ${rhs(offset)}"

        def guard(expr: D) = offset =>
          s"if ${expr(offset)}"

        def head(expr: D) = offset =>
          expr(offset)
      }

      Core.fold(alg)(tree)(0)
    }
  }

}
