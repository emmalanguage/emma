package eu.stratosphere.emma
package compiler.util

import compiler.BaseCompilerSpec

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalactic.Equality
import scala.annotation.tailrec

/** A spec defining the core fragment of Scala supported by Emma. */
@RunWith(classOf[JUnitRunner])
class ResolveSpec extends BaseCompilerSpec {

  import compiler._
  import universe._
  import Term._

  "instantiations" in {
    val examples = typeCheck(reify {
      new org.example.Foo.Bar.Baz(1)
      new org.example.foo.Bar.Baz(2)
      new org.example.foo.Bar(3)
      new org.example.foo.Baz(4)
    }).collect({
      case app(sel(New(clazz: Select), _), _, _@_*) => clazz
    })

    for (exp <- examples) {
      val sym = Type sym exp
      val act = Tree.resolve(sym)
      act shouldEqual exp
    }
  }

  "function applications" in {
    val examples = typeCheck(reify {
      org.example.Foo.Bar.Baz(1)
      org.example.foo.Bar.Baz(2)
      org.example.foo.Bar(3)
      org.example.foo.Baz(4)
    }).collect({
      case app(fn: Select, _, _@_*) => fn
    })

    for (exp <- examples) {
      val sym = Term sym exp
      val act = Tree.resolve(sym)
      act shouldEqual exp
    }
  }

  "object selections" in {
    val examples = (typeCheck(reify {
      org.example.Foo.Bar.Baz
      org.example.foo.Bar.Baz
      org.example.foo.Bar
      org.example.foo.Baz
      ()
    }) match {
      case u.Block(stats, expr: Tree) => stats
      case _ => Nil
    }) collect {
      case sel: Select => sel
    }

    for (exp <- examples) {
      val sym = Term sym exp
      val act = Tree.resolve(sym)
      act shouldEqual exp
    }
  }

  "imports" in {
    val examples = (typeCheck(reify {
      import org.example.foo.Bar._
      import org.example.foo.Baz
      val bar2 = Baz
      apply(1).x == bar2(1).x
    }) match {
      case u.Block(stats, expr: Tree) => stats
      case _ => Nil
    }) collect {
      case Import(tree, selectors) => tree
    }


    for (exp <- examples) {
      val sym = Term sym exp
      val act = Tree.resolve(sym)
      act shouldEqual exp
    }
  }

  // ---------------------------------------------------------------------------
  // Custom, symbol-chain based equality for selection chains
  // ---------------------------------------------------------------------------

  implicit final val selEq = new Equality[Tree] {
    override def areEqual(a: Tree, b: Any): Boolean = b match {
      case b: Tree =>
        val sympairs = syms(a).reverse zip syms(b).reverse
        if (sympairs.nonEmpty) sympairs forall { case (l, r) => l == r }
        else a equalsStructure b
      case _ =>
        false
    }
  }

  @tailrec
  private def syms(t: Tree, acc: List[Symbol] = Nil): List[Symbol] = t match {
    case sel@Select(qual, _) => syms(qual, sel.symbol :: acc)
    case id: Ident => id.symbol :: acc
  }

  // ---------------------------------------------------------------------------
  // Utility methods
  // ---------------------------------------------------------------------------

  def ownerChainInfo(target: Symbol): String = {

    val sb = StringBuilder.newBuilder

    val cellLength = 23

    sb.append("-" * (cellLength * 10 + 9) + "\n")
    sb.append(formatCell("symbol", cellLength, -1) + "|")
    sb.append(formatCell("isStatic", cellLength) + "|")
    sb.append(formatCell("isType", cellLength) + "|")
    sb.append(formatCell("hasPackageFlag", cellLength) + "|")
    sb.append(formatCell("isModule", cellLength) + "|")
    sb.append(formatCell("isModuleClass", cellLength) + "|")
    sb.append(formatCell("isClass", cellLength) + "|")
    sb.append(formatCell("isPackageClass", cellLength) + "|")
    sb.append(formatCell("isPackageObject", cellLength) + "|")
    sb.append(formatCell("isPackageObjectClass", cellLength) + "\n")
    sb.append("-" * (cellLength * 10 + 9) + "\n")

    for (s <- Owner.chain(target)) {

      sb.append(formatCell(s.decodedName, cellLength, -1) + "|")
      sb.append(formatCell(if (s.isStatic) "☑" else "☐", cellLength) + "|")
      sb.append(formatCell(if (s.isType) "☑" else "☐", cellLength) + "|")
      sb.append(formatCell(if (s.hasPackageFlag) "☑" else "☐", cellLength) + "|")
      sb.append(formatCell(if (s.isModule) "☑" else "☐", cellLength) + "|")
      sb.append(formatCell(if (s.isModuleClass) "☑" else "☐", cellLength) + "|")
      sb.append(formatCell(if (s.isClass) "☑" else "☐", cellLength) + "|")
      sb.append(formatCell(if (s.isPackageClass) "☑" else "☐", cellLength) + "|")
      sb.append(formatCell(if (s.isPackageObject) "☑" else "☐", cellLength) + "|")
      sb.append(formatCell(if (s.isPackageObjectClass) "☑" else "☐", cellLength) + "\n")

//      val z = s.module
//      if (z != NoSymbol) {
//        val s = z
//        sb.append(formatCell(s.decodedName + " (module)", cellLength, -1) + "|")
//        sb.append(formatCell(if (s.isStatic) "☑" else "☐", cellLength) + "|")
//        sb.append(formatCell(if (s.isType) "☑" else "☐", cellLength) + "|")
//        sb.append(formatCell(if (s.hasPackageFlag) "☑" else "☐", cellLength) + "|")
//        sb.append(formatCell(if (s.isModule) "☑" else "☐", cellLength) + "|")
//        sb.append(formatCell(if (s.isModuleClass) "☑" else "☐", cellLength) + "|")
//        sb.append(formatCell(if (s.isClass) "☑" else "☐", cellLength) + "|")
//        sb.append(formatCell(if (s.isPackageClass) "☑" else "☐", cellLength) + "|")
//        sb.append(formatCell(if (s.isPackageObject) "☑" else "☐", cellLength) + "|")
//        sb.append(formatCell(if (s.isPackageObjectClass) "☑" else "☐", cellLength) + "\n")
//      }
    }

    sb.append("-" * (cellLength * 10 + 9) + "\n")
    sb.append("\n")

    sb.toString()
  }

  private def formatCell(v: String, len: Int, centering: Int = 0) = {
    val space = len - v.length
    if (space > 0) {
      if (centering > 0) {
        val pre = " " * space.toInt
        s"$pre$v"
      } else if (centering < 0) {
        val suf = " " * space.toInt
        s"$v$suf"
      } else {
        val pre = " " * Math.floor(space / 2.0).toInt
        val suf = " " * Math.ceil(space / 2.0).toInt
        s"$pre$v$suf"
      }
    } else {
      v.substring(0, len)
    }
  }
}
