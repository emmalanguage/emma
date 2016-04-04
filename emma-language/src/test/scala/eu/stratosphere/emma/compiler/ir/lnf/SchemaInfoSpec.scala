package eu.stratosphere.emma.compiler.ir.lnf

import eu.stratosphere.emma.compiler.BaseCompilerSpec
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
 * A spec for schema information analysis.
 */
@RunWith(classOf[JUnitRunner])
class SchemaInfoSpec extends BaseCompilerSpec {

  import compiler.Schema.{Field, Info, MemberField, SimpleField}
  import compiler.Term
  import eu.stratosphere.emma.testschema.Marketing._

  import compiler.universe._

  def typeCheck[T](expr: Expr[T]): Tree = {
    compiler.typeCheck(expr.tree)
  }

  def symbolOf(name: String)(tree: Tree): Symbol = tree.collect {
    case vd@ValDef(_, TermName(`name`), _, _) => vd.symbol
  }.head

  "local schema" - {
    "without control flow" in {
      // ANF representation with `desugared` comprehensinos
      val fn = typeCheck(reify {
        (c: Click) => {
          val t = c.time
          val p = t.plusSeconds(600L)
          val m = t.minusSeconds(600L)
          val a = (c, p, m)
          a
        }
      }).asInstanceOf[Function]

      // construct expected local schema

      // 1) get schema fields
      val fld$c /*       */ = SimpleField(symbolOf("c")(fn))
      val fld$c$adID /*  */ = MemberField(fld$c.symbol, Term.member(fld$c.symbol, TermName("adID")))
      val fld$c$userID /**/ = MemberField(fld$c.symbol, Term.member(fld$c.symbol, TermName("userID")))
      val fld$c$time /*  */ = MemberField(fld$c.symbol, Term.member(fld$c.symbol, TermName("time")))

      val fld$t /*       */ = SimpleField(symbolOf("t")(fn))
      val fld$p /*       */ = SimpleField(symbolOf("p")(fn))
      val fld$m /*       */ = SimpleField(symbolOf("m")(fn))

      val fld$a /*       */ = SimpleField(symbolOf("a")(fn))
      val fld$a$_1 /*    */ = MemberField(fld$a.symbol, Term.member(fld$a.symbol, TermName("_1")))
      val fld$a$_2 /*    */ = MemberField(fld$a.symbol, Term.member(fld$a.symbol, TermName("_2")))
      val fld$a$_3 /*    */ = MemberField(fld$a.symbol, Term.member(fld$a.symbol, TermName("_3")))

      // 2) construct equivalence classes of fields
      val cls$01 /*      */ = Set[Field](fld$c, fld$a$_1)
      val cls$02 /*      */ = Set[Field](fld$c$adID)
      val cls$03 /*      */ = Set[Field](fld$c$userID)
      val cls$04 /*      */ = Set[Field](fld$c$time, fld$t)
      val cls$05 /*      */ = Set[Field](fld$p, fld$a$_2)
      val cls$06 /*      */ = Set[Field](fld$m, fld$a$_3)
      val cls$v7 /*      */ = Set[Field](fld$a)

      // 3) construct the expected local schema information
      val exp = Info(Set(cls$01, cls$02, cls$03, cls$01, cls$01, cls$01, cls$01))

      // compute actual local schema
      val act = compiler.Schema.local(fn)

      // match the two
      act shouldBe exp
    }


    "with control flow" in {
      // TODO
    }
  }
}
