import scala.reflect.runtime.currentMirror
import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

import internal._
import reificationSupport._

val tb = currentMirror.mkToolBox()

def tc[T <: Tree](t: T, typeMode: Boolean = false): T = {
  val mode = if (typeMode) tb.TYPEmode else tb.TERMmode
  tb.typecheck(t, mode = mode).asInstanceOf[T]
}

val empty = tc(EmptyTree)
empty.symbol
empty.tpe

val emptyTpt = tc(TypeTree())
emptyTpt.tpe
emptyTpt.symbol
emptyTpt.original

val tpt = tc(TypeTree(typeOf[Int]))
tpt.tpe
tpt.tpe.typeSymbol
tpt.symbol
tpt.original

val originTpt = tc(tq"Int", typeMode = true)
  .asInstanceOf[TypeTree]

originTpt.tpe
originTpt.tpe.typeSymbol
originTpt.symbol
originTpt.original
originTpt.original.tpe
originTpt.original.symbol

val Apply(Select(New(att: TypeTree), _), _) =
  tc(q"new scala.Tuple2[Int, Int](1, 2)")

val appTpt = att.original
  .asInstanceOf[AppliedTypeTree]

appTpt.tpe
appTpt.symbol
appTpt.tpt
appTpt.args

val lit = tc(q"42")
lit.tpe
lit.symbol

val Block(_, ref: Ident) = tc(q"val x = 42; x")
ref.tpe
ref.symbol
ref.name.isTermName

val Block(_, Apply(Select(New(tpeRef: Ident), _), _)) =
  tc(q"type A = Tuple2[Int, Int]; new A(1, 2)")

tpeRef.tpe
tpeRef.symbol
tpeRef.name.isTypeName

val Select(packSel: Select, _) =
  tc(q"scala.collection.immutable.Nil")

packSel.tpe
packSel.symbol
packSel.name.isTermName

val sel = tc(q"scala.Predef")
  .asInstanceOf[Select]

sel.tpe
sel.symbol
sel.name.isTermName

val ValDef(_, _, selTpt: TypeTree, _) =
  tc(q"val x: scala.Int = 42")

val tpeSel = selTpt.original
  .asInstanceOf[Select]

tpeSel.tpe
tpeSel.symbol
tpeSel.name.isTypeName

val fldSel = tc(q"(1, 2)._1")
  .asInstanceOf[Select]

fldSel.tpe
fldSel.symbol
fldSel.symbol.info
fldSel.name.isTermName

val Apply(methSel: Select, _) =
  tc(q"1 + (1, 2)._2")

methSel.tpe
methSel.symbol
methSel.name.isTermName

val app = tc(q"1 + (1, 2)._2")
  .asInstanceOf[Apply]

app.tpe
app.symbol
app.fun
app.args

val Apply(Apply(tpeApp: TypeApply, _), _) =
  tc(q"List(1, 2, 3).map(_ + 1)")

tpeApp.tpe
tpeApp.symbol
tpeApp.fun
tpeApp.args

val Apply(Select(inst: New, _), _) =
  tc(q"new Tuple2[Int, Int](1, 2)")

inst.tpe
inst.symbol
inst.tpt

val branch = tc(q"if (1 % 2 == 0) 'even else 'odd")
  .asInstanceOf[If]

branch.tpe
branch.symbol

val whileLoop = tc(q"while (true) println(42)")
  .asInstanceOf[LabelDef]

whileLoop.tpe
whileLoop.symbol
whileLoop.params
whileLoop.name
whileLoop.rhs

val doWhile = tc(q"do println(42) while (true)")
  .asInstanceOf[LabelDef]

doWhile.tpe
doWhile.symbol
doWhile.params
doWhile.name
doWhile.rhs

val tcf = tc(q"try 42 catch { case x => 3.0 } finally println(55)")
  .asInstanceOf[Try]

tcf.tpe
tcf.symbol
tcf.block
tcf.catches
tcf.finalizer

val thrw = tc(q"throw new RuntimeException()")
  .asInstanceOf[Throw]

thrw.tpe
thrw.symbol
thrw.expr

val valDef = tc(q"val x = 42")
  .asInstanceOf[ValDef]

valDef.tpe
valDef.symbol
valDef.mods
valDef.name
valDef.tpt
valDef.rhs

val varDef = tc(q"var x = 42")
  .asInstanceOf[ValDef]

varDef.tpe
varDef.symbol
varDef.mods
varDef.name
varDef.tpt
varDef.rhs

val Block(List(lazyVal: ValDef, lazyDef: DefDef),
  lazyRef: Ident) =
    tc(q"lazy val x = 42; x")
      .asInstanceOf[Block]

lazyVal.tpe
lazyVal.symbol
lazyVal.mods
lazyVal.name
lazyVal.tpt
lazyVal.rhs

lazyDef.tpe
lazyDef.symbol
lazyDef.symbol.info
lazyDef.mods
lazyDef.name
lazyDef.tpt
lazyDef.tparams
lazyDef.vparamss
lazyDef.rhs

lazyRef.tpe
lazyRef.symbol
lazyRef.name

val method = tc(q"def double(x: Int) = x * 2")
  .asInstanceOf[DefDef]

method.tpe
method.symbol
method.symbol.info
method.name
method.tpt
method.tparams
method.vparamss
method.rhs

val fun = tc(q"(i: Int) => i + 5")
  .asInstanceOf[Function]

fun.tpe
fun.symbol
fun.symbol.info
fun.vparams
fun.body

val patMat = tc(q"42 match { case x: Int => x }")
  .asInstanceOf[Match]

patMat.tpe
patMat.symbol
patMat.selector
patMat.cases

val Match(_, caseDef :: Nil) =
  tc(q"42 match { case x: Int if x % 2 == 0 => x }")

caseDef.tpe
caseDef.pat
caseDef.symbol
caseDef.guard
caseDef.body

val Match(_, CaseDef(bind: Bind, _, _) :: Nil) =
  tc(q"42 match { case x: Int if x % 2 == 0 => x }")

bind.tpe
bind.symbol
bind.name
bind.body

val Block(List(anonDef: ClassDef), anonRef) =
  tc(q"new Function0[Int] { def apply() = 42 }")

anonDef.tpe
anonDef.symbol
anonDef.mods
anonDef.name
anonDef.impl
anonDef.tparams

anonRef.tpe
anonRef.symbol

val classDef = tc(q"class Point(val x: Int, val y: Int)")
  .asInstanceOf[ClassDef]

classDef.tpe
classDef.symbol
classDef.mods
classDef.name
classDef.impl
classDef.tparams

val Block((caseClass: ClassDef) :: _, _) =
  tc(q"case class Point(x: Int, y: Int)")
    .asInstanceOf[Block]

caseClass.tpe
caseClass.symbol
caseClass.mods
caseClass.name
caseClass.impl
caseClass.tparams

val ClassDef(_, _, _, templ: Template) =
  tc(q"class Point(val x: Int, val y: Int)")
    .asInstanceOf[ClassDef]

templ.tpe
templ.symbol
templ.symbol.owner
templ.self
templ.parents
templ.body

val module = tc(q"object X")
  .asInstanceOf[ModuleDef]

module.tpe
module.symbol
module.mods
module.name
module.impl

val ClassDef(_, _, _,
  Template(_, _, _ :: _ :: _ :: _ :: _ ::
    Select(ths: This, _) :: _)) =
      tc(q"class Point(val x: Int, val y: Int) { this.x }")

ths.tpe
ths.symbol
ths.qual

val ClassDef(_, _, _,
  Template(_, _, _ :: _ :: _ :: _ ::
    DefDef(_, _, _, _, _,
      Block(Apply(
        Select(supr: Super, _), _) :: _, _)) :: _)) =
          tc(q"class Point(val x: Int, val y: Int)")

supr.tpe
supr.symbol
supr.mix
supr.qual

val Block(_, assign: Assign) =
  tc(q"var x = 0; x = 42")

assign.symbol
assign.tpe
assign.lhs
assign.rhs
