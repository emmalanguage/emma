//package eu.stratosphere.emma
//
//import scala.reflect.runtime.universe._
//
///**
// *
// */
//package object codegen {
//
//  var count = 0
//  def newName(prefix: String): TypeName = {
//    count = count + 1
//    TypeName(prefix + "_" + count)
//  }
//
//  def toParam(block: Expr[Any]): ValDef = {
//    // has to be a block
//    // type is lost in runtime
//    block.tree match {
//      case Block(List(ValDef(m, n, t, _)), _) => ValDef(Modifiers(Flag.PARAM), TermName(n.toString), TypeTree(block.staticType), EmptyTree)
//    }
//  }
//
//  def saveToFile(path: String, code: Tree) = {
//    val writer = new java.io.PrintWriter(path)
//    try writer.write(showCode(code))
//    finally writer.close()
//  }
//}
