package eu.stratosphere.emma.codegen

package object utils {

  import scala.reflect.runtime.universe._

  var count = 0 // FIXME: use a Counter and/or move to a class to proctect against concurrency

  def newOperatorName(prefix: String): TypeName = {
    count = count + 1
    TypeName(prefix + "_" + count)
  }

  def asParam(tpe: Type): ValDef = ValDef(Modifiers(Flag.PARAM), TermName("x"), TypeTree(tpe), EmptyTree) //ValDef(Modifiers(Flag.PARAM), TermName(n.toString), TypeTree(tpe), EmptyTree)

  def saveToFile(path: String, code: Tree) = {
    val writer = new java.io.PrintWriter(path)
    try writer.write(showCode(code))
    finally writer.close()
  }

  def embbed(clazzes: List[Tree]) = {
    var i = 0

    val mapping = {
      val b = Map.newBuilder[TermName, Tree]
      for (clazz <- clazzes) {
        i = i + 1
        b += TermName(s"func_$i") -> clazz
      }
      b.result()
    }

    q"""
        object Test {

          ..${for (clazz <- clazzes) yield clazz}

          def main(args: Array[String]) {
            ..${for ((ident, q"$mods class $tpname (...$paramss) extends ..$earlydefns { ..$stats }") <- mapping) yield q"val $ident = new $tpname"}
          }

        }
    """
  }
}
