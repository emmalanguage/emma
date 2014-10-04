package eu.stratosphere.emma.optimizer

package object rewrite {

  import scala.reflect.runtime.universe._
  import scala.reflect.api.{TreeCreator, TypeCreator, Universe}
  import eu.stratosphere.emma.ir._

  def getTypeTag[T: TypeTag](obj: T) = typeTag[T]

  def getWeakTypeTag[T: TypeTag](obj: T) = weakTypeTag[T]

  def getType[T: TypeTag](obj: T) = typeOf[T]

  def getWeakType[T: TypeTag](obj: T) = weakTypeOf[T]

  def mkExpr[T](mirror: Mirror)(tree: Tree)(implicit tpe: TypeTag[T]): mirror.universe.Expr[T] =
    mirror.universe.Expr[T](mirror, new TreeCreator {
      def apply[U <: Universe with Singleton](m: scala.reflect.api.Mirror[U]): U#Tree =
        if (m eq mirror) tree.asInstanceOf[U#Tree]
        else throw new IllegalArgumentException(s"Expr defined in $mirror cannot be migrated to other mirrors.")
    })

  def mkTypeTag[T](mirror: Mirror)(expr: Expr[T]) = TypeTag(mirror, new TypeCreator() {
    def apply[U <: Universe with Singleton](m: scala.reflect.api.Mirror[U]): U#Type =
      if (m eq mirror) expr.staticType.asInstanceOf[U#Type]
      else throw new IllegalArgumentException(s"TypeTag defined in ${expr.mirror} cannot be migrated to other mirrors.")
  })


  class VarTransformer(val replace: Map[String, Tree]) extends Transformer {

    override def transform(tree: Tree): Tree = tree match {
      case ident@Ident(name: TermName) =>
        replace.get(name.toString) match {
          case Some(t) => t
          case None => ident
        }
      case _ => super.transform(tree)
    }
  }

  def substituteVars(freeVars: List[String], exprs: List[Expression]): Unit = {
    substituteVars(freeVars.head, freeVars, exprs)
  }

  def substituteVars(newIdentifier: String, freeVars: List[String], exprs: List[Expression]): Unit = {
    val identifierPattern = s"$newIdentifier".r

    val replacement = {
      var i = 0
      val builder = Map.newBuilder[String, Tree]
      for (v <- freeVars) {
        builder += Tuple2(v, Select(Ident(TermName(freeVars.head)), TermName(s"_$i")))
        i = i + 1
      }
      builder.result()
    }

    for (x <- exprs) x match {
      case g: Generator[Any] =>
        for (v <- freeVars if (v == g.lhs)) {
          g.lhs = identifierPattern.replaceAllIn(g.lhs, s"${freeVars.head}._${freeVars.indexOf(v)}")
        }

      case s@ScalaExpr(_, scalaExpr) =>
        val typeTag = mkTypeTag(scalaExpr.mirror)(scalaExpr)
        s.expr = mkExpr(scalaExpr.mirror)(new VarTransformer(replacement).transform(scalaExpr.tree))(typeTag)
        s.env = s.env.map(x => replacement.get(x._1) match {
          case Some(r) => (r.toString, x._2)
          case None => x
        })
    }
  }

  /**
   * Returns a list of all free vars contained in this expression tree.
   *
   * For vars with identical identifiers, the definition in the top most scope is returned
   *
   * @param expr
   * @return
   */
  def getFreeVars(expr: Expression): Map[String, Expr[Any]] = {
    val builder = Map.newBuilder[String, Expr[Any]]
    for (expr <- globalSeq(expr)) expr match {
      case ScalaExpr(fv, _) => builder ++= fv
      case _ =>
    }
    builder.result()
  }

  def containsFreeVars(fvar: String, expression: Expression): Boolean = {
    globalSeq(expression).foldRight(false)((expr, contained) => (expr match {
      case ScalaExpr(v, _) => v.contains(fvar)
      case _ => false
    }) || contained)
  }

}
