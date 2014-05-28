package eu.stratosphere.emma.macros.program

import _root_.eu.stratosphere.emma.mc.Dataflow

import _root_.scala.collection.mutable.ListBuffer
import _root_.scala.language.existentials
import _root_.scala.language.experimental.macros
import _root_.scala.reflect.macros.blackbox.Context

class ProgramMacros(val c: Context) {

  import c.universe._

  /**
   * Lifts the root block of an Emma program into a monatic comprehension intermediate representation.
   *
   * @return
   */
  def dataflow(e: Expr[Any]): Expr[Dataflow] = {
    new DataflowHelper(e).execute()
  }

  /**
   *
   * @return
   */
  def toybox(e: Expr[Any]): Expr[String] = {
    val b = e.tree.asInstanceOf[Block]
    val z = b.expr
    z match {
      case Apply(Apply(fn, params), args) =>
        c.Expr[String]( q"""
          "OK!"
        """)
      case _ =>
        c.Expr[String]( q"""
          "Not OK!"
        """)
    }
  }

  private[program] class DataflowHelper(val root: Expr[Any]) {

    var mcCount: Int = -1
    var varCount: Int = -1

    /**
     * Lifts the root block of an Emma program into a monatic comprehension intermediate representation.
     *
     * @return
     */
    def execute(): Expr[Dataflow] = root.tree match {
      case _: Block =>
        c.Expr(liftRootBlock(root.tree.asInstanceOf[Block]))
      case _ =>
        c.abort(c.enclosingPosition, "Emma programs may consist only of term expressions")

    }

    /**
     * Lifts the root block of an Emma program.
     *
     * @param e The root block AST to be lifted.
     * @return
     */
    private def liftRootBlock(e: Block): Block = {

      // recursively lift to MC syntax starting from the sinks
      val sinks = (for (s <- extractSinkExprs(e.expr)) yield lift(e, Nil)(resolve(e)(s))).toList

      // a list of statements for the root block of the translated MC expression
      val stats = ListBuffer[Tree]()
      // 1) add required imports
      stats += c.parse("import _root_.eu.stratosphere.emma.mc._")
      stats += c.parse("import _root_.scala.collection.mutable.ListBuffer")
      stats += c.parse("import _root_.scala.reflect.runtime.universe._")
      // 2) initialize translated sinks list
      stats += c.parse("val sinks = ListBuffer[Comprehension]()")
      // 3) add the translated MC expressions for all sinks
      for (s <- sinks) {
        stats += q"sinks += ${serialize(s)}"
      }

      // construct and return a block that returns a Dataflow using the above list of sinks
      Block(stats.toList, c.parse( """Dataflow("Emma Dataflow", sinks.toList)"""))
    }

    // ---------------------------------------------------
    // Lift methods.
    // ---------------------------------------------------

    private def lift(scope: Tree, env: List[ValDef])(tree: Tree): Expression = {

      // ignore a top-level Typed node (side-effect of the Algebra inlining macros)
      val t = tree match {
        case Typed(inner, _) => inner
        case _ => tree
      }

      // match expression type
      t match {
        case Apply(TypeApply(Select(parent, TermName("map")), List(_)), List(f)) =>
          liftMap(scope, env)(t.asInstanceOf[TermTree])
        case Apply(TypeApply(Select(parent, TermName("flatMap")), List(_)), List(f)) =>
          liftFlatMap(scope, env)(t.asInstanceOf[TermTree])
        case Apply(Select(parent, TermName("withFilter")), List(f)) =>
          liftWithFilter(scope, env)(t.asInstanceOf[TermTree])
        case Apply(Apply(TypeApply(q"emma.this.`package`.write", List(_)), _), _) =>
          liftSink(scope, env)(t.asInstanceOf[TermTree])
        case Apply(TypeApply(q"emma.this.`package`.read", List(_)), _) =>
          liftSource(scope, env)(t.asInstanceOf[TermTree])
        case _ =>
          ScalaExpr(c.Expr( q"""{ ..${env}; reify { ${freeEnv(t, env)} } }"""))
      }
    }

    private def liftSource(scope: Tree, env: List[ValDef])(term: TermTree) = term match {
      case Apply(TypeApply(q"emma.this.`package`.read", List(outTpe)), location :: ifmt :: Nil) =>
        val bind_bytes = ScalaExprGenerator(TermName("bytes"), ScalaExpr(c.Expr( q"""{
            val ifmt: InputFormat[$outTpe] = $ifmt
            val dop = null.asInstanceOf[Int]

            reify{ ifmt.split($location, dop) }
          }""")))

        val bind_record = ScalaExprGenerator(TermName("record"), ScalaExpr(c.Expr( q"""{
            val ifmt: InputFormat[$outTpe] = $ifmt
            val bytes = null.asInstanceOf[Seq[Byte]]

            reify{ ifmt.read(bytes) }
          }""")))

        val head = ScalaExpr(c.Expr( q"""{
            val record = null.asInstanceOf[$outTpe]
            reify { record }
          }"""))

        Comprehension(q"monad.Bag[$outTpe]", head, bind_bytes :: bind_record :: Nil)
      case _ =>
        c.abort(term.pos, "Unexpected source expression. Cannot apply MC translation scheme.")
    }

    private def liftSink(scope: Tree, env: List[ValDef])(term: TermTree) = term match {
      case Apply(Apply(TypeApply(fn, List(t)), location :: ofmt :: Nil), List(in: Tree)) =>
        val bind_record = ComprehensionGenerator(TermName("record"), lift(scope, env)(resolve(scope)(in)).asInstanceOf[MonadExpression])

        val head = ScalaExpr(c.Expr( q"""{
            val ofmt = $ofmt
            val record = null.asInstanceOf[$t]

            reify { ofmt.write(record) }
          }"""))

        Comprehension(q"monad.All", head, bind_record :: Nil)
      case _ =>
        c.abort(term.pos, "Unexpected pattern. Expecting sink expression. Cannot apply MC translation scheme.")
    }

    private def liftMap(scope: Tree, env: List[ValDef])(term: TermTree) = {
      if (!typeUtils.isCollectionType(term.tpe.widen.typeSymbol)) {
        c.abort(term.pos, "Unsupported expression. Cannot apply MC translation scheme to non-collection type.")
      }

      term match {
        case Apply(TypeApply(Select(parent, TermName("map")), List(outTpe)), List(fn@Function(List(arg), body))) =>
          val valdef = q"val ${arg.name} = null.asInstanceOf[${parent.tpe.widen.typeArgs.head}]".asInstanceOf[ValDef]

          val bind = ComprehensionGenerator(arg.name, lift(scope, env)(resolve(scope)(parent)).asInstanceOf[MonadExpression])
          val head = lift(scope, valdef :: env)(body)

          Comprehension(q"monad.Bag[$outTpe]", head, bind :: Nil)
        case _ =>
          c.abort(term.pos, "Unexpected pattern. Expecting map expression. Cannot apply MC translation scheme.")
      }
    }

    private def liftFlatMap(scope: Tree, env: List[ValDef])(term: TermTree) = {
      if (!typeUtils.isCollectionType(term.tpe.widen.typeSymbol)) {
        c.abort(term.pos, "Unsupported expression. Cannot apply MC translation scheme to non-collection type.")
      }

      term match {
        case Apply(TypeApply(Select(parent, TermName("flatMap")), List(outTpe)), List(fn@Function(List(arg), body))) =>
          val valdef = q"val ${arg.name} = null.asInstanceOf[${parent.tpe.widen.typeArgs.head}]".asInstanceOf[ValDef]

          val bind = ComprehensionGenerator(arg.name, lift(scope, env)(resolve(scope)(parent)).asInstanceOf[MonadExpression])
          val head = lift(scope, valdef :: env)(body)

          MonadJoin(Comprehension(q"monad.Bag[$outTpe]", head, bind :: Nil))
        case _ =>
          c.abort(term.pos, "Unexpected pattern. Expecting map expression. Cannot apply MC translation scheme.")
      }
    }

    private def liftWithFilter(scope: Tree, env: List[ValDef])(term: TermTree) = {
      if (!typeUtils.isCollectionType(term.tpe.widen.typeSymbol)) {
        c.abort(term.pos, "Unsupported expression. Cannot apply MC translation scheme to non-collection type.")
      }

      term match {
        case Apply(Select(parent, TermName("withFilter")), List(fn@Function(List(arg), body))) =>
          val valdef = q"val ${arg.name}: ${parent.tpe.widen.typeArgs.head} = null.asInstanceOf[${parent.tpe.widen.typeArgs.head}]".asInstanceOf[ValDef]

          val bind = ComprehensionGenerator(arg.name, lift(scope, env)(resolve(scope)(parent)).asInstanceOf[MonadExpression])
          val filter = Filter(lift(scope, valdef :: env)(body))
          val head = lift(scope, valdef :: env)(q"${valdef.name}")

          Comprehension(q"monad.Bag[${parent.tpe.widen.typeArgs.head}]", head, bind ::  filter :: Nil)
        case _ =>
          c.abort(term.pos, "Unexpected pattern. Expecting map expression. Cannot apply MC translation scheme.")
      }
    }

    private def resolve(scope: Tree)(tree: Tree) = tree match {
      // resolve term definition
      case Ident(n: TermName) => findValDef(scope)(n).getOrElse(c.abort(scope.pos, "Could not find definition of val '" + n + "' within scope")).rhs
      case _ => tree
    }

    // ---------------------------------------------------
    // Helper methods.
    // ---------------------------------------------------

    /**
     * Extracts the symbols of the sinks returned by an Emma program.
     *
     * @param e The return statement of an Emma program.
     * @return
     */
    private def extractSinkExprs(e: Tree): Set[Tree] = e match {
      case Apply(TypeApply(Select(Select(Ident(scala), x), TermName("apply")), _), args) =>
        if (x.toString.substring(0, x.toString.length - 1) != "Tuple") {
          c.abort(e.pos, "Emma programs must end with a tuple of sink identifiers")
        }
        args.toSet
      case _ =>
        Set(e)
    }

    /**
     * Find the ValDef scope for the given TermName.
     *
     * @param scope The enclosing search scope.
     * @param name The ValDef to be looked up.
     * @return
     */
    private def findValDef(scope: Tree)(name: TermName): Option[ValDef] = {
      scope.find {
        case ValDef(_, n, _, _) => n == name
        case _ => false
      }.asInstanceOf[Option[ValDef]]
    }

    private def freeEnv(tree: Tree, env: List[ValDef]) = {
      val transformer = new VparamsRelacer(env zip env)
      transformer.transform(tree)
    }

    // ---------------------------------------------------
    // Code traversers.
    // ---------------------------------------------------

    private class DependencyTermExtractor(val scope: Tree, val term: TermTree) extends Traverser {

      val result = ListBuffer[(TermTree, Option[TermName])]()

      override def traverse(tree: Tree): Unit = {
        if (tree != term && tree.isTerm && typeUtils.isCollectionType(tree.tpe.typeSymbol)) {
          tree match {
            case ident@Ident(name: TermName) =>
              val deftree = findValDef(scope)(name).getOrElse(c.abort(scope.pos, "Could not find definition of val '" + name.toString + "' within this scope")).rhs.asInstanceOf[TermTree]
              result += Tuple2(deftree, Some(name))
            case _ => // TermTree
              result += Tuple2(tree.asInstanceOf[TermTree], None)
          }
        }
        super.traverse(tree)
      }
    }

    private class VparamsRelacer(valdefs: List[(ValDef, ValDef)]) extends Transformer {

      val defsmap = Map() ++ {
        for (v <- valdefs) yield
          (v._1.name, v)
      }

      override def transform(tree: Tree): Tree = tree match {
        case ident@Ident(name: TermName) =>
          if (defsmap.contains(name))
            Ident(defsmap(name)._2.name)
          else
            ident
        case _ =>
          super.transform(tree)
      }
    }

  }

  // ---------------------------------------------------
  // Utility objects.
  // ---------------------------------------------------

  object typeUtils {
    val collTypeSymbols = List[Symbol](
      typeOf[eu.stratosphere.emma.DataBag[_]].typeSymbol
    )

    def isCollectionType(typeSymbol: Symbol) = collTypeSymbols.contains(typeSymbol)
  }


  // ---------------------------------------------------
  // Compile-time mirror of eu.stratosphere.emma.mc
  // ---------------------------------------------------

  abstract class Expression() {
  }

  abstract class MonadExpression() extends Expression {
  }

  case class MonadJoin(expr: Expression) extends MonadExpression {
  }

  case class MonadUnit(expr: Expression) extends MonadExpression {
  }

  abstract class Qualifier extends Expression {
  }

  case class Filter(expr: Expression) extends Qualifier {
  }

  abstract class Generator(val lhs: TermName) extends Qualifier {
  }

  case class ScalaExprGenerator(override val lhs: TermName, rhs: ScalaExpr) extends Generator(lhs) {
  }

  case class ComprehensionGenerator(override val lhs: TermName, rhs: MonadExpression) extends Generator(lhs) {
  }

  case class ScalaExpr(var expr: Expr[Any]) extends Expression {
  }

  case class Comprehension(monad: Tree, head: Expression, qualifiers: List[Qualifier]) extends MonadExpression {
  }

  private def serialize(e: Expression): Tree = e match {
    case MonadUnit(expr) =>
      q"MonadUnit(${serialize(expr)})"
    case MonadJoin(expr) =>
      q"MonadJoin(${serialize(expr)})"
    case Filter(expr) =>
      q"Filter(${serialize(expr)})"
    case ScalaExprGenerator(lhs, rhs) =>
      q"ScalaExprGenerator(${lhs.toString}, ${serialize(rhs)})"
    case ComprehensionGenerator(lhs, rhs: MonadExpression) =>
      q"{ val rhs = ${serialize(rhs)}; ComprehensionGenerator(${lhs.toString}, rhs) }"
    case ScalaExpr(expr) =>
      q"ScalaExpr($expr)"
    case Comprehension(monad, head, qualifiers) =>
      q"""
      {
        // MC qualifiers
        val qualifiers = ListBuffer[Qualifier]()
        ..${for (q <- qualifiers) yield q"qualifiers += ${serialize(q)}"}

        // MC head
        val head = ${serialize(head)}

        // MC constructor
        Comprehension($monad, head, qualifiers.toList)
      }
       """
  }
}
