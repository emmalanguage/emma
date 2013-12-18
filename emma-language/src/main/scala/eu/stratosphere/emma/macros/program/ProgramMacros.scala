package eu.stratosphere.emma.macros.program

import scala.language.existentials
import scala.language.experimental.macros
import scala.reflect.macros.blackbox.Context
import scala.collection.mutable.ListBuffer
import eu.stratosphere.emma.mc._

class ProgramMacros(val c: Context) {

  import c.universe._

  /**
   * Lifts the root block of an Emma program into a monatic comprehension intermediate representation.
   *
   * @return
   */
  def liftDataflow(e: Expr[Any]): Expr[Dataflow] = {
    new DataflowHelper(e).execute()
  }

  /**
   *
   * @return
   */
  def liftToybox(e: Expr[Any]): Expr[String] = {
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
      val sinks = (for (s <- extractSinkExprs(e.expr)) yield translate(e)(s)._1).toList

      // a list of statements for the root block of the translated MC expression
      val stats = ListBuffer[Tree]()
      // 1) add required imports
      stats += c.parse("import _root_.eu.stratosphere.emma.mc._")
      stats += c.parse("import _root_.scala.collection.mutable.ListBuffer")
      stats += c.parse("import _root_.scala.reflect.runtime.universe._")
      // 2) initialize translated sinks list
      stats += c.parse("val sinks = ListBuffer[Comprehension[AlgExists, Boolean, Boolean]]()")
      // 3) add the translated MC expressions for all sinks
      stats ++= sinks

      // construct and return a block that returns a Dataflow using the above list of sinks
      Block(stats.toList, c.parse( """Dataflow("Emma Dataflow", sinks.toList)"""))
    }

    // ---------------------------------------------------
    // Translation methods.
    // ---------------------------------------------------

    private def translate(scope: Tree)(expr: Tree): (Tree, TermName) = {
      val stats = ListBuffer[Tree]()

      val t: TermTree = ((expr match {
        // resolve term definition
        case Ident(n: TermName) => findValDef(scope)(n).getOrElse(c.abort(scope.pos, "Could not find definition of val '" + n + "' within this scope")).rhs.asInstanceOf[TermTree]
        case t: TermTree => t
        case _ => c.abort(expr.pos, "Unexpected expression with is neither TermName nor TermTree.")
      }) match {
        // ignore a top-level Typed node (side-effect of the Algebra inlining macros)
        case Typed(apply@Apply(_, _), _) => apply
        case term@_ => term
      }).asInstanceOf[TermTree]

      val n: TermName = expr match {
        case Ident(n: TermName) => nextMCName(Some(n))
        case _: TermTree => nextMCName(None)
      }

      t match {
        case Apply(Select(parent, TermName("withFilter")), List(f)) =>
          stats += translateMonad(scope)(t, n)
        case Apply(TypeApply(Select(parent, TermName("map")), List(_)), List(f)) =>
          stats += translateMonad(scope)(t, n)
        case Apply(TypeApply(Select(parent, TermName("flatMap")), List(_)), List(f)) =>
          stats += translateMonad(scope)(t, n)
        case Apply(Apply(TypeApply(q"emma.this.`package`.write", List(_)), _), _) =>
          stats += translateSink(scope)(t, n)
        case Apply(TypeApply(q"emma.this.`package`.read", List(_)), _) =>
          stats += translateSource(scope)(t, n)
        case _ =>
          c.abort(t.pos, "Unsupported expression. Cannot apply MC translation scheme.")
      }

      (q"val $n = { ..${stats.toList} }", n)
    }

    private def translateSource(scope: Tree)(term: TermTree, name: TermName): Tree = {
      term match {
        case Apply(TypeApply(fn, List(t)), location :: ifmt :: Nil) =>
          q"""
          {
            val bind_bytes = DirectGenerator[Seq[Byte], List[Seq[Byte]]]("bytes", {
              val ifmt: InputFormat[$t] = $ifmt
              val dop = 20
              val bytes = null.asInstanceOf[Seq[Byte]]

              reify{ ifmt.split($location, dop) }
            })

            val head = Head[Seq[Byte], Seq[$t]]({
              val ifmt: InputFormat[$t] = $ifmt
              val dop = 20
              val bytes = null.asInstanceOf[Seq[Byte]]

              reify{ ifmt.read(bytes) }
            })

            Comprehension[AlgBag[$t],$t,List[$t]](head, bind_bytes)
          }
          """
        case _ =>
          c.abort(term.pos, "Unexpected source expression. Cannot apply MC translation scheme.")
      }
    }

    private def translateSink(scope: Tree)(term: TermTree, name: TermName): Tree = {
      term match {
        case Apply(Apply(TypeApply(fn, List(t)), location :: ofmt :: Nil), List(in: Tree)) =>
          val (inTerm, inName) = translate(scope)(in)
          q"""
          {
            $inTerm

            val bind_record = ComprehensionGenerator[AlgBag[$t],$t,List[$t]]("record", $inName)

            val head = Head[$t, Seq[Boolean]]({
              val ofmt = $ofmt
              val record = null.asInstanceOf[$t]

              reify { ofmt.write(record) }
            })

            val comp = Comprehension[AlgExists,Boolean,Boolean](head, bind_record)
            sinks += comp
            comp
          }
          """
        case _ =>
          c.abort(term.pos, "Unexpected sink expression. Cannot apply MC translation scheme.")
      }
    }

    private def translateMonad(scope: Tree)(term: TermTree, name: TermName): Tree = {

      if (!typeUtils.isCollectionType(term.tpe.widen.typeSymbol)) {
        c.abort(term.pos, "Unsupported expression. Cannot apply MC translation scheme to non-collection type.")
      }

      term match {
        case Apply(Select(parent, TermName("withFilter")), _) =>
          val (in, predicates) = collectPredicates(term)
          val (inTerm, inName) = translate(scope)(in)

          val inTpe = in.tpe.widen.typeArgs.head
          val inVar = nextVarName(None)
          val inVars = List(q"val $inVar: $inTpe".asInstanceOf[ValDef])

          // construct qualifier
          val qualifierTerms = ListBuffer[Tree](q"val qualifiers = ListBuffer[Qualifier]()")
          // add input generator
          qualifierTerms += q"qualifiers += ComprehensionGenerator[AlgBag[$inTpe], $inTpe, List[$inTpe]](${inVar.toString}, $inName)"
          // add filters with unified parameter names
          for (p <- predicates) {
            val filterExpr = freeIdents(replaceVparams(p, List(q"val $inVar: $inTpe".asInstanceOf[ValDef])), inVars.toList).body
            qualifierTerms += q"qualifiers += Filter[$inTpe](reify { $filterExpr })"
          }

          q"""
          {
            $inTerm
            ..$qualifierTerms
            val head = Head[$inTpe, $inTpe]({
              val $inVar: $inTpe = null.asInstanceOf[$inTpe]
              reify{ $inVar }
            })
            Comprehension[AlgBag[$inTpe], $inTpe, List[$inTpe]](head, qualifiers.toList)
          }
          """
        case Apply(TypeApply(Select(parent, TermName("map")), List(outTpe)), List(fn@Function(List(arg), body))) =>
          val (in, predicates) = collectPredicates(parent)
          val (inTerm, inName) = translate(scope)(in)

          val inTpe = in.tpe.widen.typeArgs.head
          val inVar = arg.name
          val inVars = List(q"val $inVar: $inTpe".asInstanceOf[ValDef])

          // construct qualifier
          val qualifierTerms = ListBuffer[Tree](q"val qualifiers = ListBuffer[Qualifier]()")
          // add input generator
          qualifierTerms += q"qualifiers += ComprehensionGenerator[AlgBag[$inTpe], $inTpe, List[$inTpe]](${inVar.toString}, $inName)"
          // add filters with unified parameter names
          for (p <- predicates) {
            val filterExpr = freeIdents(replaceVparams(p, List(q"val $inVar: ${arg.tpe}".asInstanceOf[ValDef])), inVars.toList).body
            qualifierTerms += q"qualifiers += Filter[$inTpe](reify { $filterExpr })"
          }

          q"""
          {
            $inTerm
            ..$qualifierTerms
            val head = Head[$inTpe, $outTpe](reify{ ${replaceVparams(fn, List(q"val $inVar: ${arg.tpe}".asInstanceOf[ValDef])).body} })
            Comprehension[AlgBag[$outTpe], $outTpe, List[$outTpe]](head, qualifiers.toList)
          }
          """
        case Apply(TypeApply(Select(parent, TermName("flatMap")), List(outTpe)), List(fn@Function(List(arg), body))) =>
          val inVars = ListBuffer[ValDef]()
          val inTerms = ListBuffer[Tree]()
          val qualifierTerms = ListBuffer[Tree](q"val qualifiers = ListBuffer[Qualifier]()")
          val finalTerms = ListBuffer[Tree]()

          var currParent = parent
          var currTpe = outTpe
          var currFn = fn
          var currArg = arg
          var currBody = body
          var hasNext = true

          while (hasNext) {
            val (in, predicates) = collectPredicates(currParent)
            val (inTerm, inName) = translate(scope)(in)

            val inTpe = in.tpe.widen.typeArgs.head
            val inVar = currArg.name

            inVars += q"val $inVar = null.asInstanceOf[$inTpe]".asInstanceOf[ValDef]
            inTerms += inTerm
            // add input generator
            qualifierTerms += q"qualifiers += ComprehensionGenerator[AlgBag[$inTpe], $inTpe, List[$inTpe]](${inVar.toString}, $inName)"
            // add filters with unified parameter names
            for (p <- predicates) {
              val filterExpr = freeIdents(replaceVparams(p, List(q"val $inVar: ${arg.tpe}".asInstanceOf[ValDef])), inVars.toList).body
              qualifierTerms += q"qualifiers += Filter[$inTpe](reify { $filterExpr })"
            }

            currBody match {
              case Apply(TypeApply(Select(nextParent, TermName("map")), List(nextOutTpe)), List(nextFn@Function(List(nextArg), nextBody))) =>
                currParent = nextParent
                currTpe = nextOutTpe
                currFn = nextFn
                currArg = nextArg
                currBody = nextBody
                hasNext = true
              case Apply(TypeApply(Select(nextParent, TermName("flatMap")), List(nextOutTpe)), List(nextFn@Function(List(nextArg), nextBody))) =>
                currParent = nextParent
                currTpe = nextOutTpe
                currFn = nextFn
                currArg = nextArg
                currBody = nextBody
                hasNext = true
              case _ =>
                val headExpr = freeIdents(replaceVparams(currFn, List(q"val $inVar: ${arg.tpe}".asInstanceOf[ValDef])), inVars.toList).body
                finalTerms += q"val head = Head[$inTpe, $outTpe](reify{ $headExpr })"
                finalTerms += q"Comprehension[AlgBag[$outTpe], $outTpe, List[$outTpe]](head, qualifiers.toList)"
                hasNext = false
            }
          }

          q"""
          {
            ..$inTerms
            ..$qualifierTerms
            ..$finalTerms
          }
          """

        case _ =>
          c.abort(term.pos, "Unexpected map expression. Cannot apply MC translation scheme.")
      }
    }

    // ---------------------------------------------------
    // Helper methods.
    // ---------------------------------------------------

    private def nextMCName(suffix: Option[TermName]): TermName = {
      mcCount += 1
      suffix match {
        case Some(x) => TermName(f"_MC_$mcCount%05d_$x")
        case _ => TermName(f"_MC_$mcCount%05d")
      }
    }

    private def nextVarName(suffix: Option[TermName]): TermName = {
      varCount += 1
      suffix match {
        case Some(x) => TermName(f"_x_$varCount%05d_$x")
        case _ => TermName(f"_x_$varCount%05d")
      }
    }

    /**
     * Eates-up and accumulates the predicates passed to a chain of DataSet[T].withFilter applications.
     *
     * @param term The term tree to consume
     * @return A pair consisting of the residual term tree and the accumulated predicate functions.
     */
    private def collectPredicates(term: Tree): (Tree, List[Function]) = term match {
      case Apply(Select(parent: Tree, TermName("withFilter")), List(fn@Function(List(arg), body))) =>
        val (rest, tail) = collectPredicates(parent)
        (rest, (fn :: tail).reverse)
      case Apply(TypeApply(Select(parent, TermName("map")), _), _) =>
        (term, List())
      case Apply(TypeApply(Select(parent, TermName("flatMap")), _), _) =>
        (term, List())
      case Apply(TypeApply(Select(parent, TermName("map")), _), _) =>
        (term, List())
      case Ident(_) =>
        (term, List())
      case Typed(apply@Apply(_, _), _) =>
        collectPredicates(apply) // ignore a top-level Typed node (side-effect of the Algebra inlining macros)
      case _ =>
        c.abort(term.pos, "Oppps! Something went horribly wrong here...")
    }

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
     * Validates that an identifier has the given type and extracts its term name.
     *
     * @param id The identifier.
     * @tparam T The type of the term expected for the given identifier.
     * @return
     */
    private def extractTermName[T: TypeTag](id: Ident): TermName = {
      if (!id.isTerm || id.tpe != typeOf[T]) {
        c.abort(id.pos, "Identifier should be of type " + typeOf[T].toString)
      }
      val termSymbol = id.symbol.asInstanceOf[TermSymbol]
      if (!termSymbol.isVal) {
        c.abort(id.pos, "Identifier should be introduced as val")
      }
      id.name.asInstanceOf[TermName]
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
        case ValDef(_, n, _, rhs: TermTree) => n == name
        case _ => false
      }.asInstanceOf[Option[ValDef]]
    }

    /**
     * Extracts the MC dependencies referred in a TermTree.
     * TODO: remove
     */
    private def extractDependencyTerms(scope: Tree)(term: TermTree): List[(TermTree, Option[TermName])] = {
      val t = new DependencyTermExtractor(scope, term)
      t.traverse(term)
      t.result.toList
    }

    def replaceVparams(fn: Function, vparams: List[ValDef]) = {
      val transformer = new VparamsRelacer(fn.vparams zip vparams)
      transformer.transform(fn).asInstanceOf[Function]
    }

    def freeIdents(fn: Function, idents: List[ValDef]) = {
      val transformer = new VparamsRelacer(idents zip idents)
      transformer.transform(fn).asInstanceOf[Function]
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
      typeOf[eu.stratosphere.emma.DataSet[_]].typeSymbol
    )

    def isCollectionType(typeSymbol: Symbol) = collTypeSymbols.contains(typeSymbol)
  }

}
