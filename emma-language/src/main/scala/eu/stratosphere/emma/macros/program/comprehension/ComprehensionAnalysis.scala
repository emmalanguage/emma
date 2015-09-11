package eu.stratosphere.emma.macros.program.comprehension

import eu.stratosphere.emma.macros.program.comprehension.rewrite.ComprehensionNormalization
import eu.stratosphere.emma.macros.program.controlflow.ControlFlowModel
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

private[emma] trait ComprehensionAnalysis
    extends ControlFlowModel
    with ComprehensionModel
    with ComprehensionNormalization {

  import universe._

  /**
   * A set of API method symbols to be comprehended.
   */
  protected object api {
    val moduleSymbol = rootMirror.staticModule("eu.stratosphere.emma.api.package")
    val bagSymbol    = rootMirror.staticClass("eu.stratosphere.emma.api.DataBag")
    val groupSymbol  = rootMirror.staticClass("eu.stratosphere.emma.api.Group")

    val apply        = bagSymbol.companion.info.decl(TermName("apply"))
    val read         = moduleSymbol.info.decl(TermName("read"))
    val write        = moduleSymbol.info.decl(TermName("write"))
    val stateful     = moduleSymbol.info.decl(TermName("stateful"))
    val fold         = bagSymbol.info.decl(TermName("fold"))
    val map          = bagSymbol.info.decl(TermName("map"))
    val flatMap      = bagSymbol.info.decl(TermName("flatMap"))
    val withFilter   = bagSymbol.info.decl(TermName("withFilter"))
    val groupBy      = bagSymbol.info.decl(TermName("groupBy"))
    val minus        = bagSymbol.info.decl(TermName("minus"))
    val plus         = bagSymbol.info.decl(TermName("plus"))
    val distinct     = bagSymbol.info.decl(TermName("distinct"))

    val methods = Set(
      read, write,
      stateful,
      fold,
      map, flatMap, withFilter,
      groupBy,
      minus, plus, distinct
    ) ++ apply.alternatives

    val monadic = Set(map, flatMap, withFilter)
  }

  // --------------------------------------------------------------------------
  // Comprehension Store Constructor
  // --------------------------------------------------------------------------

  /**
   * Comprehension store constructor.
   *
   * @param cfGraph The control flow graph for the comprehended algorithm.
   */
  def createComprehensionView(cfGraph: CFGraph) = {

    // step #1: compute the set of maximal terms that can be translated to comprehension syntax
    val root = cfGraph.nodes.find(_.inDegree == 0).get
    // implicit helpers
    implicit def cfBlockTraverser: CFGraph#OuterNodeTraverser = root.outerNodeTraverser()

    // step #1: compute the set of maximal terms that can be translated to comprehension syntax
    val terms = (for (block <- cfBlockTraverser; stmt <- block.stats) yield {
      // find all value applications on methods from the comprehended API for this statement
      implicit var comprehendedTerms = mutable.Set(stmt.filter({
        case Apply(fun, _) if api.methods.contains(fun.symbol) => true
        case _ => false
      }): _*)

      // reduce by removing nodes that will be comprehended with their parent
      var obsolete = mutable.Set.empty[Tree]
      do {
        comprehendedTerms = comprehendedTerms diff obsolete
        obsolete = for (t <- comprehendedTerms; c <- comprehendedChild(t)) yield c
      } while (obsolete.nonEmpty)

      // return the reduced set of applies
      comprehendedTerms
    }).flatten.toSet

    // step #2: create ComprehendedTerm entries for the identified terms
    val comprehendedTerms = mutable.Seq((for (t <- terms) yield {
      val id = TermName(c.freshName("comprehension"))
      val definition = comprehendedTermDefinition(t)
      val comprehension = normalize(ExpressionRoot(comprehend(Nil)(t) match {
        case root@combinator.Write(_, _, _) => root
        case root@combinator.Fold(_, _, _, _, _) => root
        case root: Expression => combinator.TempSink(comprehendedTermName(definition, id), root)
      }))

      ComprehendedTerm(id, t, comprehension, definition)
    }).toSeq: _*)

    // step #3: build the comprehension store
    new ComprehensionView(comprehendedTerms)
  }

  /**
   * Checks whether the parent expression in a selector chain is also comprehended.
   *
   * What happens here is that effectively we are looking for "inverse" links as compared to the traversal order in
   * the "comprehend" method
   *
   * @param t the expression to be checked
   * @param comprehendedTerms the set of comprehended terms
   * @return An option holding the comprehended parent term (if such exists).
   */
  private def comprehendedChild(t: Tree)(implicit comprehendedTerms: mutable.Set[Tree]) = t match {
    // FIXME: make this consistent with the comprehend() method patterns
    case Apply(fun, (f: Function) :: Nil) if (fun.symbol == api.map || fun.symbol == api.flatMap) && comprehendedTerms.contains(f.body) =>
      Some(f.body)
    case Apply(Apply(TypeApply(Select(_, _), _), _), List(parent)) if comprehendedTerms.contains(parent) =>
      Some(parent)
    case Apply(TypeApply(Select(_, _), _), List(parent)) if comprehendedTerms.contains(parent) =>
      Some(parent)
    case Apply(TypeApply(Select(parent, _), _), _) if comprehendedTerms.contains(parent) =>
      Some(parent)
    case Apply(Select(parent, _), _) if comprehendedTerms.contains(parent) =>
      Some(parent)
    case Apply(parent@Apply(_, _), _) if comprehendedTerms.contains(parent) =>
      Some(parent)
    case _ =>
      Option.empty[Tree]
  }

  /**
   * Looks up a definition term (ValDef or Assign) for a comprehended term.
   *
   * @param t The term to lookup.
   * @return The (optional) definition for the term.
   */
  private def comprehendedTermDefinition(t: Tree)(implicit cfBlockTraverser: TraversableOnce[CFBlock]) = {
    var optTree = Option.empty[Tree]
    for (block <- cfBlockTraverser; s <- block.stats) s.foreach({
      case vd@ValDef(_, name: TermName, _, rhs) if t == rhs =>
        optTree = Some(vd)
      case vd@Assign(Ident(name: TermName), rhs) if t == rhs =>
        optTree = Some(vd)
      case _ =>
    })
    optTree
  }

  /**
   * Looks up a definition term (ValDef or Assign) for a comprehended term.
   *
   * @param t The term to lookup.
   * @return The (optional) definition for the term.
   */
  private def comprehendedTermName(t: Option[Tree], default: TermName) = t.getOrElse(default) match {
    case ValDef(_, name: TermName, _, rhs) => name
    case Assign(Ident(name: TermName), rhs) => name
    case _ => default
  }

  // --------------------------------------------------------------------------
  // Comprehension Constructor
  // --------------------------------------------------------------------------

  /**
   * Recursive comprehend method.
   *
   * @param vars The Variable environment for the currently lifted tree
   * @param tree The tree to be lifted
   * @return A lifted, MC syntax version of the given tree
   */
  private def comprehend(vars: List[ValDef])(tree: Tree, input: Boolean = true): Expression = {

    // ignore a top-level Typed node (side-effect of the Algebra inlining macros)
    val t = tree match {
      case Typed(inner, _) => inner
      case _ => tree
    }

    // translate based on matched expression type
    t match {

      // -----------------------------------------------------
      // Monad Ops
      // -----------------------------------------------------

      // in.map(fn)
      case Apply(TypeApply(select@Select(in, _), List(tpt)), List(fn@Function(List(arg), body))) if select.symbol == api.map =>
        val bind = Generator(arg.term, comprehend(vars)(in))
        val head = comprehend(arg :: vars)(body, input = false)

        Comprehension(head, bind :: Nil)

      // in.flatMap(fn)
      case Apply(TypeApply(select@Select(in, _), List(tpt)), List(fn@Function(List(arg), body))) if select.symbol == api.flatMap =>
        val bind = Generator(arg.term, comprehend(vars)(in))
        val head = comprehend(arg :: vars)(body, input = false)

        MonadJoin(Comprehension(head, bind :: Nil))

      // in.withFilter(fn)
      case Apply(select@Select(in, _), List(fn@Function(List(arg), body))) if select.symbol == api.withFilter =>
        val bind = Generator(arg.term, comprehend(vars)(in))
        val filter = Filter(comprehend(arg :: vars)(body))
        val head = comprehend(arg :: vars)(q"${arg.name}", input = false)

        Comprehension(head, bind :: filter :: Nil)

      // -----------------------------------------------------
      // Grouping and Set operations
      // -----------------------------------------------------

      // in.groupBy(k)
      case Apply(TypeApply(select@Select(in, _), List(tpt)), List(k@Function(List(arg), body))) if select.symbol == api.groupBy =>
        combinator.Group(k, comprehend(Nil)(in))

      // in.minus(subtrahend)
      case Apply(TypeApply(select@Select(in, _), List(_)), List(subtrahend)) if select.symbol == api.minus =>
        combinator.Diff(comprehend(Nil)(in), comprehend(Nil)(subtrahend))

      // in.plus(addend)
      case Apply(TypeApply(select@Select(in, _), List(_)), List(addend)) if select.symbol == api.plus =>
        combinator.Union(comprehend(Nil)(in), comprehend(Nil)(addend))

      // in.distinct()
      case Apply(select@Select(in, _), Nil) if select.symbol == api.distinct =>
        combinator.Distinct(comprehend(Nil)(in))

      // -----------------------------------------------------
      // Aggregates
      // -----------------------------------------------------

      // in.fold(empty, sng, union)
      case Apply(TypeApply(select@Select(in, _), _), List(empty, sng, union)) if select.symbol == api.fold =>
        combinator.Fold(empty, sng, union, comprehend(Nil)(in), t)

      // ----------------------------------------------------------------------
      // Environment & Host Language Connectors
      // ----------------------------------------------------------------------

      // write[T](location, ofmt)(in)
      case Apply(Apply(TypeApply(method, List(_)), location :: ofmt :: Nil), List(in: Tree)) if method.symbol == api.write =>
        combinator.Write(location, ofmt, comprehend(vars)(in))

      // read[T](location, ifmt)
      case Apply(TypeApply(method, List(tpt)), location :: ifmt :: Nil) if method.symbol == api.read =>
        combinator.Read(location, ifmt)

      // temp result identifier
      case ident@Ident(TermName(_)) if input =>
        combinator.TempSource(ident)

      // interpret as boxed Scala expression (default)
      case _ =>
        ScalaExpr(vars, typeCheckWith(vars, t)) // trees created by the caller with q"..." have to be explicitly typechecked
    }
  }

  // --------------------------------------------------------------------------
  // Logical Optimizations
  // --------------------------------------------------------------------------

  /**
   * Inlines comprehended ValDefs occurring only once with their parents.
   *
   * @param tree The original program tree.
   * @param cfGraph The control flow graph for the comprehended algorithm.
   * @param comprehensionView A view over the comprehended terms in the tree.
   * @return An inlined inlined tree.
   */
  def inlineComprehensions(tree: Tree)
      (implicit cfGraph: CFGraph, comprehensionView: ComprehensionView) = {

    var inlinedTree = tree

    // find all valdefs that can be inlined
    var valDefs = (for (cv <- comprehensionView.terms; d <- cv.definition)
      yield cv.definition collect {
        // make sure that the associated definition is a non-mutable ValDef
        case vd @ ValDef(mods, _, _, _) if (mods.flags | Flag.MUTABLE) != mods.flags =>
          // get the identifiers referencing this ValDef symbol
          val idents = tree collect { case id: Ident if id.symbol == d.symbol => id }
          // if the symbol is referenced only once, inline the ValDef rhs in place of the ident
          if (idents.size == 1) Some(vd) else None
      }).flatten.flatten

    while (valDefs.nonEmpty) {
      // get a ValDef to inline
      val vd = valDefs.head
      // inline current ValDef in the tree
      inlinedTree = inlinedTree inline vd
      // inline in all other ValDefs and continue with those
      valDefs = for (other <- valDefs filter { _.symbol != vd.symbol })
        yield other.inline(vd).as[ValDef]
    }

    inlinedTree.typeChecked
  }

  /**
   * Performs Fold-Group-Fusion.
   *
   * @param tree The original program tree.
   * @param cfGraph The control flow graph for the comprehended algorithm.
   * @param comprehensionView A view over the comprehended terms in the tree.
   * @return
   */
  def foldGroupFusion(tree: Tree)(implicit cfGraph: CFGraph, comprehensionView: ComprehensionView) = {
    // find the symbols of all "Group[K, V]" definitoins 'g'
    val groupValDefs = tree.collect({
      case vd@ValDef(_, _, _, _) if hasGroupType(vd) => vd
    })

    // compute a flattened list of all expressions in the comprehensionView
    val allExpressions = comprehensionView.terms flatMap { _.comprehension.expr }

    for (groupValDef <- groupValDefs; (generator, group) <- generatorFor(groupValDef.term, allExpressions)) {
      // the symbol associated with 'g'
      val groupSymbol = groupValDef.symbol

      // find all 'g.values' expressions for the group symbol
      val groupValueSelects = tree.collect({
        case select@Select(id@Ident(_), TermName("values")) if id.symbol == groupSymbol => select
      })

      // for each 'g.values' expression, find an associated 'g.values.fold(...)' comprehension, if one exists
      val foldExpressions = for (select <- groupValueSelects; expr <- allExpressions; fold <- foldOverSelect(select, expr)) yield fold

      // !!! all 'g.values' expressions are used directly in a comprehended 'fold' => apply Fold-Group-Fusion !!!
      if (foldExpressions.nonEmpty && groupValueSelects.size == foldExpressions.size) {
        // create an auxiliary map
        val foldToIndex = Map(foldExpressions.map(_.origin).zipWithIndex: _*)

        // 1) fuse the group with the folds and replace the Group with a GroupFold in the enclosing generator
        generator.rhs = foldGroup(group, foldExpressions)

        // 2) replace the comprehended fold expressions
        for (expr <- allExpressions) expr match {
          // adapt scala expression nodes referencing the group
          case expr@ScalaExpr(vars, _) if vars.map(_.name).contains(groupValDef.name) =>
            // find all value selects with associated enclosed in this ScalaExpr
            val enclosedComrehendedFolds = Map(expr.tree.collect({
              case t: Tree if foldToIndex.contains(t) => t -> foldToIndex(t)
            }): _*)

            if (enclosedComrehendedFolds.nonEmpty) {
              expr.vars = expr.vars.map(v => if (v.name == groupValDef.name) ValDef(Modifiers(), groupValDef.name, tq"${generator.rhs.tpe.typeArgs.head}", EmptyTree) else v)
              expr.tree = typeCheckWith(expr.vars, new FoldTermsReplacer(enclosedComrehendedFolds, q"${groupValDef.name}.values").transform(expr.tree))
            }
          // adapt comprehensions that contain the fold as a head
          case expr@Comprehension(fold: combinator.Fold, _) if foldExpressions.contains(fold) =>
            // find all value selects with associated enclosed in this ScalaExpr
            val enclosedComrehendedFolds = Map(fold.origin -> foldToIndex(fold.origin))
            val newHeadVars = List(ValDef(Modifiers(), groupValDef.name, tq"${generator.rhs.tpe.typeArgs.head}", EmptyTree))
            val newHeadTree = typeCheckWith(newHeadVars, new FoldTermsReplacer(enclosedComrehendedFolds, q"${groupValDef.name}.values").transform(fold.origin))
            expr.hd = ScalaExpr(newHeadVars, newHeadTree)

          // adapt comprehensions that contain the fold as a filter
          case expr@Filter(fold: combinator.Fold) if foldExpressions.contains(fold) =>
            // find all value selects with associated enclosed in this ScalaExpr
            val enclosedComrehendedFolds = Map(fold.origin -> foldToIndex(fold.origin))
            val newHeadVars = List(ValDef(Modifiers(), groupValDef.name, tq"${generator.rhs.tpe.typeArgs.head}", EmptyTree))
            val newHeadTree = typeCheckWith(newHeadVars, new FoldTermsReplacer(enclosedComrehendedFolds, q"${groupValDef.name}.values").transform(fold.origin))
            expr.expr = ScalaExpr(newHeadVars, newHeadTree)
          // ignore the rest
          case _ =>
        }
      }
    }
  }

  def normalizePredicates(tree: Tree)(implicit cfGraph: CFGraph, comprehensionView: ComprehensionView) = {

    for (term <- comprehensionView.terms) yield {
      val comprehensions = term.comprehension.expr.collect({ case c@Comprehension(h, q) => c})

      for (comprehension <- comprehensions) yield {
        val qualifiers = comprehension.qualifiers

        val newQualifiers = {for (qualifier <- qualifiers) yield
          qualifier match {
            case Filter(ScalaExpr(v, t)) =>
              // normalize the tree
              val normalizedFilters = cleanConjuncts(distributeOrOverAnd(applyDeMorgan(t)))
                                      .filter(x => x != None)
                                      .map { case Some(nf) => Filter(ScalaExpr(v, nf))}
              normalizedFilters

            case _ => List(qualifier)
          }}.flatten

        comprehension.qualifiers = newQualifiers
      }
    }

    tree
  }

  /**
   * Apply deMorgan's Rules to predicates (move negations as far in as possible)
   * @param tree
   * @return
   */
  def applyDeMorgan(tree: Tree): Tree = {

    object deMorganTransformer extends Transformer {
      def moveNegationsInside(t: Tree): Tree = t match {

        case op@Apply(Select(q, n), as) => n match {

          // !(A || B) => !A && !B
          case TermName("$bar$bar") =>
            val arg = as.head
            val moved = q"(!$q && !$arg)"
            transform(moved)

          // !(A && B) => !A || !B
          case TermName("$amp$amp") =>
            val arg = as.head
            val moved = q"(!$q || !$arg)"
            transform(moved)

          /* !A => !A
             this should cover the cases for atoms that we don't want to reduce further: */
          case _ => q"!$t"

        }
        case _ =>
          throw new RuntimeException("Unforseen Tree in DeMorgatransformer!")
      }

      override def transform(tree: Tree): Tree = tree match {
        case Select(q, TermName("unary_$bang")) =>
          moveNegationsInside(q)
        case _ => super.transform(tree)
      }
    }

    deMorganTransformer.transform(tree)
  }

  /**
   * Distribute ∨ inwards over ∧
   * Repeatedly replace B ∨ (A ∧ C) with (B ∨ A) ∧ (B ∨ C)
   */
  def distributeOrOverAnd(tree: Tree): Tree = {
    object DistributeTransformer extends Transformer {
      override def transform(tree: Tree): Tree = tree match {
        case Apply(Select(lhs, TermName("$bar$bar")), rhs) =>
          (lhs, rhs.head) match {
            /* C || (A && B) => (A || C) && (C || B)

            this also covers

            (A && B) || (C && D)
             => ((A && B) || C) && ((A && B) || D)
             => (((A || C) && (B || C)) && ((A || D) && (B || D))
             */
            case (_, Apply(Select(lhs2, TermName("$amp$amp")), rhs2)) =>
              val modlhs = transform(q"($lhs || $lhs2)")
              val modrhs = transform(q"($lhs || ${rhs2.head})")
              q"$modlhs && $modrhs"

            // (A && B) || C => (A || C) && (C || B)
            case (Apply(Select(lhs2, TermName("$amp$amp")), rhs2), _) =>
              val modlhs = transform(q"($lhs2 || ${rhs.head})")
              val modrhs = transform(q"(${rhs2.head} || ${rhs.head})")
              q"$modlhs && $modrhs"

            case _ => super.transform(tree)
          }
        case _ => super.transform(tree)
      }
    }

    DistributeTransformer.transform(tree)
  }

  /** Checks the conjuncts for always true statements and duplicates and cleans them up.
    *
    * This object gets single disjuncts (A || B || C) and removes duplicates.
    * If the disjunct is always true, e.g. (A || !A || B || C), it gets removed.
    *
    * (¬B ∨ B ∨ ¬A) ∧ (¬A ∨ ¬C ∨ B ∨ ¬A) => ¬A ∨ ¬C ∨ B
    * @param tree The conjunct tree.
    * @return  A list of Option[Tree] where each option stands for one disjunct (A || ... || Z).
    *          If the disjunct is always true (e.g. (A || !A)) we can remove it and the Option will be None.
    *          In case the disjunct is not always true, the list will contain Some(tree) with the tree of
    *          the cleaned/reduced disjunct.
    */
  def cleanConjuncts(tree: Tree): ListBuffer[Option[Tree]] = {
    val conjunct = ListBuffer[Disjunct]()

    object DisjunctTraverser extends Traverser {
      var disjunct = new Disjunct()

      def newDisjunct() = {
        disjunct = new Disjunct()
      }

      def getDisjunct = disjunct

      override def traverse(tree: Tree) = tree match {
        // (A || B || ... || Y || Z)
        case Apply(Select(lhs, TermName("$bar$bar")), rhs) => {
          traverse(lhs)
          traverse(rhs.head)
        }

        // here we have a negated atom
        case Select(a@Apply(Select(_,_),_), TermName("unary_$bang")) =>
          val p = Predicate(a, negated=true)
          disjunct.addPredicate(p)

        // here we have an atom with a method select attached
        case Select(lhs, _) =>
          traverse(lhs)

        // here we should have only atoms
        case a: Apply =>
          val p = Predicate(a, negated=false)
          disjunct.addPredicate(p)

        case _ =>
          throw new RuntimeException("Discovered unforeseen structure in Predicate Disjunct!")
      }
    }

    object ConjunctTraverser extends Traverser {
      override def traverse(tree: Tree) = tree match {
        // C1 && C2 && C3 && ... && C4
        case Apply(Select(lhs, TermName("$amp$amp")), rhs) => {
          traverse(lhs)
          traverse(rhs.head)
        }

        // we found a disjunct
        case a: Apply =>
          DisjunctTraverser.traverse(a)
          conjunct += DisjunctTraverser.getDisjunct
          DisjunctTraverser.newDisjunct()

        case _ =>
          //super.traverse(tree)
          //throw new RuntimeException("Discovered unforeseen structure in Predicate Conjunct!")
      }
    }

    ConjunctTraverser.traverse(tree)
    conjunct.map(x => x.getTree)
  }

  case class Predicate(tree: Apply, negated: Boolean) {

    def negate() = Predicate(tree, negated = !this.negated)

    def getTree: Tree = if (negated) Select(tree, TermName("unary_$bang")) else tree

    override def equals(other: Any): Boolean = other match {
      case Predicate(t, n) =>
        tree.equalsStructure(t) && negated == n
      case _ => false
    }

    // TODO implement hashcode
  }

  class Disjunct(val predicates: ListBuffer[Predicate] = ListBuffer.empty[Predicate]) {

    // add predicate if it is not contained yet
    def addPredicate(p: Predicate) = if (!containsPredicate(p)) predicates += p

    def getPredicates = predicates

    def getTree: Option[Tree] = {
      // if this disjunct is always true, we omit it from the final tree
      if (alwaysTrue) return None

      predicates.toList.map(x => x.getTree) match {
        case Nil =>
          None
        case x :: Nil =>
          Some(x)
        case x :: xs =>
          val t: Tree = xs.fold(x)((p1, p2) => q"$p1 || $p2")
          Some(t)
      }
    }

    def containsPredicate(pred: Predicate): Boolean = {
      for ( p <- predicates
            if pred.tree.equalsStructure(p.tree)
            if pred.negated == p.negated) {
        return true
      }

      false
    }

    def alwaysTrue: Boolean = {
      //TODO this should be improvable...
      for (p1 <- predicates;
           p2 <- predicates;
           if p1.tree.equalsStructure(p2.tree);
           if p1.negated != p2.negated) {
        return true
      }
      false
    }
  }

  private def hasGroupType(vd: ValDef) = vd.tpt.tpe match {
    case TypeRef(_, sym, _) if sym == api.groupSymbol => true
    case _ => false
  }

  private def generatorFor(name: TermSymbol, allExpressions: Seq[Expression]) = allExpressions.collectFirst({
    case generator@Generator(lhs, group@combinator.Group(_, _)) if lhs == name => (generator, group)
  })

  private def foldOverSelect(select: Select, expr: Expression) = expr match {
    case fold@combinator.Fold(_, _, _, xs@ScalaExpr(_, _), _) if endsWith(xs.tree, select) => Some(fold)
    case _ => Option.empty[combinator.Fold]
  }

  private def endsWith(tree: Tree, expr: Tree): Boolean = tree match {
    case Block(_, ret) => endsWith(ret, expr)
    case _ => tree == expr
  }

  private def foldGroup(group: combinator.Group, folds: List[combinator.Fold]) = folds match {
    case fold :: Nil =>
      combinator.FoldGroup(group.key, fold.empty, fold.sng, fold.union, group.xs)
    case _ =>
      val elemTpe = group.xs.tpe.typeArgs.head

      // derive the unique product 'empty' function
      val empty = c.typecheck(q"(..${folds.map(_.empty)})")

      // derive the unique product 'sng' function
      val x   = freshName("x$")
      val y   = freshName("y$")
      val sng = q"($x: $elemTpe) => (..${
        for (fold <- folds) yield {
          val sng = fold.sng.as[Function]
          sng.body.rename(sng.vparams.head.name, x)
        }
      })".typeChecked

      // derive the unique product 'union' function
      val union = q"($x: ${empty.tpe}, $y: ${empty.tpe}) => (..${
        for ((fold, i) <- folds.zipWithIndex) yield {
          val union = fold.union.as[Function]
          union.body substitute Map(
            union.vparams.head.name -> q"$x.${TermName(s"_${i + 1}")}",
            union.vparams( 1 ).name -> q"$y.${TermName(s"_${i + 1}")}")
        }
      })".typeChecked

      combinator.FoldGroup(group.key, empty, sng, union, group.xs)
  }

  private class FoldTermsReplacer(val map: Map[Tree, Int], prefix: Tree) extends Transformer {
    override def transform(tree: Tree) = {
      if (map.contains(tree))
        if (map.size == 1)
          prefix
        else
          q"$prefix.${TermName(s"_${map(tree) + 1}")}"
      else
        super.transform(tree)
    }
  }

}
