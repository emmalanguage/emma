package eu.stratosphere.emma.macros.program.comprehension

import eu.stratosphere.emma.macros.program.comprehension.rewrite.ComprehensionNormalization
import eu.stratosphere.emma.macros.program.controlflow.ControlFlowModel
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.language.postfixOps

private[emma] trait ComprehensionAnalysis
    extends ControlFlowModel
    with ComprehensionModel
    with ComprehensionNormalization {

  import universe._

  // --------------------------------------------------------------------------
  // Comprehension Store Constructor
  // --------------------------------------------------------------------------

  /**
   * Comprehension store constructor.
   *
   * @param cfGraph The control flow graph for the comprehended algorithm
   * @return The comprehension structure for the algorithm
   */
  def createComprehensionView(cfGraph: CFGraph): ComprehensionView = {

    // Step #1: compute the set of maximal terms that can be translated to comprehension syntax
    val root = cfGraph.nodes.find { _.inDegree == 0 }.get
    // Implicit helpers
    implicit val cfBlockTraverser: CFGraph#OuterNodeTraverser =
      root.outerNodeTraverser()

    // Step #1: compute the set of maximal terms that can be translated to comprehension syntax
    val terms = (for (block <- cfBlockTraverser; stmt <- block.stats) yield {
      val builder = Set.newBuilder[(TermName, Tree)]
      // Find all top-level applications on methods from the comprehended API for this statement
      stmt traverse {
        // within an enclosing immutable ValDef
        case vd @ q"$_ val $_: $_ = ${ComprehendableTerm(app)}" =>
          val symb = vd.asInstanceOf[ValDef].term
          builder += symb.name -> app
        // within an enclosing mutable ValDef
        case vd @ q"$_ var $_: $_ = ${ComprehendableTerm(app)}" =>
          val symb = vd.asInstanceOf[ValDef].term
          builder += symb.name -> app
        // within an enclosing Assign
        case as @ q"${_: Ident} = ${ComprehendableTerm(app)}" =>
          val symb = as.asInstanceOf[Assign].lhs.term
          builder += symb.name -> app
        // anonymous term
        case app @ Apply(f, _)
          if api methods f.symbol =>
            builder += freshName("anon") -> app
      }
      builder.result()
    }).flatten.toSet

    // Step #2: create ComprehendedTerm entries for the identified terms
    val comprehendedTerms = (for {
      (name, term) <- terms
    } yield {
      val compName      = freshName(s"$name$$comp")
      val definition    = compTermDef(term)
      val comprehension = ExpressionRoot(comprehend(term, topLevel = true) match {
        case root: combinator.Write          => root
        case root: combinator.Fold           => root
        case root: combinator.StatefulCreate => root
        case root: Expression                => combinator.TempSink(name, root)
      }) ->> normalize

      ComprehendedTerm(compName, term, comprehension, definition)
    }).toSeq

    // Step #3: build the comprehension store
    new ComprehensionView(mutable.Seq(comprehendedTerms: _*))
  }

  object ComprehendableTerm {
    def unapply(tree: Tree): Option[Tree] = {
      tree match {
        case q"${app @ Apply(f, _)}"
          if api methods f.symbol =>
            Some(app)
        case q"${app @ Apply(f, _)}: $_"
          if api methods f.symbol =>
            Some(app)
        case _ =>
          None
      }
    }
  }

  /**
   * Look up a definition term ([[ValDef]] or [[Assign]]) for a comprehended term.
   *
   * @param term The term to lookup
   * @return The (optional) definition for the term
   */
  private def compTermDef(term: Tree)(implicit cfBlocks: TraversableOnce[CFBlock]) = {
    var termDef = Option.empty[Tree]
    for (block <- cfBlocks; stmt <- block.stats) stmt foreach {
      case vd @ q"$_ val $_: $_ = ${`term`}" => termDef = Some(vd)
      case vd @ q"$_ var $_: $_ = ${`term`}" => termDef = Some(vd)
      case as @ q"${_: Ident} = ${`term`}"   => termDef = Some(as)
      case _ =>
    }

    termDef
  }

  // --------------------------------------------------------------------------
  // Comprehension Constructor
  // --------------------------------------------------------------------------

  /**
   * Recursive comprehend method.
   *
   * @param tree The tree to be lifted
   * @return A lifted, MC syntax version of the given tree
   */
  // FIXME: Replace flag parameters with appropriate use of partial functions
  private def comprehend(tree: Tree, input: Boolean = true, topLevel: Boolean = false): Expression =
    tree.unAscribed match { // translate based on matched expression type

      // -----------------------------------------------------
      // Monad Ops
      // -----------------------------------------------------

      // xs.map(f)
      case q"${map @ Select(xs, _)}[$_]((${arg: ValDef}) => $body)"
        if map.symbol == api.map =>
          val bind = Generator(arg.term, comprehend(xs))
          val head = comprehend(body, input = false)
          Comprehension(head, bind :: Nil)

      // xs.flatMap(f)
      case q"${flatMap @ Select(xs, _)}[$_]((${arg: ValDef}) => $body)"
        if flatMap.symbol == api.flatMap =>
          val bind = Generator(arg.term, comprehend(xs))
          val head = comprehend(body, input = false)
          MonadJoin(Comprehension(head, bind :: Nil))

      // xs.withFilter(f)
      case q"${withFilter @ Select(xs, _)}((${arg: ValDef}) => $body)"
        if withFilter.symbol == api.withFilter =>
          val bind   = Generator(arg.term, comprehend(xs))
          val filter = Filter(comprehend(body))
          val head   = comprehend(mk ref arg.term, input = false)
          Comprehension(head, bind :: filter :: Nil)

      // -----------------------------------------------------
      // Grouping and Set operations
      // -----------------------------------------------------

      // xs.groupBy(key)
      case q"${groupBy @ Select(xs, _)}[$_]($key)"
        if groupBy.symbol == api.groupBy =>
          combinator.Group(key, comprehend(xs))

      // xs.minus(ys)
      case q"${minus @ Select(xs, _)}[$_]($ys)"
        if minus.symbol == api.minus =>
          combinator.Diff(comprehend(xs), comprehend(ys))

      // xs.plus(ys)
      case q"${plus @ Select(xs, _)}[$_]($ys)"
        if plus.symbol == api.plus =>
          combinator.Union(comprehend(xs), comprehend(ys))

      // xs.distinct()
      case q"${distinct @ Select(xs, _)}()"
        if distinct.symbol == api.distinct =>
          combinator.Distinct(comprehend(xs))

      // -----------------------------------------------------
      // Aggregates
      // -----------------------------------------------------

      // xs.fold(empty, sng, union)
      case tree @ q"${fold @ Select(xs, _)}[$_]($empty)($sng, $union)"
        if topLevel && fold.symbol == api.fold =>
          combinator.Fold(empty, sng, union, comprehend(xs), tree)

      // ----------------------------------------------------------------------
      // Environment & Host Language Connectors
      // ----------------------------------------------------------------------

      // write[T](loc, fmt)(xs)
      case q"${write: Tree}[$_]($loc, $fmt)($xs)"
        if write.symbol == api.write =>
          combinator.Write(loc, fmt, comprehend(xs))

      // read[T](loc, fmt)
      case q"${read: Tree}[$_]($loc, $fmt)"
        if read.symbol == api.read =>
          combinator.Read(loc, fmt)

      // Temp result identifier
      case id: Ident if input =>
        combinator.TempSource(id)

      // -----------------------------------------------------
      // Stateful data bags
      // -----------------------------------------------------

      // stateful[S,K](xs)
      case q"${stateful: Tree}[${stateType: TypeTree}, ${keyType: TypeTree}]($xs)"
        if stateful.symbol == api.stateful =>
          combinator.StatefulCreate(comprehend(xs), stateType.tpe, keyType.tpe)

      // stateful.bag()
      case q"${fetchToStateless @ Select(ident @ Ident(_), _)}()"
        if fetchToStateless.symbol == api.fetchToStateless =>
          combinator.StatefulFetch(ident)

      // stateful.updateWithZero(udf)
      case q"${updateWithZero @ Select(ident @ Ident(_), _)}[$_]($udf)"
        if updateWithZero.symbol == api.updateWithZero =>
          combinator.UpdateWithZero(ident, udf)

      // stateful.updateWithOne(updates)(keySel, udf)
      case q"${updateWithOne @ Select(ident @ Ident(_), _)}[$_, $_]($updates)($keySel, $udf)"
        if updateWithOne.symbol == api.updateWithOne =>
          combinator.UpdateWithOne(ident, comprehend(updates), keySel, udf)

      // stateful.updateWithMany(updates)(keySel, udf)
      case q"${updateWithMany @ Select(ident @ Ident(_), _)}[$_, $_]($updates)($keySel, $udf)"
        if updateWithMany.symbol == api.updateWithMany =>
          combinator.UpdateWithMany(ident, comprehend(updates), keySel, udf)


      // Interpret as boxed Scala expression (default)
      // Trees created by the caller with q"..." have to be explicitly type-checked
      case expr => ScalaExpr(expr)
    }

  // --------------------------------------------------------------------------
  // Logical Optimizations
  // --------------------------------------------------------------------------

  /**
   * Inline comprehended [[ValDef]]s occurring only once with their parents.
   *
   * @param tree The original program [[Tree]]
   * @param cfGraph The control flow graph for the comprehended algorithm
   * @param compView A view over the comprehended terms in the [[Tree]]
   * @return An inlined copy of the [[Tree]]
   */
  def inlineComprehensions(tree: Tree)
      (implicit cfGraph: CFGraph, compView: ComprehensionView): Tree = {

    var inlined = tree

    // Find all value definitions that can be inlined
    var definitions = (for (cv <- compView.terms; d <- cv.definition)
      yield cv.definition collect {
        // Make sure that the associated definition is a non-mutable ValDef
        case vd @ q"$_ val $_: ${defType: TypeTree} = $_"
          // Don't inline Stateful terms
          if defType.tpe.erasure.typeSymbol.asClass != api.statefulSymbol =>
          // Get the identifiers referencing this ValDef symbol
          val ids = tree collect { case id: Ident if id.symbol == d.symbol => id }
          // If the symbol is referenced only once, inline the ValDef rhs in place of the ident
          if (ids.size == 1) Some(vd.as[ValDef]) else None
      }).flatten.flatten

    while (definitions.nonEmpty) {
      // Get a ValDef to inline
      val vd  = definitions.head
      // Inline current value definition in the tree
      inlined = inlined inline vd
      // Inline in all other value definitions and continue with those
      definitions = for (other <- definitions if other.symbol != vd.symbol)
        yield other.inline(vd).as[ValDef]
    }

    inlined.typeChecked
  }

  /**
   * Perform Fold-Group-Fusion in-place.
   *
   * @param tree The original program [[Tree]]
   * @param cfGraph The control flow graph for the comprehended algorithm
   * @param compView A view over the comprehended terms in the [[Tree]]
   */
  def foldGroupFusion(tree: Tree)
      (implicit cfGraph: CFGraph, compView: ComprehensionView): Unit = {

    for {
      com <- compView.terms // For each comprehended term
      gen <- com.comprehension.expr // For each of it's sub-expressions
    } gen match {
      // If this is a generator binding a proper group
      case gen@Generator(gSym, group: combinator.Group) if hasProperGroupType(gen.tpe) =>

        // Check if all 'group.values' usages within the enclosing comprehensions are of type "fold"
        val groupValuesUsages = (for (ctx <- com.comprehension.trees) yield ctx.collect {
          case grpVals @ q"${id: Ident}.values" if id.symbol == gSym =>
            (grpVals, findEnclosingFold(grpVals, ctx))
        }).flatten.toList

        // Check if all 'group.values' usages within the enclosing comprehensions are of type "fold"
        val allGroupValuesUsedInFold = groupValuesUsages forall { case (_, enclosingFold) => enclosingFold.isDefined }

        if (groupValuesUsages.nonEmpty && allGroupValuesUsedInFold) {
          val comprehendedGroupValuesUsages = for {
            ((grpVals, foldOpt), idx) <- groupValuesUsages.zipWithIndex
            (foldTree)                <- foldOpt
          } yield {
            val foldComp = ExpressionRoot(comprehend(foldTree, topLevel = true)) ->> normalize
            (idx, grpVals, foldTree, foldComp)
          }

          // Project out folds
          val folds = for {
            (_, _, _, foldComp) <- comprehendedGroupValuesUsages
          } yield foldComp.expr.as[combinator.Fold]

          // Fuse the group with the folds and replace the Group with a GroupFold in the enclosing generator
          val fGrp = foldGroup(group, folds)
          val gNew = mk.valDef(gSym.name, fGrp.elementType)
          gen.rhs  = fGrp
          gen.lhs  = gNew.term

          // For each 'foldTree' -> 'idx' pair
          for ((idx, _, foldTree, _) <- comprehendedGroupValuesUsages) {
            // Replace 'foldTree' with '${valSel}._${idx}' in all trees that can be found under the enclosing 'com'
            val replTree = if (comprehendedGroupValuesUsages.length > 1) {
              q"${mk ref gen.lhs}.values.${TermName(s"_${idx + 1}")}".typeChecked
            } else {
              q"${mk ref gen.lhs}.values".typeChecked
            }
            com.comprehension.substitute(foldTree, replTree)
          }

          com.comprehension.substitute(gSym, gen.lhs)
        }

      // Ignore other expression types
      case _ =>
    }
  }

  /**
   * A quick and dirty solution for a deterministic automaton that recognizes a
   *
   * {{{ sel ->> [ withFilter | map ]* ->> fold }}}
   *
   * set of terms in the given `context`. The set of interesting terms are thereby precisely the ones in which
   * the given `sel` tree is followed by arbitrary many `withFilter` or `map` invocations and ends with a `fold`.
   *
   * @param sel The bottom of the tree.
   * @param ctx The context within
   * @return
   */
  def findEnclosingFold(sel: Tree, ctx: Tree) = {
    var state: scala.Symbol = 'find_fold
    var res: Option[Tree]   = Option.empty[Tree]

    val traverser = new Traverser {
      override def traverse(tree: Tree): Unit = state match {
        case 'find_fold => tree match {
          // xs.fold(empty)(sng, union)
          case tree @ q"${fold @ Select(xs, _)}[$_](...$_)"
            if fold.symbol == api.fold =>
              state = 'find_homomorphisms
              res = Some(tree)
              traverse(xs)
          case _ =>
            super.traverse(tree)
        }
        case 'find_homomorphisms => tree match {
          // xs.withFilter(f)
          case q"${withFilter @ Select(xs, _)}($_)"
            if withFilter.symbol == api.withFilter =>
              traverse(xs)
          // xs.map(f)
          case tree @ q"${map @ Select(xs, _)}[$_]($_)"
            if map.symbol == api.map =>
              traverse(xs)
          case _ =>
            state = 'find_identifier
            traverse(tree)
        }
        case 'find_identifier => tree match {
          // xs.withFilter(f)
          case tree @ q"${_: Ident}.values"
            if tree == sel =>
              state = 'success
              super.traverse(tree)
          case _ =>
            state = 'find_fold
            res = Option.empty[Tree]
            traverse(tree)
        }
        case _ =>
          super.traverse(tree)
      }
    }

    traverser.traverse(ctx)
    res
  }

  /** Check if the type matches the type pattern `Group[K, DataBag[A]]`. */
  private def hasProperGroupType(tpe: Type) =
    tpe.typeSymbol == api.groupSymbol && tpe.typeArgs(1).typeSymbol == api.bagSymbol

  /** Fuse a Group combinator with a list of Folds and return the resulting FoldGroup combinator. */
  private def foldGroup(group: combinator.Group, folds: List[combinator.Fold]) = folds match {
    case fold :: Nil => combinator.FoldGroup(group.key, fold.empty, fold.sng, fold.union, group.xs)
    case _ =>
      val combinator.Group(key, xs) = group
      // Derive the unique product 'empty' function
      val empty = q"(..${for (f <- folds) yield f.empty})".typeChecked

      // Derive the unique product 'sng' function
      val x   = freshName("x")
      val y   = freshName("y")
      val sng = q"($x: ${xs.elementType}) => (..${
        for (f <- folds) yield {
          val sng = f.sng.as[Function]
          sng.body.rename(sng.vparams.head.name, x)
        }
      })".typeChecked

      // Derive the unique product 'union' function
      val union = q"($x: ${empty.tpe}, $y: ${empty.tpe}) => (..${
        for ((f, i) <- folds.zipWithIndex) yield {
          val tpe   = empty.tpe typeArgs i
          val union = f.union.as[Function]
          union.body substitute Map(
            union.vparams.head.name ->
              q"$x.${TermName(s"_${i + 1}")}".withType(tpe),

            union.vparams(1).name ->
              q"$y.${TermName(s"_${i + 1}")}".withType(tpe))
        }
      })".typeChecked

      combinator.FoldGroup(key, empty, sng, union, xs)
  }

  /**
   * Normalize (in-place) the `if` predicates of all for comprehensions in a [[Tree]].
   *
   * @param tree The [[Tree]] to normalize
   * @param cfGraph The control flow graph for the comprehended algorithm
   * @param compView A view over the comprehended terms in the [[Tree]]
   * @return The [[Tree]] with all filter predicates normalized in-place
   */
  def normalizePredicates(tree: Tree)
      (implicit cfGraph: CFGraph, compView: ComprehensionView) = {

    for {
      ComprehendedTerm(_, _, ExpressionRoot(expr), _) <- compView.terms
      comprehension @ Comprehension(_, qualifiers)    <- expr
    } comprehension.qualifiers = qualifiers flatMap {
      case Filter(ScalaExpr(x)) =>
        // Normalize the tree
        (x ->> applyDeMorgan ->> distributeOrOverAnd ->> cleanConjuncts)
          .collect { case Some(nf) => Filter(ScalaExpr(nf))}

      case q => q :: Nil
    }

    tree
  }

  /**
   * Apply DeMorgan's Rules to predicates (move negations as far in as possible).
   * 
   * @param tree The [[Tree]] to normalize
   * @return A copy of the normalized [[Tree]]
   */
  def applyDeMorgan(tree: Tree): Tree = {
    object DeMorganTransformer extends Transformer {
      def moveNegationsInside(tree: Tree): Tree = tree match {
        case q"$p || $q" => transform(q"!$p && !$q")
        case q"$p && $q" => transform(q"!$p || !$q")
        case _           => q"!$tree"
      }

      override def transform(tree: Tree) = tree match {
        case q"!$p" => moveNegationsInside(p)
        case _      => super.transform(tree)
      }
    }

    DeMorganTransformer transform tree
  }

  /**
   * Distribute ∨ inwards over ∧. Repeatedly replace B ∨ (A ∧ C) with (B ∨ A) ∧ (B ∨ C).
   *
   * @param tree The [[Tree]] to be normalized
   * @return A copy of the [[Tree]] with the distribution law applied
   */
  def distributeOrOverAnd(tree: Tree): Tree = {
    object DistributeTransformer extends Transformer {
      override def transform(tree: Tree): Tree = tree match {
        case q"$a || ($b && $c)" => q"${transform(q"$a || $b")} && ${transform(q"$a || $c")}"
        case q"($a && $b) || $c" => q"${transform(q"$a || $c")} && ${transform(q"$b || $c")}"
        case _ => super.transform(tree)
      }
    }

    DistributeTransformer transform tree
  }

  /**
   * Checks the conjunctions for always true statements and duplicates and cleans them up.
   *
   * This object gets single disjunctions (A || B || C) and removes duplicates.
   * If the disjunction is always true, e.g. (A || !A || B || C), it gets removed.
   *
   * (¬B ∨ B ∨ ¬A) ∧ (¬A ∨ ¬C ∨ B ∨ ¬A) => ¬A ∨ ¬C ∨ B
   * @param tree The conjunct [[Tree]]
   * @return  A list of `Option[Tree]` where each option stands for one disjunction (A || ... || Z).
   *          If the disjunction is always true (e.g. (A || !A)) we can remove it and the Option
   *          will be `None`. In case the disjunction is not always true, the list will contain
   *          `Some(tree)` with the tree of the cleaned/reduced disjunction.
   */
  def cleanConjuncts(tree: Tree): List[Option[Tree]] = {
    val conjunctions = ListBuffer.empty[Disjunction]

    object DisjunctTraverser extends Traverser {
      var disjunction = new Disjunction()

      override def traverse(tree: Tree) = tree match {
        // (A || B || ... || Y || Z)
        case q"$p || $q" =>
          traverse(p); traverse(q)

        // Here we have a negated atom
        case q"!${app: Apply}" =>
          disjunction addPredicate Predicate(app, neg = true)

        // Here we have an atom with a method select attached
        case q"$lhs.$_" =>
          traverse(lhs)

        // Here we should have only atoms
        case app: Apply =>
          disjunction addPredicate Predicate(app, neg = false)

        case expr => c.abort(c.enclosingPosition,
          s"Unexpected structure in predicate disjunction: ${showCode(expr)}")
      }
    }

    object ConjunctTraverser extends Traverser {
      override def traverse(tree: Tree) = tree match {
        // C1 && C2 && C3 && ... && C4
        case q"$p && $q" =>
          traverse(p); traverse(q)

        // We found a disjunction
        case app: Apply =>
          DisjunctTraverser traverse app
          conjunctions += DisjunctTraverser.disjunction
          DisjunctTraverser.disjunction = new Disjunction()

        case _ =>
          //super.traverse(tree)
          //throw new RuntimeException("Unexpected structure in predicate conjunction")
      }
    }

    ConjunctTraverser traverse tree
    conjunctions.toList map { _.getTree }
  }

  case class Predicate(tree: Apply, neg: Boolean) {

    def negate =
      Predicate(tree, neg = !neg)

    def getTree: Tree =
      if (neg) q"!$tree" else tree

    override def equals(that: Any) = that match {
      case Predicate(t, n) => tree.equalsStructure(t) && neg == n
      case _ => false
    }

    // TODO: Implement hashcode
  }

  class Disjunction(predicates: ListBuffer[Predicate] = ListBuffer.empty) {

    // Add predicate if it is not contained yet
    def addPredicate(p: Predicate) =
      if (!predicates.contains(p)) predicates += p
    
    def getTree: Option[Tree] =
      // If this disjunction is always true, we omit it from the final tree
      if (alwaysTrue) None else predicates.map { _.getTree }
        .reduceOption { (p, q) => q"$p || $q" }

    def alwaysTrue: Boolean = predicates combinations 2 exists {
      case ListBuffer(p, q) => p.neg != q.neg && p.tree.equalsStructure(q.tree)
    }
  }
}
