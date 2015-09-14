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

  /** A set of API method symbols to be comprehended. */
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
      // Find all value applications on methods from the comprehended API for this statement
      implicit val compTerms: mutable.Set[Tree] =
        mutable.Set(stmt collect { case app @ Apply(f, _) if api methods f.symbol => app }: _*)

      // Reduce by removing nodes that will be comprehended with their parent
      var obsolete = mutable.Set.empty[Tree]

      do {
        compTerms --= obsolete
        obsolete = for {
          term  <- compTerms
          child <- compChild(term)
        } yield child
      } while (obsolete.nonEmpty)

      // Return the reduced set of applies
      compTerms
    }).flatten.toSet

    // Step #2: create ComprehendedTerm entries for the identified terms
    val comprehendedTerms = mutable.Seq((for (term <- terms) yield {
      val name          = freshName("comprehension")
      val definition    = compTermDef(term)
      val comprehension = ExpressionRoot(comprehend(Nil)(term) match {
        case root: combinator.Write => root
        case root: combinator.Fold  => root
        case root: Expression =>
          combinator.TempSink(compTermName(definition, name), root)
      }) ->> normalize

      ComprehendedTerm(name, term, comprehension, definition)
    }).toSeq: _*)

    // Step #3: build the comprehension store
    new ComprehensionView(comprehendedTerms)
  }

  /**
   * Check whether the parent expression in a selector chain is also comprehended.
   *
   * What happens here is that effectively we are looking for "inverse" links as compared to the
   * traversal order in the "comprehend" method.
   *
   * @param term The expression to be checked
   * @param compTerms The set of comprehended terms
   * @return An option holding the comprehended parent term (if such exists)
   */
  // FIXME: Make this consistent with the `comprehend` method patterns
  private def compChild(term: Tree)
      (implicit compTerms: mutable.Set[Tree]): Option[Tree] = term match {
    case q"${f: Tree}[$_](($_) => $body)"
      if (f.symbol == api.map || f.symbol == api.flatMap) && compTerms(body) =>
        Some(body)

    case q"$_[..$_](..$_)($parent)"
      if compTerms(parent) => Some(parent)

    case q"$_[..$_]($parent)"
      if compTerms(parent) => Some(parent)

    case q"$parent.$_[..$_](...$_)"
      if compTerms(parent) => Some(parent)

    case q"${parent: Apply}(...$_)"
      if compTerms(parent) => Some(parent)

    case _ => None
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

  /**
   * Look up a definition term ([[ValDef]] or [[Assign]]) for a comprehended term.
   *
   * @param term The term to lookup
   * @return The name of the term
   */
  private def compTermName(term: Option[Tree], default: TermName): TermName =
    term getOrElse default match {
      case q"$_ val $name: $_ = $_"  => name
      case q"$_ var $name: $_ = $_"  => name
      case q"${name: TermName} = $_" => name
      case _ => default
    }

  // --------------------------------------------------------------------------
  // Comprehension Constructor
  // --------------------------------------------------------------------------

  /**
   * Recursive comprehend method.
   *
   * @param vars The variable environment for the currently lifted tree
   * @param tree The tree to be lifted
   * @return A lifted, MC syntax version of the given tree
   */
  private def comprehend(vars: List[ValDef])(tree: Tree, input: Boolean = true): Expression =
    (tree match { // Ignore a top-level Typed node (side-effect of the Algebra inlining macros)
      case q"${inner: Tree}: $_" => inner
      case _ => tree
    }) match { // translate based on matched expression type

      // -----------------------------------------------------
      // Monad Ops
      // -----------------------------------------------------

      // xs.map(f)
      case q"${map @ Select(xs, _)}[$_]((${arg: ValDef}) => $body)"
        if map.symbol == api.map =>
          val bind = Generator(arg.term, comprehend(vars)(xs))
          val head = comprehend(arg.reset :: vars)(body, input = false)
          Comprehension(head, bind :: Nil)

      // xs.flatMap(f)
      case q"${flatMap @ Select(xs, _)}[$_]((${arg: ValDef}) => $body)"
        if flatMap.symbol == api.flatMap =>
          val bind = Generator(arg.term, comprehend(vars)(xs))
          val head = comprehend(arg.reset :: vars)(body, input = false)
          MonadJoin(Comprehension(head, bind :: Nil))

      // xs.withFilter(f)
      case q"${withFilter @ Select(xs, _)}((${arg: ValDef}) => $body)"
        if withFilter.symbol == api.withFilter =>
          val bind   = Generator(arg.term, comprehend(vars)(xs))
          val filter = Filter(comprehend(arg.reset :: vars)(body))
          val head   = comprehend(arg.reset :: vars)(q"${arg.name}", input = false)
          Comprehension(head, bind :: filter :: Nil)

      // -----------------------------------------------------
      // Grouping and Set operations
      // -----------------------------------------------------

      // xs.groupBy(key)
      case q"${groupBy @ Select(xs, _)}[$_]($key)"
        if groupBy.symbol == api.groupBy =>
          combinator.Group(key, comprehend(Nil)(xs))

      // xs.minus(ys)
      case q"${minus @ Select(xs, _)}[$_]($ys)"
        if minus.symbol == api.minus =>
          combinator.Diff(comprehend(Nil)(xs), comprehend(Nil)(ys))

      // xs.plus(ys)
      case q"${plus @ Select(xs, _)}[$_]($ys)"
        if plus.symbol == api.plus =>
          combinator.Union(comprehend(Nil)(xs), comprehend(Nil)(ys))

      // xs.distinct()
      case q"${distinct @ Select(xs, _)}()"
        if distinct.symbol == api.distinct =>
          combinator.Distinct(comprehend(Nil)(xs))

      // -----------------------------------------------------
      // Aggregates
      // -----------------------------------------------------

      // xs.fold(empty, sng, union)
      case tree @ q"${fold @ Select(xs, _)}[$_]($empty, $sng, $union)"
        if fold.symbol == api.fold =>
          combinator.Fold(empty, sng, union, comprehend(Nil)(xs), tree)

      // ----------------------------------------------------------------------
      // Environment & Host Language Connectors
      // ----------------------------------------------------------------------

      // write[T](loc, fmt)(xs)
      case q"${write: Tree}[$_]($loc, $fmt)($xs)"
        if write.symbol == api.write =>
          combinator.Write(loc, fmt, comprehend(vars)(xs))

      // read[T](loc, fmt)
      case q"${read: Tree}[$_]($loc, $fmt)"
        if read.symbol == api.read =>
          combinator.Read(loc, fmt)

      // Temp result identifier
      case id: Ident if input =>
        combinator.TempSource(id)

      // Interpret as boxed Scala expression (default)
      // Trees created by the caller with q"..." have to be explicitly type-checked
      case expr => ScalaExpr(vars, typeCheckWith(vars, expr))
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
        case vd @ q"$_ val $_: $_ = $_" =>
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

    // find the symbols of all "Group[K, V]" definitions 'g'
    val groupDefinitions = tree collect { case vd: ValDef if hasGroupType(vd) => vd }

    // compute a flattened list of all expressions in the comprehension view
    val expressions = compView.terms flatMap { _.comprehension.expr }

    for {
      gDef <- groupDefinitions if gDef.hasTerm
      (gen, group) <- generatorFor(gDef.term, expressions)
    } {
      // the symbol associated with 'g'
      val gSym = gDef.symbol

      // find all 'g.values' expressions for the group symbol
      val groupSelects = tree collect {
        case sel @ q"${id: Ident}.values" if id.symbol == gSym => sel.as[Select]
      }

      // For each 'g.values' expression, find an associated 'g.values.fold(...)' comprehension,
      // if one exists
      val folds = for {
        sel  <- groupSelects
        expr <- expressions
        fold <- foldOverSelect(sel, expr)
      } yield fold

      // !!! All 'g.values' expressions are used directly in a comprehended 'fold' =>
      // apply Fold-Group-Fusion !!!
      if (folds.nonEmpty && groupSelects.size == folds.size) {
        // Create an auxiliary map
        val foldToIndex = folds.map { _.origin }.zipWithIndex.toMap

        // 1) Fuse the group with the folds and replace the Group with a GroupFold in the
        // enclosing generator
        gen.rhs = foldGroup(group, folds)

        // 2) Replace the comprehended fold expressions
        for (expr <- expressions) expr match {
          // Adapt Scala expression nodes referencing the group
          case expr @ ScalaExpr(vars, body) if vars exists { _.name == gDef.name } =>
            // Find all value selects with associated enclosed in this ScalaExpr
            val enclosed = body.collect {
              case t if foldToIndex contains t => t -> foldToIndex(t)
            }.toMap

            if (enclosed.nonEmpty) {
              expr.vars = for (v <- vars) yield if (v.name == gDef.name)
                mk.valDef(gDef.name, gen.rhs.elementType) else v

              val replacer = new FoldTermReplacer(enclosed, q"${gDef.name}.values")
              expr.tree    = typeCheckWith(vars, replacer transform body)
            }

          // Adapt comprehensions that contain the fold as a head
          case expr @ Comprehension(fold: combinator.Fold, _) if folds contains fold =>
            // Find all value selects with associated enclosed in this ScalaExpr
            val enclosed = Map(fold.origin -> foldToIndex(fold.origin))
            val vars     = mk.valDef(gDef.name, gen.rhs.elementType) :: Nil
            val replacer = new FoldTermReplacer(enclosed, q"${gDef.name}.values")
            val body     = typeCheckWith(vars, replacer transform fold.origin)
            expr.hd      = ScalaExpr(vars, body)

          // Adapt comprehensions that contain the fold as a filter
          case expr @ Filter(fold: combinator.Fold) if folds contains fold =>
            // Find all value selects with associated enclosed in this ScalaExpr
            val enclosed = Map(fold.origin -> foldToIndex(fold.origin))
            val vars     = mk.valDef(gDef.name, gen.rhs.elementType) :: Nil
            val replacer = new FoldTermReplacer(enclosed, q"${gDef.name}.values")
            val body     = typeCheckWith(vars, replacer transform fold.origin)
            expr.expr    = ScalaExpr(vars, body)

          case _ => // Ignore the rest
        }
      }
    }
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
      case Filter(ScalaExpr(vars, x)) =>
        // Normalize the tree
        (x ->> applyDeMorgan ->> distributeOrOverAnd ->> cleanConjuncts)
          .collect { case Some(nf) => Filter(ScalaExpr(vars, nf))}

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

  private def hasGroupType(vd: ValDef) = vd.tpt.tpe match {
    case TypeRef(_, sym, _) => sym == api.groupSymbol
    case _ => false
  }

  private def generatorFor(name: TermSymbol, expressions: Seq[Expression]) =
    expressions collectFirst {
      case gen @ Generator(lhs, group: combinator.Group)
        if lhs == name => (gen, group)
    }

  private def foldOverSelect(sel: Select, expr: Expression) = expr match {
    case fold @ combinator.Fold(_, _, _, ScalaExpr(_, tree), _)
      if endsWith(tree, sel) => Some(fold)
    
    case _ => None
  }

  private def endsWith(tree: Tree, expr: Tree): Boolean = tree match {
    case Block(_, last) => endsWith(last, expr)
    case _ => tree == expr
  }

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
          val union = f.union.as[Function]
          union.body substitute Map(
            union.vparams.head.name -> q"$x.${TermName(s"_${i + 1}")}",
            union.vparams( 1 ).name -> q"$y.${TermName(s"_${i + 1}")}")
        }
      })".typeChecked

      combinator.FoldGroup(key, empty, sng, union, xs)
  }

  private class FoldTermReplacer(count: Map[Tree, Int], prefix: Tree) extends Transformer {
    override def transform(tree: Tree) = {
      if (count contains tree)
        if (count.size == 1) prefix
        else q"$prefix.${TermName(s"_${count(tree) + 1}")}"
      else super.transform(tree)
    }
  }
}
