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
      implicit val comprehendedTerms: mutable.Set[Tree] =
        mutable.Set(stmt collect { case app @ Apply(f, _) if api.methods(f.symbol) => app }: _*)

      // Reduce by removing nodes that will be comprehended with their parent
      var obsolete = mutable.Set.empty[Tree]

      do {
        comprehendedTerms --= obsolete
        obsolete = for {
          term  <- comprehendedTerms
          child <- compChild(term)
        } yield child
      } while (obsolete.nonEmpty)

      // Return the reduced set of applies
      comprehendedTerms
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
  private def compChild(term: Tree)(implicit compTerms: mutable.Set[Tree]) = term match {
    case Apply(f, Function(_, body) :: Nil)
      if (f.symbol == api.map || f.symbol == api.flatMap) && compTerms(body) =>
        Some(body)

    case Apply(Apply(TypeApply(_: Select, _), _), parent :: Nil)
      if compTerms(parent) =>
        Some(parent)

    case Apply(TypeApply(_: Select, _), parent :: Nil)
      if compTerms(parent) =>
        Some(parent)

    case Apply(TypeApply(Select(parent, _), _), _)
      if compTerms(parent) =>
        Some(parent)

    case Apply(Select(parent, _), _)
      if compTerms(parent) =>
        Some(parent)

    case Apply(parent: Apply, _)
      if compTerms(parent) =>
        Some(parent)

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
      case vd @ ValDef(_, _, _,  rhs) if term == rhs => termDef = Some(vd)
      case as @ Assign(_: Ident, rhs) if term == rhs => termDef = Some(as)
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
  private def compTermName(term: Option[Tree], default: TermName) =
    term getOrElse default match {
      case ValDef(_, name, _, _)            => name
      case Assign(Ident(name: TermName), _) => name
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
      case Typed(inner, _) => inner
      case _ => tree
    }) match { // translate based on matched expression type

      // -----------------------------------------------------
      // Monad Ops
      // -----------------------------------------------------

      // xs.map(f)
      case Apply(TypeApply(sel @ Select(xs, _), _ :: Nil), Function(arg :: Nil, body) :: Nil)
        if sel.symbol == api.map =>
          val bind = Generator(arg.term, comprehend(vars)(xs))
          val head = comprehend(arg.reset :: vars)(body, input = false)
          Comprehension(head, bind :: Nil)

      // xs.flatMap(f)
      case Apply(TypeApply(sel @ Select(xs, _), _ :: Nil), Function(arg :: Nil, body) :: Nil)
        if sel.symbol == api.flatMap =>
          val bind = Generator(arg.term, comprehend(vars)(xs))
          val head = comprehend(arg.reset :: vars)(body, input = false)
          MonadJoin(Comprehension(head, bind :: Nil))

      // xs.withFilter(f)
      case Apply(sel @ Select(xs, _), Function(arg :: Nil, body) :: Nil)
        if sel.symbol == api.withFilter =>
          val bind   = Generator(arg.term, comprehend(vars)(xs))
          val filter = Filter(comprehend(arg.reset :: vars)(body))
          val head   = comprehend(arg.reset :: vars)(q"${arg.name}", input = false)
          Comprehension(head, bind :: filter :: Nil)

      // -----------------------------------------------------
      // Grouping and Set operations
      // -----------------------------------------------------

      // xs.groupBy(k)
      case Apply(TypeApply(sel @ Select(xs, _), _ :: Nil), List(k: Function))
        if sel.symbol == api.groupBy =>
          combinator.Group(k, comprehend(Nil)(xs))

      // xs.minus(ys)
      case Apply(TypeApply(sel @ Select(xs, _), _ :: Nil), ys :: Nil)
        if sel.symbol == api.minus =>
          combinator.Diff(comprehend(Nil)(xs), comprehend(Nil)(ys))

      // xs.plus(ys)
      case Apply(TypeApply(sel @ Select(xs, _), _ :: Nil), ys :: Nil)
        if sel.symbol == api.plus =>
          combinator.Union(comprehend(Nil)(xs), comprehend(Nil)(ys))

      // xs.distinct()
      case Apply(sel @ Select(xs, _), Nil)
        if sel.symbol == api.distinct =>
          combinator.Distinct(comprehend(Nil)(xs))

      // -----------------------------------------------------
      // Aggregates
      // -----------------------------------------------------

      // xs.fold(empty, sng, union)
      case tree @ Apply(TypeApply(sel @ Select(xs, _), _), empty :: sng :: union :: Nil)
        if sel.symbol == api.fold =>
          combinator.Fold(empty, sng, union, comprehend(Nil)(xs), tree)
      
      // ----------------------------------------------------------------------
      // Environment & Host Language Connectors
      // ----------------------------------------------------------------------

      // write[T](location, fmt)(xs)
      case Apply(Apply(TypeApply(fn, _ :: Nil), location :: fmt :: Nil), xs :: Nil)
        if fn.symbol == api.write =>
          combinator.Write(location, fmt, comprehend(vars)(xs))

      // read[T](location, fmt)
      case Apply(TypeApply(fn, _ :: Nil), location :: fmt :: Nil)
        if fn.symbol == api.read =>
          combinator.Read(location, fmt)

      // temp result identifier
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
    var valDefs = (for (cv <- compView.terms; d <- cv.definition)
      yield cv.definition collect {
        // Make sure that the associated definition is a non-mutable ValDef
        case vd @ ValDef(mods, _, _, _) if (mods.flags | Flag.MUTABLE) != mods.flags =>
          // Get the identifiers referencing this ValDef symbol
          val ids = tree collect { case id: Ident if id.symbol == d.symbol => id }
          // If the symbol is referenced only once, inline the ValDef rhs in place of the ident
          if (ids.size == 1) Some(vd) else None
      }).flatten.flatten

    while (valDefs.nonEmpty) {
      // Get a ValDef to inline
      val vd = valDefs.head
      // Inline current value definition in the tree
      inlined = inlined inline vd
      // Inline in all other value definitions and continue with those
      valDefs = for (other <- valDefs if other.symbol != vd.symbol)
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
    val groupDefs = tree collect { case vd: ValDef if hasGroupType(vd) => vd }

    // compute a flattened list of all expressions in the comprehension view
    val expressions = compView.terms flatMap { _.comprehension.expr }

    for {
      gDef <- groupDefs if gDef.hasTerm
      (gen, group) <- generatorFor(gDef.term, expressions)
    } {
      // the symbol associated with 'g'
      val gSym = gDef.symbol

      // find all 'g.values' expressions for the group symbol
      val groupSels = tree collect {
        case sel @ Select(id: Ident, TermName("values")) if id.symbol == gSym => sel
      }

      // For each 'g.values' expression, find an associated 'g.values.fold(...)' comprehension,
      // if one exists
      val folds = for {
        sel  <- groupSels
        expr <- expressions
        fold <- foldOverSelect(sel, expr)
      } yield fold

      // !!! All 'g.values' expressions are used directly in a comprehended 'fold' =>
      // apply Fold-Group-Fusion !!!
      if (folds.nonEmpty && groupSels.size == folds.size) {
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
            val enclosed = body.collect { case tree: Tree
              if foldToIndex.contains(tree) => tree -> foldToIndex(tree)
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
        case Apply(Select(q, n), as) => n match {
          // !(A || B) => !A && !B
          case TermName("$bar$bar") => transform(q"(!$q && !${as.head})")
          // !(A && B) => !A || !B
          case TermName("$amp$amp") => transform(q"(!$q || !${as.head})")
          // !A => !A
          // This should cover the cases for atoms that we don't want to reduce further
          case _ => q"!$tree"
        }

        case _ => c.abort(c.enclosingPosition,
          s"Unexpected tree in DeMorgan's transformer: ${showCode(tree)}")
      }

      override def transform(tree: Tree) = tree match {
        case Select(q, TermName("unary_$bang")) => moveNegationsInside(q)
        case _ => super.transform(tree)
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
        case Apply(Select(lhs1, TermName("$bar$bar")), rhs1) =>
          (lhs1, rhs1.head) match {
            // C || (A && B) => (A || C) && (C || B)
            // This also covers:
            // (A && B) || (C && D)
            // => ((A && B) || C) && ((A && B) || D)
            // => (((A || C) && (B || C)) && ((A || D) && (B || D))
            case (_, Apply(Select(lhs2, TermName("$amp$amp")), rhs2)) =>
              val modLhs = transform(q"($lhs1 || $lhs2)")
              val modRhs = transform(q"($lhs1 || ${rhs2.head})")
              q"$modLhs && $modRhs"

            // (A && B) || C => (A || C) && (C || B)
            case (Apply(Select(lhs2, TermName("$amp$amp")), rhs2), _) =>
              val modLhs = transform(q"($lhs2 || ${rhs1.head})")
              val modRhs = transform(q"(${rhs2.head} || ${rhs1.head})")
              q"$modLhs && $modRhs"

            case _ => super.transform(tree)
          }
        
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
    val conjunct = ListBuffer.empty[Disjunct]

    object DisjunctTraverser extends Traverser {
      
      var disjunct = new Disjunct()

      override def traverse(tree: Tree) = tree match {
        // (A || B || ... || Y || Z)
        case Apply(Select(lhs, TermName("$bar$bar")), rhs) =>
          traverse(lhs)
          traverse(rhs.head)

        // Here we have a negated atom
        case Select(app @ Apply(_: Select,_), TermName("unary_$bang")) =>
          disjunct addPredicate Predicate(app, neg = true)

        // Here we have an atom with a method select attached
        case Select(lhs, _) => traverse(lhs)

        // Here we should have only atoms
        case app: Apply =>
          disjunct addPredicate Predicate(app, neg = false)

        case expr => c.abort(c.enclosingPosition,
          s"Unexpected structure in predicate Disjunct: ${showCode(expr)}")
      }
    }

    object ConjunctTraverser extends Traverser {
      override def traverse(tree: Tree) = tree match {
        // C1 && C2 && C3 && ... && C4
        case Apply(Select(lhs, TermName("$amp$amp")), rhs) =>
          traverse(lhs)
          traverse(rhs.head)

        // we found a disjunct
        case app: Apply =>
          DisjunctTraverser traverse app
          conjunct += DisjunctTraverser.disjunct
          DisjunctTraverser.disjunct = new Disjunct()

        case _ =>
          //super.traverse(tree)
          //throw new RuntimeException("Unexpected structure in predicate Conjunct")
      }
    }

    ConjunctTraverser traverse tree
    conjunct.toList map { _.getTree }
  }

  case class Predicate(tree: Apply, neg: Boolean) {

    def negate =
      Predicate(tree, neg = !neg)

    def getTree: Tree =
      if (neg) Select(tree, TermName("unary_$bang")) else tree

    override def equals(that: Any) = that match {
      case Predicate(t, n) => tree.equalsStructure(t) && neg == n
      case _ => false
    }

    //TODO: Implement hashcode
  }

  class Disjunct(predicates: ListBuffer[Predicate] = ListBuffer.empty[Predicate]) {

    // Add predicate if it is not contained yet
    def addPredicate(p: Predicate) =
      if (!predicates.contains(p)) predicates += p
    
    def getTree: Option[Tree] =
      // If this disjunction is always true, we omit it from the final tree
      if (alwaysTrue) None else predicates.map { _.getTree }
        .reduceOption { (p, q) => q"$p || $q" }

    def alwaysTrue: Boolean = {
      for {
        ListBuffer(p, q) <- predicates combinations 2
        if p.tree equalsStructure q.tree
        if p.neg != q.neg
      } return true
      
      false
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
      val x   = freshName("x$")
      val y   = freshName("y$")
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
