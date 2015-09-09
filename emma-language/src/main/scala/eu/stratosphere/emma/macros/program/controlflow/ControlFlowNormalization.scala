package eu.stratosphere.emma.macros.program.controlflow

import eu.stratosphere.emma.macros.program.util.ProgramUtils
import eu.stratosphere.emma.util.Counter

private[emma] trait ControlFlowNormalization extends ProgramUtils {
  import universe._

  /** Normalize */
  def normalize(tree: Tree): Tree = {
    c.typecheck( q"""{
      import _root_.scala.reflect._
      ${normalizeControlFlowTests(untypecheck(/*normalizeClassNames*/ (normalizeEnclosingParameters(tree))))}
    }""").asInstanceOf[Block].expr
  }

  // --------------------------------------------------------------------------
  // Normalize control flow condition tests.
  // --------------------------------------------------------------------------

  /** Normalizes the tests in an expression tree.
    *
    * This process includes:
    *
    * - unnesting of compex trees ouside while loop tests.
    * - unnesting of compex trees ouside do-while loop tests.
    * - unnesting of compex trees ouside if-then-else conditions.
    */
  object normalizeControlFlowTests extends Transformer with (Tree => Tree) {
    val testCounter = new Counter()

    override def transform(tree: Tree): Tree = tree match {
      // while (`cond`) { `body` }
      case LabelDef(_, _, If(cond, Block(body, _), _)) =>
        cond match {
          case Ident(_: TermName) =>
            // if condition is a simple identifier, no normalization needed
            super.transform(tree)
          case _ =>
            // introduce condition variable
            val condVar = TermName(f"testA${testCounter.advance.get}%03d")
            // move the complex test outside the condition
            q"{ var $condVar = $cond; while ($condVar) { $body; $condVar = $cond } }"
        }

      // do { `body` } while (`cond`)
      case LabelDef(_, _, Block(body, If(cond, _, _))) =>
        cond match {
          case Ident(_: TermName) =>
            // if condition is a simple identifier, no normalization needed
            super.transform(tree)
          case _ =>
            // introduce condition variable
            val condVar = TermName(f"testB${testCounter.advance.get}%03d")
            // move the complex test outside the condition
            q"{ var $condVar = null.asInstanceOf[Boolean]; do { $body; $condVar = $cond } while ($condVar) }"
        }

      // if (`cond`) `thenp` else `elsep`
      case If(cond, thenp, elsep) =>
        cond match {
          case Ident(_: TermName) =>
            // if condition is a simple identifier, no normalization needed
            super.transform(tree)
          case _ =>
            // introduce condition value
            val condVal = TermName(f"testC${testCounter.advance.get}%03d")
            // move the complex test outside the condition
            q"val $condVal = $cond; ${If(Ident(condVal), thenp, elsep)}"
        }

      // default case
      case _ =>
        super.transform(tree)
    }

    def apply(tree: Tree): Tree = transform(tree)
  }

  // --------------------------------------------------------------------------
  // Normalize encolsing object parameter access.
  // --------------------------------------------------------------------------

  private class normalizeEnclosingParameters(val classSymbol: Symbol) extends Transformer {
    val aliases = collection.mutable.Map.empty[Symbol, ValDef]

    override def transform(t: Tree) = t match {
      case select@Select(encl@This(_), n) if needsSubstitution(encl, select) =>
        val alias = aliases.getOrElseUpdate(select.symbol, newValDef(TermName(s"__this$$$n"), select.tpe.widen, rhs = Some(select)))
        Ident(alias.symbol)
      case _ =>
        super.transform(t)
    }

    /** Check if a select from an enclosing 'this' needs to be substituted. */
    def needsSubstitution(encl: This, select: Select) = {
      encl.symbol == classSymbol && // enclosing 'this' is a class
        select.symbol.isTerm && // term selection
        (select.symbol.asTerm.isStable || select.symbol.asTerm.isGetter) // getter or val select
    }
  }

  /** Normalize encolsing object parameter access.
    *
    * - Identifies usages of enclosing object parameters.
    * - Replaces the selects with values of the form `__this${name}`.
    * - Assign the accessed parameters to local values of the form `__this${name}` at the beginning of the code.
    */
  object normalizeEnclosingParameters extends (Tree => Tree) {

    def apply(root: Tree): Tree = {
      val ownerClassSym = getOwnerClass(c.internal.enclosingOwner)

      val normalize = new normalizeEnclosingParameters(ownerClassSym) // construct function
      val normalizedTree = normalize.transform(root) // normalize tree and collect alias symbols

      // construct normalized code snippet
      q"""
      ..${normalize.aliases.values.toSeq}
      ..$normalizedTree
      """
    }

    /** Return the first class owner of this symbol.
      *
      * @param c The symbol to test.
      * @return
      */
    private def getOwnerClass(c: Symbol): Symbol = {
      if (c.isClass) c else getOwnerClass(c.owner) // FIXME: this does not have a safe termination criterion
    }
  }

  // --------------------------------------------------------------------------
  // Normalize class names
  // --------------------------------------------------------------------------

  /** Substitutes the names of local classes to fully qualified names. */
  object normalizeClassNames extends Transformer with (Tree => Tree) {

    override def transform(t: Tree): Tree = {
      t match {
        // search for a class call without 'new'
        case a@Apply(Select(i@Ident(_: TermName), termName), args) if !isParam(i.symbol) =>
          val selectChain = mkSelect(i.symbol, apply = true)
          val result = Apply(Select(selectChain, termName), args.map(transform))
          result
        // search for a class call with 'new'
        case a@Apply(Select(n@New(i@Ident(_: TypeName)), termNames.CONSTRUCTOR), args) if !i.symbol.owner.isTerm =>
          val selectChain = mkSelect(i.symbol, apply = false)
          val result = Apply(Select(New(selectChain), termNames.CONSTRUCTOR), args.map(transform))
          result
        case _ =>
          super.transform(t)
      }
    }

    def apply(root: Tree): Tree = transform(root)

    /** Check if the symbol is, a parameter of a function, a `val`, or a `var`. */
    def isParam(symbol: Symbol): Boolean = {
      (symbol.isTerm && (symbol.asTerm.isVal || symbol.asTerm.isVar)) || (symbol.owner.isMethod && symbol.isParameter)
    }

    /** Creates a select chain for a class. Example: Select(Select(Ident("...", ...
      *
      * @param sym root symbol
      * @param apply set to true if a function is applied on the symbol
      * @return Tree with select chain
      */
    def mkSelect(sym: Symbol, apply: Boolean): Tree = {
      if (sym.owner != c.mirror.RootClass) {
        val newSymbol: Name =
          if (apply)
            sym.name.toTermName
          else if (!sym.isPackage)
            sym.name.toTypeName
          else
            sym.name.toTermName

        Select(mkSelect(sym.owner, apply), newSymbol)
      } else {
        Ident(c.mirror.staticPackage(sym.fullName))
      }
    }
  }

  // --------------------------------------------------------------------------
  // Normalize an expression tree.
  // --------------------------------------------------------------------------

  /** Creates a new symbol PARAM ValDef.
    *
    * @param name The name of the new term.
    * @param tpe The type of the new term.
    * @param owner The owner symbol of this ValDef.
    */
  def newValDef(name: TermName, tpe: Type, owner: Symbol = c.internal.enclosingOwner, flags: FlagSet = NoFlags, rhs: Option[Tree] = Option.empty[Tree]) = {
    val symbol = c.internal.newTermSymbol(c.internal.enclosingOwner, name, flags = flags)
    val valdef =
      if (rhs.isDefined)
        c.internal.valDef(c.internal.setInfo(symbol, tpe), rhs.get)
      else
        c.internal.valDef(c.internal.setInfo(symbol, tpe))
    valdef
  }
}
