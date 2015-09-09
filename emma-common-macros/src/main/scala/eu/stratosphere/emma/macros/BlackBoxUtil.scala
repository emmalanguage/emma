package eu.stratosphere.emma.macros

/**
 * Implements various utility functions that mitigate and/or workaround deficiencies in Scala's
 * macros APIs, e.g. non- idempotent type checking, lack of hygiene, capture-avoiding substitution,
 * fully-qualified names, fresh name generation, identifying closures, etc.
 */
trait BlackBoxUtil extends BlackBox with ReflectUtil {
  import universe._
  import c.internal._

  def parse(str: String) =
    c.parse(str)

  def typeCheck(tree: Tree) =
    if (tree.isType) c.typecheck(tree, c.TYPEmode)
    else c.typecheck(tree)

  override val freshName = (prefix: String) => TermName(c.freshName(prefix))
  override val freshType = (prefix: String) => TypeName(c.freshName(prefix))

  /** Syntactic sugar for [[Tree]]s. */
  implicit class BlackBoxTreeOps(self: Tree) extends TreeOps(self) {

    /**
     * Annotate this [[Tree]] with a specified [[Type]].
     *
     * @param tpe The [[Type]] to use for this [[Tree]]
     * @return This [[Tree]] with its [[Type]] set
     */
    def withType(tpe: Type): Tree =
      setType(self, tpe)

    /**
     * Annotate this [[Tree]] with a specified [[Type]].
     *
     * @tparam T The [[Type]] to use for this [[Tree]]
     * @return This [[Tree]] with its [[Type]] set
     */
    def withType[T: TypeTag]: Tree =
      withType(typeOf[T])

    /**
     * Annotate this [[Tree]] with a specified [[Symbol]].
     *
     * @param sym The [[Symbol]] to use for this [[Tree]]
     * @return This [[Tree]] with its [[Symbol]] set
     */
    def withSymbol(sym: Symbol): Tree =
      setSymbol(self, sym)

    /**
     * Replace the owner of all [[Symbol]]s in this [[Tree]].
     *
     * NOTE: The [[Tree]] will be implicitly type-checked if it doesn't have a [[Type]].
     *
     * @param prev The owner [[Symbol]] to replace
     * @param next The new owner [[Symbol]] to use
     * @return This [[Tree]] with all [[Symbol]] owner changed from `prev` to `next`
     */
    def withOwner(prev: Symbol, next: Symbol): Tree =
      changeOwner(typeChecked, prev, next)

    /**
     * Transform this [[Tree]] while repairing the [[Symbol]] owner chain. After this there should
     * be no need to type-check or re-type-check the [[Tree]].
     *
     * NOTE: The [[Tree]] will be implicitly type-checked if it doesn't have a [[Type]].
     *
     * @param pf A transforming [[PartialFunction]]
     * @return A tranformed copy of this [[Tree]]
     */
    def typingTransform(pf: (Tree, TypingTransformApi) ~> Tree): Tree =
      c.internal.typingTransform(typeChecked) {
        case x if pf isDefinedAt x => pf(x)
        case (tree, transform) => transform default tree
      }

    /**
     * Transform this [[Tree]] while repairing the [[Symbol]] owner chain. After this there should
     * be no need to type-check or re-type-check the [[Tree]].
     *
     * NOTE: The [[Tree]] will be implicitly type-checked if it doesn't have a [[Type]].
     *
     * @param owner The owner [[Symbol]] tu use when type-checking
     * @param pf A transforming [[PartialFunction]]
     * @return A tranformed copy of this [[Tree]]
     */
    def typingTransform(owner: Symbol)(pf: (Tree, TypingTransformApi) ~> Tree): Tree =
      c.internal.typingTransform(typeChecked, owner) {
        case x if pf isDefinedAt x => pf(x)
        case (tree, transform) => transform default tree
      }
  }
}
