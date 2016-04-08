package eu.stratosphere.emma.compiler.ir.lnf

import eu.stratosphere.emma.compiler.ir.CommonIR

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scalax.collection.GraphEdge.UnDiEdge
import scalax.collection.immutable.Graph


trait SchemaOptimizations extends CommonIR {
  self: Language =>

  import universe._
  import Tree._

  // ---------------------------------------------------------------------------
  // Schema Analysis & Optimizations
  // ---------------------------------------------------------------------------

  object Schema {

    import CaseClassMeta._

    /** Root trait for all fields. */
    sealed trait Field

    /** A simple field representing a ValDef name. */
    case class SimpleField(symbol: Symbol) extends Field

    /** A field representing a member selection. */
    case class MemberField(symbol: Symbol, member: Symbol) extends Field

    /** A class of equivalent fields. */
    type FieldClass = Set[Field]

    /** Fields can be either */
    case class Info(fieldClasses: Set[FieldClass])

    /**
     * Compute the (global) schema information for a tree fragment.
     *
     * @param tree The ANF [[Tree]] to be analyzed.
     * @return The schema information for the input tree.
     */
    private[emma] def global(tree: Tree): Schema.Info = {
      Info(Set.empty[FieldClass])
    }

    /**
     * Compute the (local) schema information for an anonymous function.
     *
     * @param tree The ANF [[Tree]] to be analyzed.
     * @return The schema information for the input tree.
     */
    private[emma] def local(tree: Function): Schema.Info = {
      // initialize equivalences relation
      val equivalences: mutable.Buffer[(Field, Field)] = ArrayBuffer()

      // traverse the function and collect equivalences
      traverse(tree) {
        // patterns of type `x = y.target`
        case val_(x, Select(y, member@TermName(_)), _) =>
          equivalences += SimpleField(x) -> MemberField(Term.of(y), Term.member(y, member))
          equivalences ++= caseClassMemberEquivalences(x)

        // patterns of type `x = constructor (arg1, ..., argN)`
        case val_(x, Apply(fun, args), _) if isCaseClassConstructor(fun.symbol) =>
          equivalences += SimpleField(x) -> SimpleField(x)

          // add constructor equivalences and members
          val meta = CaseClassMeta(Type.of(x).typeSymbol)
          equivalences ++= args.zip(meta.constructorProjections(fun.symbol)).map {
            case (arg, param) => SimpleField(arg.symbol) -> MemberField(x, param)
          }
          equivalences ++= meta.memberEquivalences(x)

        // just a regular ValDef
        case val_(x, _, _) =>
          equivalences += SimpleField(x) -> SimpleField(x)
          equivalences ++= caseClassMemberEquivalences(x)

      }

      Info(equivalenceClasses(equivalences))
    }

    private def equivalenceClasses(equivalences: Seq[(Field, Field)]): Set[FieldClass] = {
      val edges = equivalences.map { case (f, t) => UnDiEdge(f, t) }
      val nodes = edges.flatMap(_.toSet)
      val graph = Graph.from[Field, UnDiEdge](nodes, edges)

      // each connected component forms an equivalence class
      graph.componentTraverser().map(_.nodes.map(_.value)).toSet
    }

    class CaseClassMeta(sym: ClassSymbol) {

      // assert that the input argument is a case class symbol
      assert(sym.isCaseClass)

      def constructorProjections(fun: Symbol): Seq[Symbol] = {
        assert(isCaseClassConstructor(fun))
        val parameters = fun.asMethod.paramLists.head
        parameters.map(p => sym.info.decl(p.name))
      }

      def members(): Set[Symbol] = {
        Type.of(sym).members.filter {
          case m: TermSymbol => m.isGetter && m.isCaseAccessor
        }.toSet
      }

      def memberFields(symbol: Symbol): Set[MemberField] = {
        members().map(member => MemberField(symbol, member))
      }

      def memberEquivalences(symbol: Symbol): Set[(Field, Field)] = {
        memberFields(symbol).map(f => f -> f)
      }
    }

    object CaseClassMeta {

      def apply(s: Symbol) = new CaseClassMeta(s.asClass)

      def isCaseClass(s: Symbol): Boolean = s.isClass && s.asClass.isCaseClass

      def isCaseClassConstructor(fun: Symbol): Boolean = {
        assert(fun.isMethod)
        val isCtr = fun.owner.companion.isModule && fun.isConstructor
        val isApp = fun.owner.isModuleClass && fun.name == TermName("apply") && fun.isSynthetic

        isCtr || isApp
      }

      def caseClassMemberEquivalences(s: Symbol): Set[(Field, Field)] = {
        val tpe = Type.of(s).typeSymbol
        if (isCaseClass(tpe)) {
          CaseClassMeta(tpe).memberEquivalences(s)
        } else {
          Set.empty
        }
      }
    }

  }

}
