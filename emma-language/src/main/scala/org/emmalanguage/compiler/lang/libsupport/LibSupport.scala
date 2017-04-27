/*
 * Copyright © 2014 TU Berlin (emma@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.emmalanguage
package compiler.lang.libsupport

import api.emma
import compiler.{Common, Compiler}
import util.Monoids
import util.Memo

import cats.instances.all._
import shapeless._

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

/** Basic support for library functions inlining & normalization. */
trait LibSupport extends Common {
  self: Compiler =>

  import api._
  import UniverseImplicits._
  import LibSupport.LibDefRegistry.withAST

  private[compiler] object LibSupport {

    // -------------------------------------------------------------------------
    // Data structures
    // -------------------------------------------------------------------------

    /**
     * The type of the function call graph.
     *
     * The vertex set `vs` represents callers.
     *
     * The directed edges set `es` represents
     * - `caller calls callee` relations or
     * - `param binds to lambda` relations.
     */
    case class CG
    (
      vs: Set[CG.Vertex] = Set.empty[CG.Vertex],
      es: Set[CG.Edge] = Set.empty[CG.Edge]
    )

    object CG {

      sealed trait Vertex

      sealed trait TreeRef extends Vertex {
        val tree: u.Tree
      }

      sealed trait Callee extends Vertex {
        val sym: u.TermSymbol

        override def equals(that: Any): Boolean =
          that match {
            case that: Callee => this.sym == that.sym
            case _ => false
          }

        override def hashCode: Int =
          sym.hashCode()
      }

      case class Snippet(tree: u.Tree) extends Vertex with TreeRef {
        override def toString: String = "snippet"
      }

      case class FunPar(sym: u.TermSymbol) extends Callee {
        override def toString: String = s"${sym.name}"
      }

      case class Lambda(sym: u.TermSymbol, tree: u.Tree) extends Callee with TreeRef {
        override def toString: String = s"${sym.name}"
      }

      case class LibDef(sym: u.MethodSymbol, tree: u.Tree) extends Callee with TreeRef {
        override def toString: String = s"${sym.owner.name}.${sym.name}"
      }

      sealed trait Edge {
        val src: Vertex
        val tgt: Vertex
      }

      object Edge {
        def unapply(e: Edge): Option[(Vertex, Vertex)] = e match {
          case Calls(src, tgt) => Some((src, tgt))
          case Binds(src, tgt) => Some((src, tgt))
          case _ => None
        }
      }

      case class Calls(src: Vertex, tgt: Callee) extends Edge {
        override def toString: String = s"$src -> $tgt"
      }

      case class Binds(src: FunPar, tgt: Callee) extends Edge {
        override def toString: String = s"$src := $tgt"
      }

    }

    // -------------------------------------------------------------------------
    // API
    // -------------------------------------------------------------------------

    lazy val expand: u.Tree => u.Tree = tree => {
      val cg = LibSupport.callGraph(tree)

      def dep(v: CG.Vertex, rest: Set[CG.Vertex]): String = rest
        .find(u => cg.es.exists { case CG.Edge(`v`, `u`) => true })
        .map(u => s"$v -> ${dep(u, rest - u)}")
        .getOrElse(v.toString)

      for {
        comp <- LibSupport.stronglyConnComp(cg)
        if comp.size > 1
      } self.abort(s"Cyclic call dependency: ${dep(comp.head, comp.tail)}")

      normalize(cg)
    }

    private[libsupport] lazy val normalize: CG => u.Tree = cg => {

      def recur(cg: CG): u.Tree = {
        // 1. find unique Snippet vertex s
        val s = (cg.vs collectFirst {
          case v: CG.Snippet => v
        }).get

        // 2. find incident out-bound Calls edges e = (s, v)
        val out = cg.es collect {
          case e@CG.Calls(`s`, _) => e
        }

        // 3. inline and β-reduce all out-bound Calls edges
        val sNew = out.foldLeft(s)((r, e) => inline(e.tgt, r))

        // 4. remove v and all e from the call graph, insert result Snippet vertex sNew
        val cgNew = {
          // approximate new edge set `es`
          val esNew = {
            // `ts` is the set of all `t`'s occurring in a path `s -> v -> t` where `s -> v` is part of `out`
            val ts = out.map(e => e.tgt).toSet[CG.Vertex]
            // from the old `es`
            // remove `out` and
            // add a call edge `sNew -> w` for each call edge `t -> w` where `t` is in `ts`
            cg.es -- out ++ cg.es.collect {
              case CG.Calls(t, w) if ts(t) => CG.Calls(sNew, w)
            }
          }
          // new vertex set `vs` is the set of source vertices occurring in the new `es`
          val vsNew = esNew.map(_.tgt) ++ Set(sNew)
          // new `cg` consists of the new `vs` and the new `es` with dangling edges removed
          cg.copy(
            vs = vsNew,
            es = esNew.filter(e => vsNew(e.src))
          )
        }

        // 5. check modified call graph
        if (cgNew.vs == Set(sNew)) sNew.tree // if singleton, return the final Snippet
        else recur(cgNew) // else recurse
      }

      recur(cg)
    }

    private[libsupport] lazy val inline: (CG.Callee, CG.Snippet) => CG.Snippet = (callee, snippet) => {
      val result = callee match {
        case CG.Lambda(sym, Lambda(_, _, _)) =>
          val result = BottomUp.withDefs.transformWith({
            case Attr.none(call@DefCall(Some(TermRef(`sym`)), _, Seq(), _)) =>
              call
          })(snippet.tree).tree

          result

        case CG.LibDef(sym, fun@DefDef(_, tparams, paramss, body)) =>
          // collect parameter uses for `fun`
          val (bndDefs, parUses) = defDefAttrs(fun)

          // partition function parameters into two sets:
          // parameters used mostly once and parameters used more than once
          val (parsUsedOnce, parsUsedMany) = paramss.flatten.map {
            case ParDef(par, _) => par
          }.toSet.partition(parUses(_) <= 1)

          val result = BottomUp
            .accumulate({
              case api.TermDef(s) => Set[u.Name](s.name)
            })
            .withOwner
            .transformWith({
              case Attr(DefCall(Some(TermRef(_)), `sym`, targs, argss), _, owner :: _, _) =>
                // compute type bindings sequence
                val typesSeq = for {
                  (tp, ta) <- tparams zip targs
                } yield tp -> ta.typeSymbol.asType
                // compute type bindings map
                val typesMap = (for {
                  (tp, ta) <- typesSeq
                } yield tp.toType -> ta.toType).toMap.withDefault(t => t)

                // compute term bindings sequence
                val termsSeq = for {
                  s <- (bndDefs diff parsUsedOnce).toSeq
                } yield s -> Sym.With(s)(
                  nme = TermName.fresh(s),
                  tpe = s.info.map(typesMap),
                  flg = if (parsUsedMany(s)) u.NoFlags else Sym.flags(s)
                ).asTerm
                // compute term bindings sequence
                val termsMap = termsSeq.toMap

                // compute a sequence of `symbol -> tree` substitutions
                val substSeq = for {
                  (ParDef(p, _), a) <- paramss.flatten zip argss.flatten
                } yield {
                  if (parsUsedOnce(p)) p -> a
                  else p -> Ref(termsMap(p))
                }

                // compute prefix of ValDefs derived from ParDefs used more than once
                val prefxSeq = for {
                  (ParDef(p, _), a) <- paramss.flatten zip argss.flatten
                  if parsUsedMany(p)
                } yield ValDef(termsMap(p), a)

                // substitute terms and types and inline arguments in the body
                val substBody = ({
                  Tree.rename(termsSeq ++ typesSeq)
                } andThen {
                  Tree.subst(substSeq)
                }) (body)

                // prepend ValDefs prefix to the substitution result
                val prefxBody =
                if (prefxSeq.isEmpty) substBody
                else substBody match {
                  case Block(stats, expr) => Block(prefxSeq ++ stats, expr)
                  case expr => Block(prefxSeq, expr)
                }

                val anyEq = u.typeOf[Any].member(TermName("==")).asMethod
                val fixEqBody = BottomUp.transform({
                  case DefCall(Some(lhs), `anyEq`, Seq(), Seq(rhs)) =>
                    val specializedEq = lhs.tpe.member(anyEq.name).asTerm
                    DefCall(Some(lhs), specializedEq, Seq.empty, Seq(rhs))
                })(prefxBody).tree

                Owner.at(owner)(fixEqBody)
            })(snippet.tree).tree

          result
      }

      CG.Snippet(result)
    }

    private[libsupport] lazy val callGraph: u.Tree => CG = tree => {
      @tailrec
      def discover(cg: CG, frontier: Set[CG.TreeRef] = Set.empty): CG =
        frontier.headOption match {
          case Some(v) => // vertex frontier is not empty
            // collect lambdas defined and library functions refered in this vertex
            val lambdas = lambdaDefsIn(v.tree)
            val libdefs = libdefRefsIn(v.tree)

            // collect the call edges in this vertex
            val edgBldr = Set.newBuilder[CG.Edge]
            // helper method: adds edges representing function parameter bindings
            val addBnds = (argss: Seq[Seq[u.Tree]], sym: u.MethodSymbol) =>
              for {
                (args, params) <- argss zip sym.paramLists
                (TermRef(arg), TermSym(param)) <- args zip params
                if lambdas.contains(arg)
              } edgBldr += CG.Binds(CG.FunPar(param), lambdas(arg))
            // main traversal that accumulates the edges in `edgBldr`
            TopDown
              .inherit({
                // inherit enclosing caller lambda or library function
                case ValDef(sym, Lambda(_, _, _)) =>
                  lambdas.getOrElse(sym, v)
              })(Monoids.last(v))
              .inherit({
                // inherit function type parameters of enclosing Lambda and DefDef nodes
                case Lambda(_, params, _) => (for {
                  param <- params.map(_.symbol)
                  if Sym.funs(param.info.dealias.widen.typeSymbol)
                } yield param).toList
                case DefDef(_, _, paramss, _) => (for {
                  param <- paramss.flatten.map(_.symbol)
                  if Sym.funs(param.info.dealias.widen.typeSymbol)
                } yield param).toList
              })
              .traverseWith({
                // function parameter call
                case Attr.inh(DefCall(Some(TermRef(tgt)), sym, _, argss), enclFunPars :: caller :: _)
                  if enclFunPars.contains(tgt) =>
                  edgBldr += CG.Calls(caller, CG.FunPar(tgt))
                  addBnds(argss, sym)
                // lambda function call
                case Attr.inh(DefCall(Some(TermRef(tgt)), sym, _, argss), _ :: caller :: _)
                  if lambdas.contains(tgt) =>
                  edgBldr += CG.Calls(caller, lambdas(tgt))
                  addBnds(argss, sym)
                // library function call
                case Attr.inh(DefCall(Some(TermRef(_)), sym, _, argss), _ :: caller :: _)
                  if LibDefRegistry(sym).isDefined =>
                  edgBldr += CG.Calls(caller, CG.LibDef(sym, LibDefRegistry(sym).get))
                  addBnds(argss, sym)
              })(v.tree)
            val edges = edgBldr.result()

            // all parameters occurring in a `binds` edge
            val funpars = edges.collect {
              case CG.Binds(src, _) => src
            }

            // continue recursively
            discover(
              cg.copy(
                vs = cg.vs ++ (lambdas.values ++ libdefs.values ++ funpars).toSet[CG.Vertex] + v,
                es = cg.es ++ edges
              ),
              frontier.tail ++ (lambdas.values ++ libdefs.values).collect {
                case u: CG.TreeRef if u != v && !cg.vs(u) => u // all new TreeRef vertices
              })
          case None => // vertext frontier is empty
            cg
        }

      discover(CG(Set(CG.Snippet(tree))), Set(CG.Snippet(tree)))
    }

    /**
     * Kosaraju's algorithm for strongly connected components.
     *
     * This is not the fastest solution, but is straight-forward to implement and understand.
     * Should be good enough for a start, especially given the expected graph sizes and the
     * comaratively much higher time to parse and loat an AST from a string.
     */
    private[libsupport] lazy val stronglyConnComp: CG => Set[Set[CG.Vertex]] = cg => {
      // out-bound neighbours map
      val out = cg.es
        .groupBy(_.src)
        .mapValues(_.map(e => e.tgt).toSeq)
        .withDefault(_ => Seq.empty[CG.Vertex])

      // in-bound neighbours map
      val in = cg.es
        .groupBy(e => e.tgt)
        .mapValues(_.map(_.src).toSeq)
        .withDefault(_ => Seq.empty[CG.Vertex])

      // visit procedure and state (1st DFS)
      def postorder(): List[CG.Vertex] = {
        var visited = collection.mutable.Set.empty[CG.Vertex]
        val postord = ListBuffer.empty[CG.Vertex]

        def visit(v: CG.Vertex): Unit =
          if (!visited(v)) {
            visited += v
            for (u <- out(v))
              visit(u)
            v +=: postord
          }

        for (v <- cg.vs)
          visit(v)

        postord.result()
      }

      // assign procedure and state (2nd DFS)
      def assign(): Map[CG.Vertex, CG.Vertex] = {
        val component = collection.mutable.Map.empty[CG.Vertex, CG.Vertex]

        def assign(u: CG.Vertex, r: CG.Vertex): Unit =
          if (!component.contains(u)) {
            component(u) = component.getOrElse(r, r)
            for (v <- in(u))
              assign(v, r)
          }

        // the actual algorithm
        for (u <- postorder())
          assign(u, u)

        component.toMap
      }

      val assignment = assign()

      // transform the component map into a nested set of strongly connected components
      assignment.toSeq.groupBy(_._2)
        .mapValues(_.map(_._1).toSet)
        .values.toSet
    }

    // -------------------------------------------------------------------------
    // Helper methods & objects
    // -------------------------------------------------------------------------

    /** Return a pair of `(binding defs, parameter uses)` for a `DefDef` node. */
    private lazy val defDefAttrs: u.DefDef => (Set[u.TermSymbol], Map[u.TermSymbol, Int]) =
      BottomUp.withParUses.withBindDefs.traverseAny andThen {
        case Attr.syn(_, bndDefs :: parUses :: _) => (bndDefs.keySet, parUses)
      }

    /** Build a map of all lambdas defined in the given tree. */
    private lazy val lambdaDefsIn: u.Tree => Map[u.TermSymbol, CG.Lambda] = _.collect({
      case ValDef(sym, lambda@Lambda(_, _, _)) => sym -> CG.Lambda(sym, lambda)
    }).toMap

    /** Build a map of all library functions called in the given tree. */
    private lazy val libdefRefsIn: u.Tree => Map[u.TermSymbol, CG.LibDef] = _.collect({
      case DefCall(Some(TermRef(_)), sym withAST ast, _, _)
        if Sym.findAnn[emma.src](sym).isDefined => sym -> CG.LibDef(sym, ast)
    }).toMap

    /* Lirary function registry. */
    object LibDefRegistry {

      /** If the given MethodSymbol is a library function, loads it into the registry and retuns its AST. */
      def apply(sym: u.MethodSymbol): Option[u.DefDef] = getOrLoad(sym)

      /** Extractor for the AST associated with a library function symbol. */
      object withAST {
        def unapply(sym: u.MethodSymbol): Option[(u.MethodSymbol, u.DefDef)] =
          apply(sym).map(ast => (sym, ast))
      }

      /** Extractor for the `emma.src` annotation associated with a library function symbol. */
      object withAnn {
        def unapply(sym: u.MethodSymbol): Option[(u.MethodSymbol, u.Annotation)] =
          Sym.findAnn[emma.src](sym) match {
            case Some(ann) => Some(sym, ann)
            case None => None
          }
      }

      /** Loads and memoizes an AST for a library function from a `sym` with an `emma.src` annotation. */
      private lazy val getOrLoad = Memo[u.MethodSymbol, Option[u.DefDef]]({
        case sym withAnn ann =>
          ann.tree.collect {
            case Lit(fldName: String) =>
              // build an `objSym.fldSym` selection expression
              val cls = sym.owner.asClass
              val fld = cls.info.member(TermName(fldName)).asTerm
              val sel = qualifyStatics(DefCall(Some(Ref(cls.module)), fld))
              // evaluate `objSym.fldSym` and grab the DefDef source
              val src = eval[String](sel)
              // parse the source and grab the DefDef AST
              parse(Seq(_.collect(extractDef).head, fixDefDefSymbols(sym)))(src)
          }.collectFirst(extractDef)
        case _ =>
          None
      })

      /** Extracts the first DefDef from an input tree. */
      private lazy val extractDef: u.Tree =?> u.DefDef = {
        case dd@DefDef(_, _, _, _) => dd
      }

      /** Parses a DefDef AST from a string. */
      private val parse = (transformations: Seq[u.Tree => u.Tree]) =>
        pipeline(typeCheck = true, withPost = false)(
          transformations: _*
        ).compose(self.parse)

      /** Replaces the top-level DefDef symbol of the quoted tree with the corrsponding original symbol. */
      private val fixDefDefSymbols = (original: u.MethodSymbol) =>
        TopDown.break.transform {
          case DefDef(_, tparams, paramss, body) =>
            DefDef(original, tparams, paramss.map(_.map(_.symbol.asTerm)), body)
        } andThen (_.tree)
    }

  }

}
