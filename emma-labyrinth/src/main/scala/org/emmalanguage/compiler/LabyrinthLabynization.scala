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
package compiler

import labyrinth.partitioners._
import labyrinth.operators.InputFormatWithInputSplit
import api.Group

import org.apache.flink.core.fs.FileInputSplit
import shapeless.::

trait LabyrinthLabynization extends LabyrinthCompilerBase {

  import API._
  import UniverseImplicits._

  val labyrinthLabynize: TreeTransform = TreeTransform("labyrinthLabynize", (tree: u.Tree) => {

    val exprSymOpt = tree match {
      case core.Let(_,_,core.Ref(sym)) => Some(sym)
      case _ => None
    }

    // println("!!!!!!!!!!!!!!!!!!!!!! 0tree Labynization !!!!!!!!!!!!!!!!!!!!!!!")
    // println(tree)
    // println("!!!!!!!!!!!!!!!!!!!!!! 0tree End !!!!!!!!!!!!!!!!!!!!!!!")

    // Define a name for the enclosing block ("enclosingOwner"). This is a little hack to easily get all basic block
    // dependencies.
    val outerTermName = api.TermName.fresh("OUTER")
    val outerToEncl = Map(outerTermName.toString -> enclosingOwner.name.toString)
    // Returns the name of a termsymbol. Replaces it by `enclosingOwner` if it is the previously defined outer name.
    def name(ts: u.TermSymbol): String = {
      if (outerToEncl.keys.toList.contains(ts.name.toString)) {
        outerToEncl(ts.name.toString)
      } else {
        ts.name.toString
      }
    }

    // wrap outer block into dummy defdef to get all basic block affiliations (little hack, see above)
    val wraptree = tree match {
      case lt @ core.Let(_,_,_) => {
        val owner = enclosingOwner
        val dd = core.DefDef(
          api.DefSym(owner, outerTermName, res = api.Type.any),
          Seq(),
          Seq(),
          lt
        )
        val expr = api.Term.unit
        core.Let(Seq(), Seq(dd), expr)
      }
      case t: u.Tree => t
    }

    // ---> get control flow info ---> //
    // ControlFlow returns some nice info; extracted the necessary info for later use.
    val G = ControlFlow.cfg(wraptree)
    val bbParents = G.ctrl.edges.map(e => name(e.from)).toSet
    val bbChildren = G.ctrl.edges.map(e => name(e.to)).toSet
    // define bbids
    val bbIdMap = scala.collection.mutable.Map[String, Int]()
    bbIdMap += (enclosingOwner.name.toString -> 0)
    var idCounter = 1
    bbChildren.foreach( e => { bbIdMap += (e -> idCounter); idCounter += 1 })
    // define dependencies (parent to list of children)
    val gDependencies = scala.collection.mutable.Map[String, Seq[String]]()
    G.ctrl.edges.foreach(n =>
      if (!gDependencies.keys.toList.contains(name(n.from))) { gDependencies += (name(n.from) -> Seq(name(n.to))) }
      else { gDependencies(name(n.from)) = gDependencies(name(n.from)) :+ name(n.to) }
    )
    val gBbDependencies = scala.collection.mutable.Map[Int, Seq[Int]]()
    gDependencies.keys.foreach(k => if (bbIdMap.contains(k)) {
      // This if is needed when there is a DefCall in a higher-order context
      gBbDependencies += (bbIdMap(k) -> gDependencies(k).map(bbIdMap(_)))
    })
    // count number of outgoing edges for each basic block
    val bbIdNumOut = scala.collection.mutable.Map[Int, Int]()
    gDependencies.keys.foreach(n => if (bbIdMap.contains(n)) { // Same as above
      bbIdNumOut += (bbIdMap(n) -> gDependencies(n).size)
    })

    def bbIdShortcut(start: Int): Seq[Int] = {
      if (!bbIdNumOut.keys.toList.contains(start) || bbIdNumOut(start) != 1) {
        Seq(start)
      } else {
        Seq(start) ++ gBbDependencies(start).flatMap( id => bbIdShortcut(id) )
      }
    }
    // <--- ofni wolf lortnoc teg <--- // (magical incantation to prevent program crashes)

    // transformation helpers
    val replacements = scala.collection.mutable.Map[u.TermSymbol, u.TermSymbol]()
    // here we have to use names because we need mapping from different blocks during the transformation and we
    // generate new symbols during pattern matching (should be no problem though, as they are unique after lifting)
    val defSymNameToPhiRef = scala.collection.mutable.Map[String, Seq[Seq[u.Ident]]]()

    // All val defs we gather during traversal. Added into a single letblock later.
    var valDefsFinal = Seq[u.ValDef]()

    // Traversal to create LabyNodes and all necessary ValDefs
    api.TopDown
      .withOwner
      .traverseWith {
        case Attr.inh(vd @ core.ValDef(_, core.Lambda(_,_,_)), _) => valDefsFinal = valDefsFinal :+ vd
        case Attr.inh(vd @ core.ValDef(_, rhs), _) if isAlg(rhs) => valDefsFinal = valDefsFinal :+ vd
        case Attr.inh(vd @ core.ValDef(lhs, rhs), owner :: _)
          if !isFun(lhs) && !hasFunInOwnerChain(lhs) && !isAlg(rhs) => {

          rhs match {
            case dc @ core.DefCall(_,  DataBag$.empty, Seq(targ), _) =>

              val bagOp = core.DefCall(Some(ScalaOps$.ref), ScalaOps$.empty, Seq(targ), Seq())
              val bagOpRefDef = valRefAndDef(owner, "emptyOp", bagOp)

              val refDefs = generateLabyNode[Always0[Any]](
                owner,
                bagOpRefDef._1,
                getTpe[labyrinth.util.Nothing],
                targ,
                1,
                Seq(),
                "empty",
                1,
                bbIdMap
              )
              val defs = refDefs._1.map(_._2)
              val finalNode = refDefs._2

              // put everything into a block
              valDefsFinal = valDefsFinal ++ Seq(bagOpRefDef._2) ++ defs :+ finalNode._2
              replacements += (lhs -> finalNode._3)
              skip(dc)
              ()

            case dc @ core.DefCall(_, DB$.fromSingSrcReadText, _, Seq(Seq(core.Ref(dbPathSym)))) =>
              val dbPathSymRepl = replacements(dbPathSym)

              //// get splits
              // bagoperator
              val bagOpSplitsVDrhs = core.DefCall(Some(ScalaOps$.ref), ScalaOps$.textSource, Seq(), Seq())
              val bagOpSplitsRefDef = valRefAndDef(owner, "inputSplits", bagOpSplitsVDrhs)

              // typeinfo OUT for splits
              val tpeOutSplits = api.Type.apply(
                getTpe[InputFormatWithInputSplit[Any, FileInputSplit]].widen.typeConstructor,
                Seq(u.typeOf[String], getTpe[FileInputSplit])
              )

              val refDefsSplits = generateLabyNode[Always0[Any]](
                owner,
                bagOpSplitsRefDef._1,
                dbPathSym.info.typeArgs.head,
                tpeOutSplits,
                1,
                Seq(dbPathSymRepl),
                "inputSplits",
                1,
                bbIdMap
              )
              val defsSplits = refDefsSplits._1.map(_._2)
              val finalNodeSplits = refDefsSplits._2

              //// read splits
              // bagoperator
              val bagOpReadVDrhs = core.DefCall(Some(ScalaOps$.ref), ScalaOps$.textReader, Seq(), Seq())
              val bagOpReadRefDef = valRefAndDef(owner, "readSplits", bagOpReadVDrhs)

              val refDefsRead = generateLabyNode[Always0[Any]](
                owner,
                bagOpReadRefDef._1,
                tpeOutSplits,
                u.typeOf[String],
                1,
                Seq(finalNodeSplits._3),
                "readSplits",
                1,
                bbIdMap
              )
              val defsRead = refDefsRead._1.map(_._2)
              val finalNodeRead = refDefsRead._2

              // put everything into a block
              valDefsFinal =
                valDefsFinal ++ Seq(bagOpSplitsRefDef._2) ++ defsSplits ++ Seq(finalNodeSplits._2) ++
                                Seq(bagOpReadRefDef._2) ++ defsRead ++ Seq(finalNodeRead._2)
              replacements += (lhs -> finalNodeRead._3)
              skip(dc)

            // singSrc to LabyNode
            case dc @ core.DefCall(_, DB$.singSrc, Seq(targ), Seq(Seq(funarg))) =>

              // bagoperator
              val bagOpVDrhs = core.DefCall(Some(ScalaOps$.ref), ScalaOps$.fromNothing, Seq(targ), Seq(Seq(funarg)))
              val bagOpRefDef = valRefAndDef(owner, "fromNothing", bagOpVDrhs)

              val refDefs = generateLabyNode[Always0[Any]](
                owner,
                bagOpRefDef._1,
                getTpe[labyrinth.util.Nothing],
                targ,
                1,
                Seq(),
                "fromNothing",
                1,
                bbIdMap
              )
              val defs = refDefs._1.map(_._2)
              val finalNode = refDefs._2

              // put everything into a block
              valDefsFinal = valDefsFinal ++ Seq(bagOpRefDef._2) ++ defs :+ finalNode._2
              replacements += (lhs -> finalNode._3)
              skip(dc)

            // fromSingSrc to LabyNode
            case dc @ core.DefCall(_, DB$.fromSingSrcApply, Seq(targ), Seq(Seq(core.Ref(singSrcDBsym)))) =>

              val singSrcDBReplSym = replacements(singSrcDBsym)

              val inTpe = singSrcDBsym.info.typeArgs.head

              // bagoperator
              val bagOpVDrhs = core.DefCall(
                Some(ScalaOps$.ref),
                ScalaOps$.fromSingSrcApply,
                Seq(targ, inTpe),
                Seq(Seq())
              )
              val bagOpRefDef = valRefAndDef(owner, "fromSingSrcApplyOp", bagOpVDrhs)

              val refDefs = generateLabyNode[Always0[Any]](
                owner,
                bagOpRefDef._1,
                inTpe,
                targ,
                1,
                Seq(singSrcDBReplSym),
                "fromSingSrcApply",
                1,
                bbIdMap
              )
              val defs = refDefs._1.map(_._2)
              val finalNode = refDefs._2

              // put everything into a block
              valDefsFinal = valDefsFinal ++ Seq(bagOpRefDef._2) ++ defs :+ finalNode._2
              replacements += (lhs -> finalNode._3)
              skip(dc)

            // ref.map to LabyNode
            case dc @ core.DefCall(Some(core.Ref(tgtSym)), DataBag.map, Seq(outTpe), Seq(Seq(lbdaRef))) =>

              val tgtReplSym = replacements(tgtSym)

              val inTpe = tgtSym.info.typeArgs.head

              // bagoperator
              val bagOpVDrhs = core.DefCall(
                Some(ScalaOps$.ref),
                ScalaOps$.map,
                Seq(inTpe, outTpe),
                Seq(Seq(lbdaRef))
              )
              val bagOpRefDef = valRefAndDef(owner, "mapOp", bagOpVDrhs)

              val refDefs = generateLabyNode[Always0[Any]](
                owner,
                bagOpRefDef._1,
                inTpe,
                outTpe,
                1,
                Seq(tgtReplSym),
                "map",
                1,
                bbIdMap
              )
              val defs = refDefs._1.map(_._2)
              val finalNode = refDefs._2

              // put everything into a block
              valDefsFinal = valDefsFinal ++ Seq(bagOpRefDef._2) ++ defs :+ finalNode._2
              replacements += (lhs -> finalNode._3)
              skip(dc)

            // ref.flatMap to LabyNode
            case dc @ core.DefCall(Some(core.Ref(tgtSym)), DataBag.flatMap, Seq(outTpe), Seq(Seq(lbdaRef))) =>

              val tgtReplSym = replacements(tgtSym)

              val inTpe = tgtSym.info.typeArgs.head

              // bagoperator
              val bagOpVDrhs = core.DefCall(
                Some(ScalaOps$.ref),
                ScalaOps$.flatMapDataBagHelper,
                Seq(inTpe, outTpe),
                Seq(Seq(lbdaRef))
              )
              val bagOpRefDef = valRefAndDef(owner, "flatMapOp", bagOpVDrhs)

              val refDefs = generateLabyNode[Always0[Any]](
                owner,
                bagOpRefDef._1,
                inTpe,
                outTpe,
                1,
                Seq(tgtReplSym),
                "flatMap",
                1,
                bbIdMap
              )
              val defs = refDefs._1.map(_._2)
              val finalNode = refDefs._2

              // put everything into a block
              valDefsFinal = valDefsFinal ++ Seq(bagOpRefDef._2) ++ defs :+ finalNode._2
              replacements += (lhs -> finalNode._3)
              skip(dc)

            // ref.withFilter to LabyNode
            case dc @ core.DefCall(Some(core.Ref(tgtSym)), DataBag.withFilter, Seq(), Seq(Seq(lbdaRef))) =>

              val tgtReplSym = replacements(tgtSym)

              val inTpe = tgtSym.info.typeArgs.head

              // bagoperator
              val bagOpVDrhs = core.DefCall(
                Some(ScalaOps$.ref),
                ScalaOps$.withFilter,
                Seq(inTpe),
                Seq(Seq(lbdaRef))
              )
              val bagOpRefDef = valRefAndDef(owner, "filter", bagOpVDrhs)

              val refDefs = generateLabyNode[Always0[Any]](
                owner,
                bagOpRefDef._1,
                inTpe,
                inTpe,
                1,
                Seq(tgtReplSym),
                "filter",
                1,
                bbIdMap
              )
              val defs = refDefs._1.map(_._2)
              val finalNode = refDefs._2

              // put everything into a block
              valDefsFinal = valDefsFinal ++ Seq(bagOpRefDef._2) ++ defs :+ finalNode._2
              replacements += (lhs -> finalNode._3)
              skip(dc)

            case dc @ core.DefCall
              (Some(core.Ref(lhsSym)), DataBag.union, _, Seq(Seq(core.Ref(rhsSym)))) =>

              val tpe = lhsSym.info.typeArgs.head
              val lhsReplSym = replacements(lhsSym)
              val rhsReplSym = replacements(rhsSym)

              // =========== Labynode union ===========
              // bagoperator

              val bagOpUnionVDrhs = core.DefCall(
                Some(ScalaOps$.ref),
                ScalaOps$.union,
                Seq(tpe),
                Seq()
              )
              val bagOpUnionRefDef = valRefAndDef(owner, "unionOp", bagOpUnionVDrhs)

              val refDefs = generateLabyNode[Always0[Any]](
                owner,
                bagOpUnionRefDef._1,
                tpe,
                tpe,
                1,
                Seq(lhsReplSym, rhsReplSym),
                "union",
                1,
                bbIdMap
              )
              val defs = refDefs._1.map(_._2)
              val finalNode = refDefs._2

              // put everything into a block
              valDefsFinal = valDefsFinal ++ Seq(bagOpUnionRefDef._2) ++ defs :+ finalNode._2
              replacements += (lhs -> finalNode._3)
              skip(dc)

            case dc @ core.DefCall
              (_, Ops.cross, Seq(tpeA, tpeB), Seq(Seq(core.Ref(lhsSym), core.Ref(rhsSym)))) =>


              // =========== Labynode map to Left() ===========

              // get replacement lhs
              val lhsReplSym = replacements(lhsSym)
              val lhsReplRef = core.ValRef(lhsReplSym)
              val refsDefsLeft = toEither(owner, bbIdMap, tpeA, tpeB, lhsReplSym, lhsReplRef, leftTRightF = true)

              // =========== Labynode map to Right() ===========

              // get replacement rhs
              val rhsReplSym = replacements(rhsSym)
              val rhsReplRef = core.ValRef(rhsReplSym)
              val refsDefsRight = toEither(owner, bbIdMap, tpeA, tpeB, rhsReplSym, rhsReplRef, leftTRightF = false)


              // =========== Labynode cross ===========
              // bagoperator

              val bagOpCrossVDrhs = core.DefCall(
                Some(ScalaOps$.ref),
                ScalaOps$.cross,
                Seq(tpeA,tpeB),
                Seq()
              )
              val bagOpCrossRefDef = valRefAndDef(owner, "crossOp", bagOpCrossVDrhs)

              val inTpe = api.Type.apply(getTpe[scala.util.Either[Any,Any]], Seq(tpeA, tpeB))
              val outTpe = api.Type.apply(getTpe[(Any,Any)], Seq(tpeA, tpeB))

              val refDefs = generateLabyNode[Always0[Any]](
                owner,
                bagOpCrossRefDef._1,
                inTpe,
                outTpe,
                1,
                Seq(refsDefsLeft._8._3, refsDefsRight._8._3),
                "cross",
                1,
                bbIdMap
              )
              val defs = refDefs._1.map(_._2)
              val finalNode = refDefs._2


              // put everything into a block
              valDefsFinal = valDefsFinal ++
                Seq(refsDefsLeft._1._2, refsDefsLeft._2._2, refsDefsLeft._3._2, refsDefsLeft._4._2,
                  refsDefsLeft._5._2, refsDefsLeft._6._2, refsDefsLeft._7._2,
                  refsDefsLeft._8._2,
                  refsDefsRight._1._2, refsDefsRight._2._2, refsDefsRight._3._2, refsDefsRight._4._2,
                  refsDefsRight._5._2, refsDefsRight._6._2, refsDefsRight._7._2,
                  refsDefsRight._8._2,
                  bagOpCrossRefDef._2) ++ defs :+ finalNode._2
              replacements += (lhs -> finalNode._3)
              skip(dc)

            // join
            case dc @ core.DefCall
              (_, Ops.equiJoin, Seq(tpeA, tpeB, tpeK), Seq(Seq(extrARef, extrBRef),
                Seq(core.Ref(db1Sym), core.Ref(db2Sym)))) =>

              // db1 to Left()
              val db1ReplSym = replacements(db1Sym)
              val db1ReplRef = core.ValRef(db1ReplSym)
              val db1refDefs = toEither(owner, bbIdMap, tpeA, tpeB, db1ReplSym, db1ReplRef, leftTRightF = true)

              // db2 to Right()
              val db2ReplSym = replacements(db2Sym)
              val db2ReplRef = core.ValRef(db2ReplSym)
              val db2refDefs = toEither(owner, bbIdMap, tpeA, tpeB, db2ReplSym, db2ReplRef, leftTRightF = false)


              // =========== Labynode equijoin ===========
              // bagoperator

              val bagOpCrossVDrhs = core.DefCall(
                Some(ScalaOps$.ref),
                ScalaOps$.joinScala,
                Seq(tpeA, tpeB, tpeK),
                Seq(Seq(extrARef, extrBRef))
              )
              val bagOpCrossRefDef = valRefAndDef(owner, "joinOp", bagOpCrossVDrhs)

              val inTpe = api.Type.apply(getTpe[scala.util.Either[Any,Any]], Seq(tpeA, tpeB))
              val outTpe = api.Type.apply(getTpe[(Any,Any)], Seq(tpeA, tpeB))

              val refDefs = generateLabyNode[Always0[Any]](
                owner,
                bagOpCrossRefDef._1,
                inTpe,
                outTpe,
                1,
                Seq(db1refDefs._8._3, db2refDefs._8._3),
                "join",
                1,
                bbIdMap
              )
              val defs = refDefs._1.map(_._2)
              val finalNode = refDefs._2

              // put everything into a block
              valDefsFinal = valDefsFinal ++
                Seq(
                  // Left
                  db1refDefs._1._2, db1refDefs._2._2, db1refDefs._3._2, db1refDefs._4._2,
                  db1refDefs._5._2, db1refDefs._6._2, db1refDefs._7._2, db1refDefs._8._2,
                  // Right
                  db2refDefs._1._2, db2refDefs._2._2, db2refDefs._3._2, db2refDefs._4._2,
                  db2refDefs._5._2, db2refDefs._6._2, db2refDefs._7._2, db2refDefs._8._2,
                  // joinScala
                  bagOpCrossRefDef._2) ++ defs :+ finalNode._2
              replacements += (lhs -> finalNode._3)
              skip(dc)

            // fold1
            case dc @ core.DefCall(_, DB$.fold1, targs @ Seq(tpeA, tpeB), Seq(Seq(core.Ref(dbSym), alg))) =>

              val dbReplSym = replacements(dbSym)

              // bagoperator
              val bagOpVDrhs = core.DefCall(Some(ScalaOps$.ref), ScalaOps$.foldAlgHelper, targs, Seq(Seq(alg)))
              val bagOpRefDef = valRefAndDef(owner, "fold1Op", bagOpVDrhs)

              val refDefs = generateLabyNode[Always0[Any]](
                owner,
                bagOpRefDef._1,
                tpeA,
                tpeB,
                1,
                Seq(dbReplSym),
                "fold1",
                1,
                bbIdMap
              )
              val defs = refDefs._1.map(_._2)
              val finalNode = refDefs._2

              // put everything into a block
              valDefsFinal = valDefsFinal ++ Seq(bagOpRefDef._2) ++ defs :+ finalNode._2
              replacements += (lhs -> finalNode._3)
              skip(dc)

            case dc @ core.DefCall
              (_, DB$.fold2, targs @ Seq(tpeA, tpeB), Seq(Seq(core.Ref(dbSym), zero, init, plus))) =>

              val dbReplSym = replacements(dbSym)

              // bagoperator
              val bagOpVDrhs = core.DefCall(Some(ScalaOps$.ref), ScalaOps$.fold, targs, Seq(Seq(zero, init, plus)))
              val bagOpRefDef = valRefAndDef(owner, "fold2Op", bagOpVDrhs)

              val refDefs = generateLabyNode[Always0[Any]](
                owner,
                bagOpRefDef._1,
                tpeA,
                tpeB,
                1,
                Seq(dbReplSym),
                "fold2",
                1,
                bbIdMap
              )
              val defs = refDefs._1.map(_._2)
              val finalNode = refDefs._2

              // put everything into a block
              valDefsFinal = valDefsFinal ++ Seq(bagOpRefDef._2) ++ defs :+ finalNode._2
              replacements += (lhs -> finalNode._3)
              skip(dc)

            case dc @ core.DefCall
              (_, Ops.foldGroup, Seq(tpeA, tpeB, tpeK),
              Seq(Seq(core.Ref(dbSym), extrRef @ core.Ref(_), alg))) =>

              val dbReplSym = replacements(dbSym)

              // bagoperator
              val bagOpVDrhs = core.DefCall(
                Some(ScalaOps$.ref),
                ScalaOps$.foldGroupAlgHelper,
                Seq(tpeK, tpeA, tpeB), Seq(Seq(extrRef, alg))
              )
              val bagOpRefDef = valRefAndDef(owner, "foldGroupOp", bagOpVDrhs)

              val tpeOut = api.Type.apply(getTpe[Group[tpeK.type, tpeB.type]], Seq(tpeK, tpeB))

              val refDefs = generateLabyNode[Always0[Any]](
                owner,
                bagOpRefDef._1,
                tpeA,
                tpeOut,
                1,
                Seq(dbReplSym),
                "foldGroup",
                1,
                bbIdMap
              )
              val defs = refDefs._1.map(_._2)
              val finalNode = refDefs._2

              // put everything into a block
              valDefsFinal = valDefsFinal ++ Seq(bagOpRefDef._2) ++ defs :+ finalNode._2
              replacements += (lhs -> finalNode._3)
              skip(dc)

            case dc @ core.DefCall(_, DB$.fromDatabagWriteCSV, Seq(dbTpe),
            Seq(Seq(core.Ref(dbSym), core.Ref(pathSym), core.Ref(csvSym)))) =>

              val dbReplSym = replacements(dbSym)
              val pathReplSym = replacements(pathSym)
              val csvReplSym = replacements(csvSym)
              val dbReplRef = core.Ref(dbReplSym)
              val csvReplRef = core.Ref(csvReplSym)

              val csvTpe = csvSym.info.typeArgs.head
              val pathTpe = pathSym.info.typeArgs.head

              // ========== db to Left ========= //
              // bagoperator
              val dbRefDefs = toEither(owner, bbIdMap, dbTpe, csvTpe, dbReplSym, dbReplRef, leftTRightF = true)

              // ========== csv to Right ========== //
              // bagoperator
              val csvRefDefs = toEither(owner, bbIdMap, dbTpe, csvTpe, csvReplSym, csvReplRef, leftTRightF = false)

              // ========== toCsvString ========== //
              val bagOpToCsvStringVDrhs = core.DefCall(
                Some(ScalaOps$.ref),
                ScalaOps$.toCsvString,
                Seq(dbTpe),
                Seq()
              )
              val bagOpToCsvStringRefDef = valRefAndDef(owner, "toCsvStringOp", bagOpToCsvStringVDrhs)

              val inTpeCsvString = api.Type.apply(getTpe[scala.util.Either[Any,Any]], Seq(dbTpe, csvTpe))

              val refDefsCsvString = generateLabyNode[Always0[Any]](
                owner,
                bagOpToCsvStringRefDef._1,
                inTpeCsvString,
                pathTpe,
                1,
                Seq(dbRefDefs._8._3, csvRefDefs._8._3),
                "toCsvString",
                1,
                bbIdMap
              )
              val defsCsvString = refDefsCsvString._1.map(_._2)
              val finalNodeCsvString = refDefsCsvString._2

              // ========== stringSink ========== //
              // bagoperator

              val bagOpWriteStringVDrhs = core.DefCall(
                Some(ScalaOps$.ref),
                ScalaOps$.writeString,
                Seq(),
                Seq()
              )
              val bagOpWriteStringRefDef = valRefAndDef(owner, "writeString", bagOpWriteStringVDrhs)

              val outTpeWriteString = u.typeOf[Unit]

              val refDefsWriteString = generateLabyNode[Always0[Any]](
                owner,
                bagOpWriteStringRefDef._1,
                pathTpe,
                outTpeWriteString,
                1,
                Seq(pathReplSym, finalNodeCsvString._3),
                "stringFileSink",
                1,
                bbIdMap
              )
              val defsWriteString = refDefsWriteString._1.map(_._2)
              val finalNodeWriteString = refDefsWriteString._2

              // put everything into a block
              valDefsFinal = valDefsFinal ++
                Seq(
                  // data to Left()
                  dbRefDefs._1._2, dbRefDefs._2._2, dbRefDefs._3._2, dbRefDefs._4._2,
                  dbRefDefs._5._2, dbRefDefs._6._2, dbRefDefs._7._2,
                  dbRefDefs._8._2,
                  // csv to Right()
                  csvRefDefs._1._2, csvRefDefs._2._2, csvRefDefs._3._2, csvRefDefs._4._2,
                  csvRefDefs._5._2, csvRefDefs._6._2, csvRefDefs._7._2,
                  csvRefDefs._8._2
                ) ++ Seq(bagOpToCsvStringRefDef._2) ++ defsCsvString ++ Seq(finalNodeCsvString._2) ++
                     Seq(bagOpWriteStringRefDef._2) ++ defsWriteString ++ Seq(finalNodeWriteString._2)
              replacements += (lhs -> finalNodeWriteString._3)
              skip(dc)
          }
        }

        case Attr.inh(dd @ core.DefDef(ddSym, _, pars, core.Let(_,_,_)), owner::_)
            if !hasFunInOwnerChain(ddSym) =>

          val bbid = bbIdMap(dd.symbol.name.toString)

          // create a phinode for every parameter
          val phiAllRefDefs = pars.map(
            s => s.map{
              case pd @ core.ParDef(sym, _) =>

                val tpeOut = sym.info.typeArgs.head

                // phinode name
                val nmLit = core.Lit(sym.name + "Phi")

                // phinode bbid
                val bbidLit = core.Lit(bbid)

                // partitioner
                val targetPara = 1
                val part = core.Inst(
                  getTpe[Always0[Any]],
                  Seq(tpeOut),
                  Seq(Seq(core.Lit(targetPara)))
                )
                val partPhiRefDef = valRefAndDef(owner, "partitioner", part)

                // phinode inSer
                val inserLit = core.Lit(null)

                // typeinfo OUT
                val typeInfoRefDef = getTypeInfoForTypeRefDef(
                  owner,
                  tpeOut
                )
                // ElementOrEventTypeInfo
                val elementOrEventTypeInfoRefDef = getElementOrEventTypeInfoRefDef(
                  owner,
                  tpeOut,
                  typeInfoRefDef._1
                )

                val phiDC = core.DefCall(
                  Some(LabyStatics$.ref),
                  LabyStatics$.phi,
                  Seq(tpeOut),
                  Seq(Seq(nmLit, bbidLit, partPhiRefDef._1, inserLit, elementOrEventTypeInfoRefDef._1))
                )
                val phiSym = newValSym(dd.symbol, "phiNode", phiDC)
                val phiDCRefDef = valRefAndDef(phiSym, phiDC)

                // save mapping from ParRef to PhiNode for later addInput
                replacements += (sym -> phiSym)

                // prepend valdefs to body letblock and return letblock
                Seq(partPhiRefDef, typeInfoRefDef, elementOrEventTypeInfoRefDef, phiDCRefDef)
            })

          val argPhis = phiAllRefDefs.map( s => s.map( ss => ss.last._1 ) )
          defSymNameToPhiRef += (dd.symbol.name.toString -> argPhis)

          // prepend new defdefs to old defdefs
          var nDefs = Seq[u.ValDef]()
          phiAllRefDefs.foreach(s => nDefs = nDefs ++ s.flatMap(ss => ss.map(refDef => refDef._2)))
          valDefsFinal = valDefsFinal ++ nDefs

        // create condNode when encountering if statements
        case Attr.inh(cnd @ core.Branch(cond @ core.Ref(condSym),
        thn @ core.DefCall(_, thnSym, _, _),
        els @ core.DefCall(_, elsSym, _, _)
        ), owner::_) if !hasFunInOwnerChain(owner) =>

          val condReplSym = replacements(condSym)

          val thnIds = bbIdShortcut(bbIdMap(thnSym.name.toString))
          val elsIds = bbIdShortcut(bbIdMap(elsSym.name.toString))
          val thnIdsDC = core.DefCall(Some(Seq$.ref), Seq$.apply, Seq(u.typeOf[Int]), Seq(thnIds.map(core.Lit(_))))
          val elsIdsDC = core.DefCall(Some(Seq$.ref), Seq$.apply, Seq(u.typeOf[Int]), Seq(elsIds.map(core.Lit(_))))
          val thnIdsDCRefDef = valRefAndDef(owner, "seq", thnIdsDC)
          val elsIdsDCRefDef = valRefAndDef(owner, "seq", elsIdsDC)
          val condOp = core.DefCall(Some(ScalaOps$.ref), ScalaOps$.condNode, Seq(),
            Seq(Seq(
              thnIdsDCRefDef._1,
              elsIdsDCRefDef._1
            )))
          val condOpRefDef = valRefAndDef(owner, "condOp", condOp)

          val inTpe = u.typeOf[scala.Boolean]
          val outTpe = getTpe[labyrinth.util.Unit]

          val refDefs = generateLabyNode[Always0[Any]](
            owner,
            condOpRefDef._1,
            inTpe,
            outTpe,
            1,
            Seq(condReplSym),
            "condNode",
            1,
            bbIdMap
          )
          val defs = refDefs._1.map(_._2)
          val finalNode = refDefs._2

          // put everything into block
          valDefsFinal =
            valDefsFinal ++ Seq(thnIdsDCRefDef._2, elsIdsDCRefDef._2, condOpRefDef._2) ++ defs :+ finalNode._2

        // add inputs to phi nodes when encountering defcalls
        case Attr.inh(dc @ core.DefCall(_, ddSym, _, args), owner :: _)
          if !meta(dc).all.all.contains(SkipTraversal) && !hasFunInOwnerChain(ddSym) &&
            G.ctrl.nodes.contains(ddSym) && // if this is false, then it's a call to some external Def instead of ctrlfl
            !(args.size == 1 && args.head.isEmpty) /*skip if no arguments*/ && args.nonEmpty && !isAlg(dc) =>

          val insideBlock = false

          // phiNode refs according to the arg positions of the def are stored in defSymNameToPhiRef. Iterate through
          // phiNode refs and add the according input
          val defArgPhis = defSymNameToPhiRef(dc.symbol.name.toString)
          var currIdx = 0
          var argIdx = 0
          args.foreach(
            s => { s.foreach {
              case core.ParRef(sym) => {
                val phiRef = defArgPhis(currIdx)(argIdx)
                val argReplSym = replacements(sym)
                val addInpRefDef = getAddInputRefDef(owner, phiRef, core.ParRef(argReplSym), insideBlock)
                valDefsFinal = valDefsFinal :+ addInpRefDef._2
                argIdx += 1
              }
              case core.Ref(sym) => {
                val phiRef = defArgPhis(currIdx)(argIdx)
                val argReplSym = replacements(sym)
                val addInpRefDef = getAddInputRefDef(owner, phiRef, core.Ref(argReplSym), insideBlock)
                valDefsFinal = valDefsFinal :+ addInpRefDef._2
                argIdx += 1
              }
            }
              currIdx += 1
            }
          )

      }._tree(tree)

    // create a flat LetBlock with all ValDefs without any nesting or control flow
    val flatTrans = core.Let(valDefsFinal)

    // add Labyrinth statics to beginning and end of code
    // kickoffsources, register custom serializer, terminal basic block, call translateAll, execute
    val labyStaticsTrans = flatTrans match {
      case core.Let(valdefs, _, _) => {
        val owner = enclosingOwner

        // terminal basic block id: find block which has no outgoing edges
        val terminalSet = bbChildren.filter(!bbParents.contains(_))
        val terminalBbid = if (bbParents.isEmpty) bbIdMap(enclosingOwner.name.toString) else bbIdMap(terminalSet.head)

        // before code
        val custSerDC = core.DefCall(Some(LabyStatics$.ref), LabyStatics$.registerCustomSerializer, Seq(), Seq(Seq()))
        val customSerDCRefDef = valRefAndDef(owner, "registerCustomSerializer", custSerDC)

        val termIdDC = core.DefCall(Some(LabyStatics$.ref), LabyStatics$.setTerminalBbid, Seq(),
          Seq(Seq(core.Lit(terminalBbid))))
        val termIdDCRefDef = valRefAndDef(owner, "terminalBbId", termIdDC)

        // kickoff blocks: start with enclosingOwner and add child of each block that has only one child
        val startBbIds = bbIdShortcut(bbIdMap(enclosingOwner.name.toString))
        val startingBasicBlocks = startBbIds.map(core.Lit(_))
        val kickOffWorldCup2018SourceDC = core.DefCall(Some(LabyStatics$.ref), LabyStatics$.setKickoffSource, Seq(),
          Seq(startingBasicBlocks))
        val kickOffWorldCup2018SourceDCRefDef = valRefAndDef(owner, "kickOffSource", kickOffWorldCup2018SourceDC)

        // after code
        val transAllDC = core.DefCall(Some(LabyStatics$.ref), LabyStatics$.translateAll, Seq(), Seq())
        val transAllDCRefDef = valRefAndDef(owner, "translateAll", transAllDC)

        val envImplDC = core.DefCall(
          Some(core.Ref(api.Sym.predef)),
          api.Sym.implicitly,
          Seq(StreamExecutionEnvironment$.tpe),
          Seq()
        )
        val envImplDCRefDef = valRefAndDef(owner, "implEnv", envImplDC)

        val collValDef = scala.collection.mutable.ArrayBuffer[u.ValDef]() // collectToClient call

        // If there is no exprSym, then we just need to call execute;
        // otherwise we need to call collectToClient (to add a LabyNode) and call executeAndGetCollected.
        val execDCRefDef = exprSymOpt match {

          case None =>
            val execDC =
              core.DefCall(Some(LabyStatics$.ref), LabyStatics$.executeWithCatch, Seq(), Seq(Seq(envImplDCRefDef._1)))
            valRefAndDef(owner, "env.executeWithCatch", execDC)

          case Some(exprSym) =>
            val (socCollRef, socCollDef) =
              valRefAndDef(owner, "socColl", core.DefCall(Some(ScalaOps$.ref), ScalaOps$.collectToClient,
                Seq(exprSym.info.typeArgs.head), // exprSym: DataBag[T]
                Seq(Seq(envImplDCRefDef._1, core.Ref(replacements(exprSym)), core.Lit(terminalBbid)))))

            collValDef.append(socCollDef)

            val ortIsBag = meta(tree).all.all.find(_.isInstanceOf[OrigReturnType]) match {
              case Some(OrigReturnType(isBag)) => isBag
              case _ =>
                throw new AssertionError("OrigReturnType attachment not found. (LabyrinthNormalization adds it)")
            }

            val execDC = core.DefCall(Some(LabyStatics$.ref),
              if (ortIsBag) {
                LabyStatics$.executeAndGetCollectedBag
              } else {
                LabyStatics$.executeAndGetCollectedNonBag
              },
              Seq(),
              Seq(Seq(
                envImplDCRefDef._1,
                socCollRef
              )))
            valRefAndDef(owner, "env.executeAndGetCollectedBag", execDC)
        }

        val newVals = Seq(customSerDCRefDef._2, termIdDCRefDef._2, kickOffWorldCup2018SourceDCRefDef._2) ++
          valdefs ++
          Seq(envImplDCRefDef._2) ++
          collValDef ++
          Seq(transAllDCRefDef._2) ++
          Seq(execDCRefDef._2)

        core.Let(newVals, Seq(), execDCRefDef._1)
      }
    }

    // println("+++++++++++++++++++++++++++++++++++++++++++++++")
    // postPrint(labyStaticsTrans)
    Core.unnest(labyStaticsTrans)
  })

  // =======================
  // some helper functions to avoid (even more) repetition of code
  // =======================

  def getTypeInfoForTypeRefDef(owner: u.Symbol, tpe: u.Type): (u.Ident, u.ValDef) = {
    val typeInfoOUTVDrhs = core.DefCall(
      Option(Memo$.ref),
      Memo$.typeInfoForType,
      Seq(tpe),
      Seq()
    )
    valRefAndDef(owner, "typeInfo", typeInfoOUTVDrhs)}

  def getElementOrEventTypeInfoRefDef(owner: u.Symbol, tpe: u.Type, typeInfo: u.Ident): (u.Ident, u.ValDef) = {
    val eleveVDrhs = core.Inst(
      ElementOrEventAPI.tpe,
      Seq(tpe),
      Seq(Seq(typeInfo))
    )
    valRefAndDef(owner, "elementOrEventTypeInfo", eleveVDrhs)
  }

  def getLabyNodeRefDef(
    owner: u.Symbol,
    tpeInOut: (u.Type, u.Type),
    name: String,
    bagOpRef: u.Ident,
    bbId: Int,
    partRef: u.Ident,
    elementOrEventTypeInfoRef: u.Ident
  ): (u.Ident, u.ValDef) = {
    val labyNodeVDrhs = core.Inst(
      LabyNodeAPI.tpe,
      Seq(tpeInOut._1, tpeInOut._2),
      Seq(Seq(
        core.Lit(name),           // name
        bagOpRef,                 // bagoperator
        core.Lit(bbId),           // bbId
        partRef,                  // partitioner
        core.Lit(null),           // inputSerializer
        elementOrEventTypeInfoRef // elemTypeInfo
      ))
    )
    valRefAndDef(owner, "labyNode", labyNodeVDrhs)
  }

  def getAddInputRefDef(
    owner: u.Symbol,
    tgtRef: u.Ident,
    singSrcDBReplRef: u.Ident,
    insideBlock: Boolean = true
  ) : (u.Ident, u.ValDef) = {
    val condOut = !insideBlock
    val addInputVDrhs = core.DefCall(
      Some(tgtRef),
      LabyNodeAPI.addInput,
      Seq(),
      Seq(Seq(singSrcDBReplRef, core.Lit(insideBlock), core.Lit(condOut)))
    )
    valRefAndDef(owner, "addInput", addInputVDrhs)
  }

  def getSetParallelismRefDefSym(owner: u.Symbol, tgtRef: u.Ident, parallelism: Int):
  (u.Ident, u.ValDef, u.TermSymbol) = {
    val setParVDrhs = core.DefCall(
      Some(tgtRef),
      LabyNodeAPI.setParallelism,
      Seq(),
      Seq(Seq(core.Lit(parallelism)))
    )
    val prlSym = newValSym(owner, "setPrllzm", setParVDrhs)
    val refDef = valRefAndDef(prlSym, setParVDrhs)
    (refDef._1, refDef._2, prlSym)
  }

  def generateLabyNode[partitionerType: u.WeakTypeTag](
    owner: u.Symbol,
    bagOpRef: u.Ident,
    tpeIn: u.Type,
    tpeOut: u.Type,
    partTargetPara: Int,
    inputs: Seq[u.TermSymbol],
    labyNodeName: String,
    labyNodeParallelism: Int,
    bbIdMap: scala.collection.mutable.Map[String, Int]
  ) : (Seq[(u.Ident, u.ValDef)], (u.Ident, u.ValDef, u.TermSymbol)) = {

    // partitioner
    val partVDrhs = core.Inst(
      getTpe[partitionerType],
      Seq(tpeIn),
      Seq(Seq(core.Lit(partTargetPara)))
    )
    val partRefDef = valRefAndDef(owner, "partitioner", partVDrhs)

    // typeinfo OUT
    val typeInfoOUTRefDef = getTypeInfoForTypeRefDef(owner, tpeOut)

    // ElementOrEventTypeInfo
    val elementOrEventTypeInfoRefDef = getElementOrEventTypeInfoRefDef(owner, tpeOut, typeInfoOUTRefDef._1)

    // LabyNode
    val labyNodeRefDef = getLabyNodeRefDef(
      owner,
      (tpeIn, tpeOut),
      labyNodeName,
      bagOpRef,
      bbIdMap(owner.name.toString),
      partRefDef._1,
      elementOrEventTypeInfoRefDef._1
    )

    var refDefs =
      Seq(partRefDef, typeInfoOUTRefDef, elementOrEventTypeInfoRefDef, labyNodeRefDef)

    var lastRef = labyNodeRefDef._1
    inputs.foreach(
      symRef => {
        val insideBlock = symRef.owner == owner
        val refDef = getAddInputRefDef(owner, lastRef, core.Ref(symRef), insideBlock)
        refDefs = refDefs :+ refDef
        lastRef = refDef._1
      }
    )

    // setParallelism (we also need the Symbol here for later use)
    val SetParallelismRefDefSym = getSetParallelismRefDefSym(owner, lastRef, labyNodeParallelism)

    // return all refDef(Sym)
    (refDefs, SetParallelismRefDefSym)
  }

  def toEither(owner: u.Symbol, bbIdMap: scala.collection.mutable.Map[String, Int], tpeA: u.Type, tpeB: u.Type,
    inpSym: u.TermSymbol, inpRef: u.Ident, leftTRightF: Boolean = true) = {
    // bagoperator
    val parSym = api.ParSym(owner, api.TermName.fresh("t"), if (leftTRightF) tpeA else tpeB)
    val parRef = core.ParRef(parSym)
    val lbdaCall = core.DefCall(
      Some(if (leftTRightF) Left$.ref else Right$.ref),
      if (leftTRightF) Left$.apply else Right$.apply,
      if (leftTRightF) Seq(tpeA, getTpe[scala.Nothing]) else Seq(getTpe[scala.Nothing], tpeB),
      Seq(Seq(parRef))
    )
    val lbdaCAllRefDef = valRefAndDef(owner, "lambda", lbdaCall)
    val lbdaBody = core.Let(Seq(lbdaCAllRefDef._2), Seq(), lbdaCAllRefDef._1)
    val mapLambda = core.Lambda(Seq(parSym), lbdaBody)
    val mapLambdaRefDef = valRefAndDef(owner, if (leftTRightF)"toLeft" else "toRight", mapLambda)

    val bagOpVDrhs = core.DefCall(
      Some(ScalaOps$.ref),
      ScalaOps$.map,
      Seq(if (leftTRightF) tpeA else tpeB, api.Type.apply(getTpe[scala.util.Either[Any,Any]], Seq(tpeA, tpeB))),
      Seq(Seq(mapLambdaRefDef._1))
    )
    val bagOpMapRefDef = valRefAndDef(owner, if (leftTRightF) "mapToLeftOp" else "mapToRightOp", bagOpVDrhs)

    // partitioner
    val targetPara = 1
    val partVDrhs = core.Inst(
      getTpe[Always0[Any]],
      Seq(if (leftTRightF) tpeA else tpeB),
      Seq(Seq(core.Lit(targetPara)))
    )
    val partMapRefDef = valRefAndDef(owner, "partitioner", partVDrhs)

    // typeinfo OUT
    val typeInfoMapOUTRefDef =
      getTypeInfoForTypeRefDef(owner, api.Type.apply(getTpe[scala.util.Either[Any,Any]], Seq(tpeA, tpeB)))

    // ElementOrEventTypeInfo
    val elementOrEventTypeInfoMapLhsRefDef =
      getElementOrEventTypeInfoRefDef(
        owner,
        api.Type.apply(getTpe[scala.util.Either[Any,Any]], Seq(tpeA, tpeB)),
        typeInfoMapOUTRefDef._1)

    // LabyNode
    val labyNodeMapLhsRefDef = getLabyNodeRefDef(
      owner,
      (if (leftTRightF) tpeA else tpeB, api.Type.apply(getTpe[scala.util.Either[Any,Any]], Seq(tpeA, tpeB))),
      "map",
      bagOpMapRefDef._1,
      bbIdMap(owner.name.toString),
      partMapRefDef._1,
      elementOrEventTypeInfoMapLhsRefDef._1
    )

    // def insideblock
    val insideBlock = inpSym.owner == owner

    // addInput
    val addInputMapRefDef = getAddInputRefDef(owner, labyNodeMapLhsRefDef._1, inpRef, insideBlock)

    // setParallelism
    val setParallelismMapLhsRefDefSym = getSetParallelismRefDefSym(owner, addInputMapRefDef._1, 1)

    (mapLambdaRefDef, bagOpMapRefDef, partMapRefDef, typeInfoMapOUTRefDef, elementOrEventTypeInfoMapLhsRefDef,
      labyNodeMapLhsRefDef, addInputMapRefDef, setParallelismMapLhsRefDefSym)
  }
}