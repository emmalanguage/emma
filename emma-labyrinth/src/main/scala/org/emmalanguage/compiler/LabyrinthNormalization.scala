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

import shapeless.::

trait LabyrinthNormalization extends LabyrinthCompilerBase {

  import API._
  import UniverseImplicits._

  // non-bag variables to DataBag
  val labyrinthNormalize = TreeTransform("labyrinthNormalize", (tree: u.Tree) => {
    val replacements = scala.collection.mutable.Map[u.TermSymbol, u.TermSymbol]()
    val defs = scala.collection.mutable.Map[u.TermSymbol, u.ValDef]()

    // println("==0tree Normalization==")
    // println(tree)
    // println("==0tree END==")

    // first traversal does the labyrinth normalization. second for block type correction.
    val firstRun = api.TopDown.unsafe
      .withOwner
      .transformWith {

        // find a valdef - according to the rhs we have to do different transformations
        case Attr.inh(vd @ core.ValDef(lhs, rhs), owner :: _)
          if !meta(vd).all.all.contains(SkipTraversal)
            && refsSeen(rhs, replacements) && !isFun(lhs) && !hasFunInOwnerChain(lhs) && !isAlg(rhs) =>

          // helper function to make sure that arguments in a "fromSingSrc"-method are indeed singSources
          def defcallargBecameSingSrc(s: u.TermSymbol) : Boolean = {
            val argSymRepl = replacements(s)
            val argReplDef = defs(argSymRepl)
            isDatabag(argReplDef.rhs) && meta(argReplDef).all.all.contains(SkipTraversal)
          }

          // if the rhs of a valdef is simple a valref, replace that valref with the valref of a newly created valdef,
          // iff original valdef has been replaced by a databag
          rhs match {
            case core.ValRef(sym) if replacements.keys.toList.contains (sym) =>
              val nvr = core.ValRef(replacements(sym))
              val ns = newValSym(owner, lhs.name.toString, nvr)
              val nvd = core.ValDef(ns, nvr)
              skip(nvd)
              replacements += (lhs -> ns)
              nvd

            /*
            In the following we have to catch all the cases where the user manually creates Databags.

            first: check if the rhs is a singleton bag of Seq[A] due to anf and labyrinth transformation
            {{{
                 val a = Seq.apply[Int](2)
                 val b = DataBag.apply[Int](a)

                 val a' = singSrc[Seq[Int]](Seq(2)) ==> Databag[Seq[Int]]
                 val b' = fromSingSrc[Int](a')      ==> Databag[Int]
            }}}
             */
            case core.DefCall(_, DataBag$.apply, _, Seq(Seq(core.ValRef(argsym)))) =>
              val argSymRepl = replacements(argsym)
              val argReplRef = core.ValRef(argSymRepl)

              if (defcallargBecameSingSrc(argsym)) {
                val dbRhs = core.DefCall(
                  Some(DB$.ref),
                  DB$.fromSingSrcApply,
                  Seq(
                    argReplRef.tpe.widen // something like DataBag[Seq[A]]
                    .typeArgs.head // e.g., Seq[A], but beware that it can be a descendant, such as Range.Inclusive
                    .baseType(api.Type.seq.typeSymbol) // get Seq[Int] from Range.Inclusive
                    .typeArgs.head),
                  Seq(Seq(argReplRef))
                )
                val dbSym = newValSym(owner, "db", dbRhs)
                val db = core.ValDef(dbSym, dbRhs)
                skip(db)

                replacements += (lhs -> dbSym)
                defs += (dbSym -> db)
                db
              } else {
                vd
              }

            // check if the user creates a databag using the .readText, .readCSV, or .readParqet methods
            // we basically have to create custom functions for all databag-specific calls.
            case core.DefCall(_, DataBag$.readText, _, Seq(Seq(core.ValRef(argsym)))) =>
              val argSymRepl = replacements(argsym)
              val argReplRef = core.ValRef(argSymRepl)

              if (defcallargBecameSingSrc(argsym)) {
                val dbRhs = core.DefCall(
                  Some(DB$.ref),
                  DB$.fromSingSrcReadText,
                  Seq(),
                  Seq(Seq(argReplRef))
                )
                val dbSym = newValSym(owner, "db", dbRhs)
                val db = core.ValDef(dbSym, dbRhs)
                skip(db)

                replacements += (lhs -> dbSym)
                defs += (dbSym -> db)
                db
              } else {
                vd
              }

            case core.DefCall(_, DataBag$.readCSV, targs, Seq(Seq(core.ValRef(pathSym), core.ValRef(formatSym)))) => {
              val pathSymRepl = replacements(pathSym)
              val pathSymReplRef = core.ValRef(pathSymRepl)
              val formatSymRepl = replacements(formatSym)
              val formatSymReplRef = core.ValRef(formatSymRepl)

              if (defcallargBecameSingSrc(pathSym) && defcallargBecameSingSrc(formatSym)) {
                val dbRhs = core.DefCall(
                  Some(DB$.ref),
                  DB$.fromSingSrcReadCSV,
                  targs,
                  Seq(Seq(pathSymReplRef, formatSymReplRef))
                )
                val dbSym = newValSym(owner, "db", dbRhs)
                val db = core.ValDef(dbSym, dbRhs)
                skip(db)

                replacements += (lhs -> dbSym)
                defs += (dbSym -> db)
                db
              } else {
                vd
              }
            }
            // TODO
            case core.DefCall(_, DataBag$.readParquet, _, Seq(Seq(core.ValRef(argsym)))) => {
              assert(false, "readParquet NOT YET IMPLEMENTED")
              vd
            }

            case core.DefCall(Some(tgt), DataBag.writeCSV, _,
            Seq(Seq(core.ValRef(pathSym), core.ValRef(csvSym)))
            ) => {
              val pathSymRepl = replacements(pathSym)
              val csvSymRepl = replacements(csvSym)
              val pathReplRef = core.ValRef(pathSymRepl)
              val csvReplRef = core.ValRef(csvSymRepl)

              if (defcallargBecameSingSrc(pathSym) && defcallargBecameSingSrc(csvSym)) {
                val dbRhs = core.DefCall(
                  Some(DB$.ref),
                  DB$.fromDatabagWriteCSV,
                  Seq(tgt.tpe.widen.typeArgs.head),
                  Seq(Seq(tgt, pathReplRef, csvReplRef))
                )
                val dbSym = newValSym(owner, "db", dbRhs)
                val db = core.ValDef(dbSym, dbRhs)
                skip(db)

                replacements += (lhs -> dbSym)
                defs += (dbSym -> db)
                db}
              else {
                vd
              }
            }

            // collect
            case dc @ core.DefCall(Some(core.ValRef(tgtSym)), DataBag.collect, _, _) =>
              val tgtSymRepl =
                if (replacements.keys.toList.map(_.name).contains(tgtSym.name)) replacements(tgtSym)
                else tgtSym
              val tgtRepl = core.ValRef(tgtSymRepl)

              val bagTpe = tgtSym.info.typeArgs.head
              val targsRepl = Seq(bagTpe)

              val ndc =core.DefCall(Some(DB$.ref), DB$.collect, targsRepl, Seq(Seq(tgtRepl)))
              val ndcRefDef = valRefAndDef(owner, "collect", ndc)

              val blockFinal = core.Let(Seq(ndcRefDef._2), Seq(), ndcRefDef._1)
              val blockFinalSym = newValSym(owner, "db", blockFinal)
              val blockFinalRefDef = valRefAndDef(blockFinalSym, blockFinal)
              skip(blockFinalRefDef._2)

              replacements += (lhs -> blockFinalSym)
              defs += (blockFinalSym -> blockFinalRefDef._2)

              blockFinalRefDef._2

            // fold1
            // catch all cases of DataBag[A] => B, transform to DataBag[A] => DataBag[B]
            // val db = ...
            // val alg: Alg[A,B] = ...
            // val nondb = db.fold[B](alg)
            //=========================
            // val db = ...
            // val alg = ...
            // val db = DB.foldToBagAlg[A,B](db, alg)
            case dc @ core.DefCall(Some(core.ValRef(tgtSym)), DataBag.fold1, targs, Seq(Seq(alg))) =>
              val tgtSymRepl =
                if (replacements.keys.toList.map(_.name).contains(tgtSym.name)) replacements(tgtSym)
                else tgtSym
              val tgtRepl = core.ValRef(tgtSymRepl)

              val inTpe = tgtSym.info.typeArgs.head
              val outTpe = targs.head
              val targsRepl = Seq(inTpe, outTpe)

              val ndc = core.DefCall(Some(DB$.ref), DB$.fold1, targsRepl, Seq(Seq(tgtRepl, alg)))
              val ndcRefDef = valRefAndDef(owner, "fold1", ndc)

              val blockFinal = core.Let(Seq(ndcRefDef._2), Seq(), ndcRefDef._1)
              val blockFinalSym = newValSym(owner, "db", blockFinal)
              val blockFinalRefDef = valRefAndDef(blockFinalSym, blockFinal)
              skip(blockFinalRefDef._2)

              replacements += (lhs -> blockFinalSym)
              defs += (blockFinalSym -> blockFinalRefDef._2)

              blockFinalRefDef._2

            // fold2
            // val db = ...
            // val zero: B = ...
            // val init: (A => B) = ...
            // val plus: (B => B) = ...
            // val nondb = db.fold[B](zero)(init, plus)
            //=========================
            // val db = ...
            // val zero: B = ...
            // val init: (A => B) = ...
            // val plus: (B => B) = ...
            // val db = DB.foldToBag[A,B](db, zero, init, plus)
            case core.DefCall(Some(core.ValRef(tgtSym)), DataBag.fold2, targs, Seq(Seq(zero), Seq(init, plus))) =>
              zero match {
                case core.Lit(_) => ()
                case _ => assert(false, "not supported type in fold2: zero has to be literal")
              }
              val tgtSymRepl =
                if (replacements.keys.toList.map(_.name).contains(tgtSym.name)) replacements(tgtSym)
                else tgtSym
              val tgtRepl = core.ValRef(tgtSymRepl)

              val inTpe = tgtSym.info.typeArgs.head
              val outTpe = targs.head
              val targsRepl = Seq(inTpe, outTpe)

              val ndc = core.DefCall(Some(DB$.ref), DB$.fold2, targsRepl, Seq(Seq(tgtRepl, zero, init, plus)))
              val ndcRefDef = valRefAndDef(owner, "fold2", ndc)

              val blockFinal = core.Let(Seq(ndcRefDef._2), Seq(), ndcRefDef._1)
              val blockFinalSym = newValSym(owner, "db", blockFinal)
              val blockFinalRefDef = valRefAndDef(blockFinalSym, blockFinal)
              skip(blockFinalRefDef._2)

              replacements += (lhs -> blockFinalSym)
              defs += (blockFinalSym -> blockFinalRefDef._2)

              blockFinalRefDef._2

            // if there is 1 non-constant argument inside the defcall, call map on argument databag
            case dc @ core.DefCall(_, _, _, _) if countSeenRefs(dc, replacements)==1 && !isDatabag(dc) =>
              val refs = dc.collect{
                case vr @ core.ValRef(_) => vr
                case pr @ core.ParRef(_) => pr
              }
              // here we have to use ref names to compare different refs refering to the same valdef
              val nonC = refs.filter(e => replacements.keys.toList.map(_.name).contains(e.name))

              val x = nonC(0) match {
                case core.ValRef(sym) => core.ValRef(replacements(sym))
                case core.ParRef(sym) => core.ParRef(replacements(sym))
              }

              val lbdaSym = api.ParSym(owner, api.TermName.fresh("t"), nonC(0).tpe.widen)
              val lbdaRef = core.ParRef(lbdaSym)
              //   lambda = t -> {
              //     t.f(c1, ..., ck)(impl ...)
              //   }

              val m = Map(nonC(0) -> lbdaRef)

              val lmbdaRhsDC = api.TopDown.transform{
                case v @ core.ValRef(_) => if (m.keys.toList.contains(v)) m(v) else v
                case p @ core.ParRef(_) => if (m.keys.toList.contains(p)) m(p) else p
              }._tree(dc)
              val lmbdaRhsDCRefDef = valRefAndDef(owner, "lbdaRhs", lmbdaRhsDC)
              skip(lmbdaRhsDCRefDef._2)
              val lmbdaRhs = core.Let(Seq(lmbdaRhsDCRefDef._2), Seq(), lmbdaRhsDCRefDef._1)
              val lmbda = core.Lambda(
                Seq(lbdaSym),
                lmbdaRhs
              )
              val lambdaRefDef = valRefAndDef(owner, "lambda", lmbda)

              val mapDC = core.DefCall(Some(x), DataBag.map, Seq(dc.tpe), Seq(Seq(lambdaRefDef._1)))
              val mapDCRefDef = valRefAndDef(owner, "map", mapDC)
              skip(mapDCRefDef._2)

              val blockFinal = core.Let(Seq(lambdaRefDef._2, mapDCRefDef._2), Seq(), mapDCRefDef._1)
              val blockFinalSym = newValSym(owner, "db", blockFinal)
              val blockFinalRefDef = valRefAndDef(blockFinalSym, blockFinal)
              skip(blockFinalRefDef._2)

              replacements += (lhs -> blockFinalSym)
              defs += (blockFinalSym -> blockFinalRefDef._2)

              blockFinalRefDef._2

            // if there are 2 non-constant arguments inside the defcall, cross and apply the defcall method to the tuple
            case dc @ core.DefCall(_, _, _, _) if countSeenRefs(dc, replacements)==2 && !isDatabag(dc)  =>
              val refs = dc.collect{
                case vr @ core.ValRef(_) => vr
                case pr @ core.ParRef(_) => pr
              }
              // here we have to use ref names to compare different refs refering to the same valdef
              val nonC = refs.filter(e => replacements.keys.toList.map(_.name).contains(e.name))
              val nonCReplRefs = nonC.map {
                case core.ValRef(sym) => core.ValRef(replacements(sym))
                case core.ParRef(sym) => core.ParRef(replacements(sym))
              }

              val targsRepls = nonC.map(_.tpe.widen)
              val crossDc = core.DefCall(Some(Ops.ref), Ops.cross, targsRepls, Seq(nonCReplRefs))
              skip(crossDc)

              val x = nonC(0)
              val y = nonC(1)

              val xyTpe = api.Type.kind2[Tuple2](x.tpe, y.tpe)
              val lbdaSym = api.ParSym(owner, api.TermName.fresh("t"), xyTpe)
              val lbdaRef = core.ParRef(lbdaSym)
              //   lambda = t -> {
              //     t1 = t._1
              //     t2 = t._2
              //     t1.f(c1, ..., cn, t2, cn+2, ..., ck)(impl ...)
              //   }

              //     t1 = t._1
              val t1 = core.DefCall(Some(lbdaRef), _1, Seq(), Seq())
              val t1RefDef = valRefAndDef(owner, "t1", t1)
              skip(t1RefDef._2)

              //     t2 = t._2
              val t2 = core.DefCall(Some(lbdaRef), _2, Seq(), Seq())
              val t2RefDef = valRefAndDef(owner, "t2", t2)
              skip(t2RefDef._2)

              val m = Map(x -> t1RefDef._1, y -> t2RefDef._1)

              val lmbdaRhsDC = api.TopDown.transform{
                case v @ core.ValRef(_) => if (m.keys.toList.contains(v)) m(v) else v
                case p @ core.ParRef(_) => if (m.keys.toList.contains(p)) m(p) else p
              }._tree(dc)
              val lmbdaRhsDCRefDef = valRefAndDef(owner, "lbdaRhs", lmbdaRhsDC)
              skip(lmbdaRhsDCRefDef._2)
              val lmbdaRhs = core.Let(Seq(t1RefDef._2, t2RefDef._2, lmbdaRhsDCRefDef._2), Seq(), lmbdaRhsDCRefDef._1)
              val lmbda = core.Lambda(
                Seq(lbdaSym),
                lmbdaRhs
              )
              val lambdaRefDef = valRefAndDef(owner, "lambda", lmbda)

              val crossSym = newValSym(owner, "cross", crossDc)
              val crossRefDef = valRefAndDef(crossSym, crossDc)
              skip(crossRefDef._2)

              val mapDC = core.DefCall(Some(crossRefDef._1), DataBag.map, Seq(dc.tpe), Seq(Seq(lambdaRefDef._1)))
              val mapDCRefDef = valRefAndDef(owner, "map", mapDC)
              skip(mapDCRefDef._2)

              val blockFinal = core.Let(Seq(crossRefDef._2, lambdaRefDef._2, mapDCRefDef._2), Seq(), mapDCRefDef._1)
              val blockFinalSym = newValSym(owner, "db", blockFinal)
              val blockFinalRefDef = valRefAndDef(blockFinalSym, blockFinal)
              skip(blockFinalRefDef._2)

              replacements += (lhs -> blockFinalSym)
              defs += (blockFinalSym -> blockFinalRefDef._2)

              blockFinalRefDef._2

            // if there are 3 non-constant arguments inside the defcall, cross and apply the defcall method to the tuple
            case dc @ core.DefCall(_, _, _, _) if countSeenRefs(dc, replacements)==3 && !isDatabag(dc)  =>
              val refs = dc.collect{
                case vr @ core.ValRef(_) => vr
                case pr @ core.ParRef(_) => pr
              }
              val nonC = refs.filter(e => replacements.keys.toList.map(_.name).contains(e.name))
              val nonCReplRefs = nonC.map {
                case core.ValRef(sym) => core.ValRef(replacements(sym))
                case core.ParRef(sym) => core.ParRef(replacements(sym))
              }

              val targsRepls = nonC.map(_.tpe.widen)
              val crossDc = core.DefCall(Some(DB$.ref), DB$.cross3, targsRepls, Seq(nonCReplRefs))
              skip(crossDc)

              val x = nonC(0)
              val y = nonC(1)
              val z = nonC(2)

              val xyzTpe = api.Type.kind3[Tuple3](x.tpe, y.tpe, z.tpe)
              val lbdaSym = api.ParSym(owner, api.TermName.fresh("t"), xyzTpe)
              val lbdaRef = core.ParRef(lbdaSym)
              //   lambda = t -> {
              //     t1 = t._1
              //     t2 = t._2
              //     t3 = t._3
              //     t1.f(c1, ..., cn, t2, cn+2, ..., ck)(impl ...)
              //   }

              //     t1 = t._1
              val t1 = core.DefCall(Some(lbdaRef), _3_1, Seq(), Seq())
              val t1RefDef = valRefAndDef(owner, "t1", t1)
              skip(t1RefDef._2)

              //     t2 = t._2
              val t2 = core.DefCall(Some(lbdaRef), _3_2, Seq(), Seq())
              val t2RefDef = valRefAndDef(owner, "t2", t2)
              skip(t2RefDef._2)

              //     t2 = t._3
              val t3 = core.DefCall(Some(lbdaRef), _3_3, Seq(), Seq())
              val t3RefDef = valRefAndDef(owner, "t3", t3)
              skip(t3RefDef._2)

              val m = Map(x -> t1RefDef._1, y -> t2RefDef._1, z -> t3RefDef._1)

              val lmbdaRhsDC = api.TopDown.transform{
                case v @ core.ValRef(_) => if (m.keys.toList.contains(v)) m(v) else v
                case p @ core.ParRef(_) => if (m.keys.toList.contains(p)) m(p) else p
              }._tree(dc)
              val lmbdaRhsDCRefDef = valRefAndDef(owner, "lbdaRhs", lmbdaRhsDC)
              skip(lmbdaRhsDCRefDef._2)
              val lmbdaRhs = core.Let(
                Seq(t1RefDef._2, t2RefDef._2, t3RefDef._2, lmbdaRhsDCRefDef._2),
                Seq(),
                lmbdaRhsDCRefDef._1
              )
              val lmbda = core.Lambda(
                Seq(lbdaSym),
                lmbdaRhs
              )
              val lambdaRefDef = valRefAndDef(owner, "lambda", lmbda)

              val crossSym = newValSym(owner, "cross3", crossDc)
              val crossRefDef = valRefAndDef(crossSym, crossDc)
              skip(crossRefDef._2)

              val mapDC = core.DefCall(Some(crossRefDef._1), DataBag.map, Seq(dc.tpe), Seq(Seq(lambdaRefDef._1)))
              val mapDCRefDef = valRefAndDef(owner, "map", mapDC)
              skip(mapDCRefDef._2)

              val blockFinal = core.Let(Seq(crossRefDef._2, lambdaRefDef._2, mapDCRefDef._2), Seq(), mapDCRefDef._1)
              val blockFinalSym = newValSym(owner, "db", blockFinal)
              val blockFinalRefDef = valRefAndDef(blockFinalSym, blockFinal)
              skip(blockFinalRefDef._2)

              replacements += (lhs -> blockFinalSym)
              defs += (blockFinalSym -> blockFinalRefDef._2)

              blockFinalRefDef._2

            case _ => {
              vd
            }

          }

        // if valdef rhs is not of type DataBag, turn it into a databag
        case Attr.inh(vd @ core.ValDef(lhs, rhs), owner :: _) if !meta(vd).all.all.contains(SkipTraversal)
            && !refsSeen(rhs, replacements) && !isDatabag(rhs) && !isFun(lhs) && !hasFunInOwnerChain(lhs)
            && !isAlg(rhs) =>

          // create lambda () => rhs
          val rhsSym = newValSym(owner, "tmp", rhs)
          val rhsRefDef = valRefAndDef(rhsSym, rhs)
          skip(rhsRefDef._2)
          val lRhs = core.Let(Seq(rhsRefDef._2), Seq(), rhsRefDef._1)
          val l = core.Lambda(Seq(), lRhs)
          val lSym = newValSym(owner, "fun", l)
          val lRefDef = valRefAndDef(lSym, l)
          skip(lRefDef._2)

          val dbRhsDC = core.DefCall(Some(DB$.ref), DB$.singSrc, Seq(rhs.tpe), Seq(Seq(lRefDef._1)))
          val dbRhsDCSym = newValSym(owner, "dbRhs", dbRhsDC)
          val dbRhsDCRefDef = valRefAndDef(dbRhsDCSym, dbRhsDC)
          skip(dbRhsDCRefDef._1)
          skip(dbRhsDCRefDef._2)
          val dbRhs = core.Let(Seq(lRefDef._2, dbRhsDCRefDef._2), Seq(), dbRhsDCRefDef._1)
          val dbSym = newValSym(owner, "db", dbRhsDC)
          val db = core.ValDef(dbSym, dbRhs)
          skip(db)

          // save mapping of refs -> defs
          val dbDefs = db.collect{ case dbvd @ core.ValDef(ld, _) => (ld, dbvd) }
          dbDefs.map(t => defs += (t._1 -> t._2))

          replacements += (lhs -> dbSym)
          defs += (dbSym -> db)
          //postPrint(db)
          db

        // if we encounter a ParDef whose type is not DataBag (e.g. Int), databagify (e.g. DataBag[Int])
        case Attr.inh(pd @ core.ParDef(ts, _), owner::_) if !(ts.info.typeConstructor =:= API.DataBag.tpe)
            && !meta(pd).all.all.contains(SkipTraversal)
            && !hasFunInOwnerChain(ts) =>
          val nts = api.ParSym(
            owner,
            api.TermName.fresh("arg"),
            api.Type.apply(API.DataBag.tpe.typeConstructor, Seq(ts.info))
          )
          val npd = core.ParDef(nts)
          skip(npd)
          replacements += (ts -> nts)
          npd

        case Attr.inh(vr @ core.Ref(sym), _) =>
          if (replacements.keys.toList.contains(sym)) {
            val nvr = vr match {
              case core.ParRef(`sym`) => core.ParRef(replacements(sym))
              case core.ValRef(`sym`) => core.ValRef(replacements(sym))
            }
            skip(nvr)
            nvr
          } else {
            vr
          }

        case Attr.inh(vd @ core.ValDef(lhs, rhs), owner :: _) if isAlg(rhs) =>
          replacements += (lhs -> lhs)
          vd

        // if we encounter a letblock whose expr is a DefCall with literals as arguments, create valdefs for literals,
        // prepend to valdefs of the letblock and replace DefCall args with references
        case Attr.inh(lb @ core.Let(valDefs, defDefs, core.DefCall(tgt, ms, targs, args)), owner :: _)
          if argsContainLiterals(args) =>

          var tmpDefs = Seq[u.ValDef]()
          val argsRepl = args.map{
            sl => sl.map {
              case lt@core.Lit(_) => {
                val defSym = newValSym(owner, "anfLaby", lt)
                val ltRefDef = valRefAndDef(defSym, lt)
                tmpDefs = tmpDefs ++ Seq(ltRefDef._2)
                ltRefDef._1
              }
              case (t: u.Tree) => t
            }
          }

          val dc = core.DefCall(tgt, ms, targs, argsRepl)

          val letOut = core.Let(
            tmpDefs ++ valDefs,
            defDefs,
            dc
          )

          letOut

      }._tree(tree)

    // second traversal to correct block types
    // Background: scala does not change block types if expression type changes
    // (see internal/Trees.scala - Tree.copyAttrs)
    val secondRun = api.BottomUp.unsafe
      .withOwner
      .transformWith {
        case Attr.inh(lb @ core.Let(valdefs, defdefs, expr), _) if lb.tpe != expr.tpe =>
          val nlb = core.Let(valdefs, defdefs, expr)
          nlb
      }._tree(firstRun)

    val unnested = Core.unnest(secondRun)

    meta(unnested).update(tree match {
      case core.Let(_,_,core.Ref(sym)) if isDatabag(sym) => OrigReturnType(true)
      case _ => OrigReturnType(false)
    })

    // postPrint(unnested)
    unnested

  })

  def argsContainLiterals(args: Seq[Seq[u.Tree]]): Boolean = {
    var out = false
    args.foreach{
      sl => sl.foreach{
        case core.Lit(_) => out = true
        case (t: u.Tree) => t
      }
    }
    out
  }
}
