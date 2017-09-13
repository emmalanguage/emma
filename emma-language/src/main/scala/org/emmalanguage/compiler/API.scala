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

import ast.AST

/** Common IR tools. */
protected[emmalanguage] trait API extends AST {

  import UniverseImplicits._

  trait ReflectedSymbol[S <: u.Symbol] {
    def sym: S

    def ops: Set[u.MethodSymbol]

    protected def op(name: String): u.MethodSymbol =
      sym.info.member(api.TermName(name)).alternatives.collectFirst({
        case api.DefSym(m) if m.isPublic => m
      }).get

    protected def op(name: String, arities: List[Int]): u.MethodSymbol =
      sym.info.member(api.TermName(name)).alternatives.collectFirst({
        case api.DefSym(m) if m.isPublic && m.info.paramLists.map(_.size) == arities => m
      }).get

    protected def ann(sym: u.ClassSymbol) =
      u.Annotation(api.Inst(sym.toType, argss = Seq(Seq.empty)))
  }

  trait ClassAPI extends ReflectedSymbol[u.ClassSymbol] {
    def tpe = sym.toTypeConstructor
  }

  trait ModuleAPI extends ReflectedSymbol[u.ModuleSymbol] {
    //@formatter:off
    lazy val ref              = api.Ref(sym)
    //@formatter:on
  }

  trait emmaAPI extends ModuleAPI {
    //@formatter:off
    lazy val sym              = api.Sym[org.emmalanguage.api.emma.`package`.type].asModule

    val prettyPrint           = op("prettyPrint")
    val quote                 = op("quote")

    lazy val ops              = Set(prettyPrint, quote)
    //@formatter:on
  }

  class DataBagAPI(cls: u.ClassSymbol) extends ClassAPI {

    def apply(A: u.Type): u.Type = api.Type(tpe, Seq(A))

    //@formatter:off
    lazy val sym              = cls

    // Sinks
    val collect               = op("collect")
    val as                    = op("as")
    val writeCSV              = op("writeCSV")
    val writeParquet          = op("writeParquet")
    val writeText             = op("writeText")
    // Monad ops
    val map                   = op("map")
    val flatMap               = op("flatMap")
    val withFilter            = op("withFilter")
    // Grouping
    val groupBy               = op("groupBy")
    // Set ops
    val union                 = op("union")
    val distinct              = op("distinct")
    // Partition-based ops
    val sample                = op("sample")
    val zipWithIndex          = op("zipWithIndex")
    // Structural recursion & Folds
    val fold1                 = op("fold", List(1 /*alg*/ , 1 /*meta*/))
    val fold2                 = op("fold", List(1 /*zero*/ , 2 /*init, plus*/ , 1 /*meta*/))
    val isEmpty               = op("isEmpty")
    val nonEmpty              = op("nonEmpty")
    val reduce                = op("reduce")
    val reduceOption          = op("reduceOption")
    val min                   = op("min")
    val max                   = op("max")
    val sum                   = op("sum")
    val product               = op("product")
    val size                  = op("size")
    val count                 = op("count")
    val exists                = op("exists")
    val forall                = op("forall")
    val find                  = op("find")
    val bottom                = op("bottom")
    val top                   = op("top")

    val sinkOps               = Set(collect, as, writeCSV, writeParquet, writeText)
    val monadOps              = Set(map, flatMap, withFilter)
    val nestOps               = Set(groupBy)
    val boolAlgOps            = Set(union, distinct)
    val partOps               = Set(sample, zipWithIndex)
    val foldOps               = Set(
      fold1, fold2,
      isEmpty, nonEmpty,
      reduce, reduceOption,
      min, max, sum, product,
      size, count,
      exists, forall,
      find, top, bottom
    )

    val ops = sinkOps | monadOps | nestOps | boolAlgOps | partOps | foldOps
    //@formatter:on
  }

  class DataBag$API(mod: u.ModuleSymbol) extends ModuleAPI {
    //@formatter:off
    lazy val sym              = mod

    val from                  = op("from")
    val empty                 = op("empty")
    val apply                 = op("apply")
    val readCSV               = op("readCSV")
    val readParquet           = op("readParquet")
    val readText              = op("readText")

    lazy val ops              = Set(from, empty, apply, readCSV, readParquet, readText)
    //@formatter:on
  }

  class MutableBagAPI(cls: u.ClassSymbol) extends ClassAPI {
    //@formatter:off
    lazy val sym              = cls

    val update                = op("update")
    val bag                   = op("bag")
    val copy                  = op("copy")

    lazy val ops              = Set(update, bag, copy)
    //@formatter:on
  }

  class MutableBag$API(mod: u.ModuleSymbol) extends ModuleAPI {
    //@formatter:off
    lazy val sym              = mod

    val apply                 = op("apply")

    lazy val ops              = Set(apply)
    //@formatter:on
  }

  trait GroupAPI extends ClassAPI {

    def apply(K: u.Type, V: u.Type): u.Type = api.Type(tpe, Seq(K, V))

    //@formatter:off
    lazy val sym              = api.Sym[org.emmalanguage.api.Group[Any, Any]].asClass

    val key                   = op("key")
    val values                = op("values")

    lazy val ops              = Set(key, values)
    //@formatter:on
  }

  class Group$API extends ModuleAPI {
    //@formatter:off
    lazy val sym              = api.Sym[org.emmalanguage.api.Group.type].asModule

    val apply                 = op("apply")

    lazy val ops              = Set(apply)
    //@formatter:on
  }

  trait ComprehensionCombinatorsAPI[S <: u.Symbol] extends ReflectedSymbol[S] {
    //@formatter:off
    val cross                 = op("cross")
    val equiJoin              = op("equiJoin")
    val foldGroup             = op("foldGroup")

    def ops                   = Set(cross, equiJoin, foldGroup)
    //@formatter:on
  }

  trait RuntimeAPI[S <: u.Symbol] extends ReflectedSymbol[S] {
    //@formatter:off
    val cache                 = op("cache")

    def ops                   = Set(cache)
    //@formatter:on
  }

  class OpsAPI(mod: u.ModuleSymbol) extends ModuleAPI
    with ComprehensionCombinatorsAPI[u.ModuleSymbol]
    with RuntimeAPI[u.ModuleSymbol] {
    //@formatter:off
    lazy val sym              = mod

    override lazy val ops     = super[ComprehensionCombinatorsAPI].ops ++ super[RuntimeAPI].ops
    //@formatter:on
  }

  trait ComprehensionSyntaxAPI extends ModuleAPI {
    //@formatter:off
    lazy val sym            = api.Sym[ir.ComprehensionSyntax.type].asModule

    val generator           = op("generator")
    val comprehension       = op("comprehension")
    val guard               = op("guard")
    val head                = op("head")

    lazy val ops            = Set(generator, comprehension, guard, head)
    //@formatter:on
  }

  trait GraphRepresentationAPI extends ModuleAPI {
    //@formatter:off
    lazy val sym              = api.Sym[ir.GraphRepresentation.type].asModule

    val phi                   = op("phi")

    lazy val ops              = Set(phi)
    //@formatter:on
  }

  trait DSCFAnnotationsAPI extends ModuleAPI {
    //@formatter:off
    lazy val sym              = api.Sym[ir.DSCFAnnotations.type].asModule

    // Annotation symbols
    val branch                = api.Sym[ir.DSCFAnnotations.branch].asClass
    val loop                  = api.Sym[ir.DSCFAnnotations.loop].asClass
    val suffix                = api.Sym[ir.DSCFAnnotations.suffix].asClass
    val thenBranch            = api.Sym[ir.DSCFAnnotations.thenBranch].asClass
    val elseBranch            = api.Sym[ir.DSCFAnnotations.elseBranch].asClass
    val whileLoop             = api.Sym[ir.DSCFAnnotations.whileLoop].asClass
    val doWhileLoop           = api.Sym[ir.DSCFAnnotations.doWhileLoop].asClass
    val loopBody              = api.Sym[ir.DSCFAnnotations.loopBody].asClass

    // Annotation trees
    val suffixAnn             = ann(suffix)
    val thenAnn               = ann(thenBranch)
    val elseAnn               = ann(elseBranch)
    val whileAnn              = ann(whileLoop)
    val doWhileAnn            = ann(doWhileLoop)
    val loopBodyAnn           = ann(loopBody)

    lazy val ops              = Set.empty[u.MethodSymbol]
    //@formatter:on
  }

  /** Backend-specific APIs. This trait should be implemented by each backend. */
  trait BackendAPI {

    def implicitTypes: Set[u.Type]

    def DataBag: DataBagAPI

    def DataBag$: DataBag$API

    def MutableBag: MutableBagAPI

    def MutableBag$: MutableBag$API

    def Ops: OpsAPI
  }

  /** Reflection of the Emma API. */
  object API {

    val implicitTypes = Set(
      api.Type[org.emmalanguage.api.Meta[Any]].typeConstructor,
      api.Type[org.emmalanguage.api.LocalEnv],
      api.Type[org.emmalanguage.io.csv.CSVConverter[Any]].typeConstructor,
      api.Type[org.emmalanguage.io.parquet.ParquetConverter[Any]].typeConstructor
    )

    object emma extends emmaAPI

    object DataBag extends DataBagAPI(api.Sym[org.emmalanguage.api.DataBag[Any]].asClass)

    object DataBag$ extends DataBag$API(api.Sym[org.emmalanguage.api.DataBag.type].asModule)

    object ScalaSeq extends DataBagAPI(api.Sym[org.emmalanguage.api.ScalaSeq[Any]].asClass)

    object ScalaSeq$ extends DataBag$API(api.Sym[org.emmalanguage.api.ScalaSeq.type].asModule) {
      //@formatter:off
      val fromDataBag       = op("fromDataBag")
      override lazy val ops = Set(fromDataBag, from, empty, apply, readCSV, readParquet, readText)
      //@formatter:on
    }

    object MutableBag extends MutableBagAPI(api.Sym[org.emmalanguage.api.MutableBag[Any, Any]].asClass)

    object MutableBag$ extends MutableBag$API(api.Sym[org.emmalanguage.api.MutableBag.type].asModule)

    object Group extends GroupAPI

    object Group$ extends Group$API

    object Ops extends OpsAPI(api.Sym[org.emmalanguage.api.backend.LocalOps.type].asModule)

    object ComprehensionCombinators extends ComprehensionCombinatorsAPI[u.ClassSymbol] {
      override def sym = api.Sym[org.emmalanguage.api.backend.ComprehensionCombinators[Any]].asClass
    }

    object Runtime extends RuntimeAPI[u.ClassSymbol] {
      override def sym = api.Sym[org.emmalanguage.api.backend.Runtime[Any]].asClass
    }

    object ComprehensionSyntax extends ComprehensionSyntaxAPI

    object GraphRepresentation extends GraphRepresentationAPI

    object DSCFAnnotations extends DSCFAnnotationsAPI

  }

}
