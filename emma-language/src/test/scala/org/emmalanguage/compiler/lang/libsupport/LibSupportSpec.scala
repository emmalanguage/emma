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

import org.scalactic.Equality

/** A spec for the `FunSupport.callGraph` function. */
class LibSupportSpec extends LibSupportExamples {

  import compiler._
  import compiler.api._

  val prePipeline: u.Expr[Any] => u.Tree = compiler
    .pipeline(typeCheck = true, withPost = false)(
      api.Owner.atEncl
    ).compose(_.tree)

  val callGraph: u.Tree => CG = tree =>
    time(LibSupport.callGraph(tree), "callGraph")

  val strongyConnComp: CG => Set[Set[CG.Vertex]] = cg =>
    time(LibSupport.stronglyConnComp(cg), "strongyConnComp")

  val normalize: u.Tree => u.Tree = tree =>
    time((LibSupport.callGraph andThen LibSupport.normalize)(tree), "normalize")

  implicit private object CGEq extends Equality[CG] {
    override def areEqual(x: CG, a: Any): Boolean = a match {
      case y: CG =>
        // sizes are equal
        x.vs should have size y.vs.size
        x.es should have size y.es.size
        // vertices are equal
        val actVertices = x.vs.toSeq.sortBy(_.toString)
        val expVertices = y.vs.toSeq.sortBy(_.toString)
        for ((av, ev) <- actVertices zip expVertices)
          av shouldEqual ev
        // edges are equal
        val actEdges = x.es.toSeq.sortBy(_.toString)
        val expEdges = y.es.toSeq.sortBy(_.toString)
        for ((ae, ee) <- actEdges zip expEdges)
          ae shouldEqual ee
        // if no error so far, then the graphs are be equal
        true

      case _ =>
        false // rhs is not a graph
    }
  }

  "Exampe A" - {

    type Lib = lib.example.type
    implicit val ltag = u.typeTag[lib.example.type]

    val inp = prePipeline(`Example A (Emma Source)`)

    val expGraph = {
      //@formatter:off
      val snippet = CG.Snippet(inp)
      val libfuns = Seq(
        'plus    -> libfunVertex[Lib]("plus"),
        'times   -> libfunVertex[Lib]("times"),
        'square  -> libfunVertex[Lib]("square")
      ).toMap
      val lambdas = Seq(
        'check   -> lambdaVertex("check", inp)
      ).toMap

      val edges = Set[CG.Edge](
        CG.Calls(snippet          , lambdas('check)),
        CG.Calls(snippet          , libfuns('plus)),
        CG.Calls(snippet          , libfuns('times)),
        CG.Calls(snippet          , libfuns('square)),
        CG.Calls(libfuns('square) , libfuns('times))
      )
      //@formatter:on

      CG(Set(snippet) ++ (libfuns ++ lambdas).values.toSet, edges)
    }

    val expSCCs = Set[Set[CG.Vertex]](
      Set(CG.Snippet(inp)),
      Set(lambdaVertex("check", inp)),
      Set(libfunVertex[Lib]("plus")),
      Set(libfunVertex[Lib]("times")),
      Set(libfunVertex[Lib]("square"))
    )

    val expNTree = prePipeline(`Example A (normalized)`)

    "call graph" in {
      callGraph(inp) shouldEqual expGraph
    }
    "strongly connected components" in {
      strongyConnComp(expGraph) shouldEqual expSCCs
    }
    "normalized code" in {
      normalize(inp) shouldBe alphaEqTo(expNTree)
    }
  }

  "Exampe B" - {

    type Lib = lib.example.type
    implicit val ltag = u.typeTag[lib.example.type]

    val inp = prePipeline(`Example B (Emma Source)`)

    val expGraph = {
      //@formatter:off
      val snippet = CG.Snippet(inp)
      val lambdas = Map.empty[Symbol, CG.Lambda]
      val libfuns = Seq(
        'f1 -> libfunVertex[Lib]("f1"),
        'f2 -> libfunVertex[Lib]("f2")
      ).toMap

      val edges = Set[CG.Edge](
        CG.Calls(snippet      , libfuns('f1)),
        CG.Calls(libfuns('f1) , libfuns('f2)),
        CG.Calls(libfuns('f2) , libfuns('f1))
      )
      //@formatter:on

      CG(Set(snippet) ++ (libfuns ++ lambdas).values.toSet, edges)
    }

    val expSCCs = Set[Set[CG.Vertex]](
      Set(CG.Snippet(inp)),
      Set(libfunVertex[Lib]("f1"), libfunVertex[Lib]("f2"))
    )

    "call graph" in {
      callGraph(inp) shouldEqual expGraph
    }
    "strongly connected components" in {
      strongyConnComp(expGraph) shouldEqual expSCCs
    }
  }

  "Exampe C" - {

    type Lib = lib.example.type
    implicit val ltag = u.typeTag[lib.example.type]

    val inp = prePipeline(`Example C (Emma Source)`)

    val expGraph = {
      //@formatter:off
      val snippet = CG.Snippet(inp)
      val libfuns = Seq(
        'g1    -> libfunVertex[Lib]("g1"),
        'g2    -> libfunVertex[Lib]("g2"),
        'g3    -> libfunVertex[Lib]("g3")
      ).toMap
      val lambdas = Seq(
        'g4    -> lambdaVertex("g4", libfunVertex[Lib]("g1").tree)
      ).toMap
      val funpars = Seq(
        'g5    -> funparVertex("g5", libfunVertex[Lib]("g2").tree)
      ).toMap

      val edges = Set[CG.Edge](
        CG.Calls(snippet      , libfuns('g1)),
        CG.Calls(libfuns('g1) , libfuns('g2)),
        CG.Calls(libfuns('g3) , libfuns('g1)),
        CG.Calls(lambdas('g4) , libfuns('g3)),
        CG.Calls(libfuns('g2) , funpars('g5)),
        CG.Binds(funpars('g5) , lambdas('g4))
      )
      //@formatter:on

      CG(Set(snippet) ++ (libfuns ++ lambdas ++ funpars).values.toSet, edges)
    }

    val expSCCs = Set[Set[CG.Vertex]](
      Set(CG.Snippet(inp)),
      Set(
        libfunVertex[Lib]("g1"),
        libfunVertex[Lib]("g2"),
        libfunVertex[Lib]("g3"),
        lambdaVertex("g4", libfunVertex[Lib]("g1").tree),
        funparVertex("g5", libfunVertex[Lib]("g2").tree)
      )
    )

    "call graph" in {
      callGraph(inp) shouldEqual expGraph
    }
    "strongly connected components" in {
      strongyConnComp(expGraph) shouldEqual expSCCs
    }
  }

  "Exampe D" - {

    type Lib = lib.example.type
    implicit val ltag = u.typeTag[lib.example.type]

    val inp = prePipeline(`Example D (Emma Source)`)

    val expGraph = {
      //@formatter:off
      val snippet = CG.Snippet(inp)
      val lambdas = Map.empty[Symbol, CG.Lambda]
      val libfuns = Seq(
        'baz   -> libfunVertex[Lib]("baz"),
        'h1    -> libfunVertex[Lib]("h1"),
        'h2    -> libfunVertex[Lib]("h2"),
        'h3    -> libfunVertex[Lib]("h3")
      ).toMap

      val edges = Set[CG.Edge](
        CG.Calls(snippet      , libfuns('baz)),
        CG.Calls(snippet      , libfuns('h1)),
        CG.Calls(libfuns('h1) , libfuns('h2)),
        CG.Calls(libfuns('h2) , libfuns('h3)),
        CG.Calls(libfuns('h3) , libfuns('h1))
      )
      //@formatter:on

      CG(Set(snippet) ++ (libfuns ++ lambdas).values.toSet, edges)
    }

    val expSCCs = Set[Set[CG.Vertex]](
      Set(CG.Snippet(inp)),
      Set(libfunVertex[Lib]("baz")),
      Set(
        libfunVertex[Lib]("h1"),
        libfunVertex[Lib]("h2"),
        libfunVertex[Lib]("h3")
      )
    )

    "call graph" in {
      val actGraph = callGraph(inp)

      actGraph.vs should have size expGraph.vs.size
      actGraph.es should have size expGraph.es.size

      actGraph shouldEqual expGraph
    }
    "strongly connected components" in {
      strongyConnComp(expGraph) shouldEqual expSCCs
    }
  }

  "Exampe E" - {

    type Lib = lib.example.type
    implicit val ltag = u.typeTag[lib.example.type]

    val inp = prePipeline(`Example E (Emma Source)`)

    val expGraph = {
      //@formatter:off
      val snippet = CG.Snippet(inp)
      val libfuns = Seq(
        'pow   -> libfunVertex[Lib]("pow"),
        'times -> libfunVertex[Lib]("times")
      ).toMap

      val edges = Set[CG.Edge](
        CG.Calls(snippet       , libfuns('pow)),
        CG.Calls(libfuns('pow) , libfuns('times))
      )
      //@formatter:on

      CG(Set(snippet) ++ libfuns.values.toSet, edges)
    }

    val expSCCs = Set[Set[CG.Vertex]](
      Set(CG.Snippet(inp)),
      Set(libfunVertex[Lib]("pow")),
      Set(libfunVertex[Lib]("times"))
    )

    "call graph" in {
      callGraph(inp) shouldEqual expGraph
    }
    "strongly connected components" in {
      strongyConnComp(expGraph) shouldEqual expSCCs
    }
    "normalized" in {
      normalize(inp) shouldBe alphaEqTo(prePipeline(`Example E (normalized)`))
    }
  }

  "Exampe F" - {

    type Lib = lib.example.type
    implicit val ltag = u.typeTag[lib.example.type]

    val inp = prePipeline(`Example F (Emma Source)`)

    val expGraph = {
      //@formatter:off
      val snippet = CG.Snippet(inp)
      val libfuns = Seq(
        'sameAs -> libfunVertex[Lib]("sameAs")
      ).toMap

      val edges = Set[CG.Edge](
        CG.Calls(snippet, libfuns('sameAs))
      )
      //@formatter:on

      CG(Set(snippet) ++ libfuns.values.toSet, edges)
    }

    val expSCCs = Set[Set[CG.Vertex]](
      Set(CG.Snippet(inp)),
      Set(libfunVertex[Lib]("sameAs"))
    )

    "call graph" in {
      callGraph(inp) shouldEqual expGraph
    }
    "strongly connected components" in {
      strongyConnComp(expGraph) shouldEqual expSCCs
    }
    "normalized" in {
      normalize(inp) shouldBe alphaEqTo(prePipeline(`Example F (normalized)`))
    }
  }

  "Exampe G" - {

    type Lib = lib.example.type
    implicit val ltag = u.typeTag[lib.example.type]

    val inp = prePipeline(`Example G (Emma Source)`)

    val expGraph = {
      //@formatter:off
      val snippet = CG.Snippet(inp)
      val libfuns = Seq(
        'xpfx   -> libfunVertex[Lib]("xpfx"),
        'square -> libfunVertex[Lib]("square"),
        'times  -> libfunVertex[Lib]("times")
      ).toMap
      val funpars = Seq(
        'f     -> funparVertex("f", libfunVertex[Lib]("xpfx").tree)
      ).toMap

      val edges = Set[CG.Edge](
        CG.Calls(snippet          , libfuns('xpfx)),
        CG.Calls(snippet          , libfuns('square)),
        CG.Calls(libfuns('square) , libfuns('times)),
        CG.Calls(libfuns('xpfx)   , funpars('f))
      )
      //@formatter:on

      CG(Set(snippet) ++ (libfuns ++ funpars).values.toSet, edges)
    }

    val expSCCs = Set[Set[CG.Vertex]](
      Set(CG.Snippet(inp)),
      Set(libfunVertex[Lib]("xpfx")),
      Set(libfunVertex[Lib]("square")),
      Set(libfunVertex[Lib]("times")),
      Set(funparVertex("f", libfunVertex[Lib]("xpfx").tree))
    )

    "call graph" in {
      callGraph(inp) shouldEqual expGraph
    }
    "strongly connected components" in {
      strongyConnComp(expGraph) shouldEqual expSCCs
    }
    "normalized" in {
      normalize(inp) shouldBe alphaEqTo(prePipeline(`Example G (normalized)`))
    }
  }

  // ---------------------------------------------------------------------------
  // Helper functions
  // ---------------------------------------------------------------------------

  private def lambdaVertex(name: String, tree: u.Tree): CG.Lambda =
    tree.collect({
      case ValDef(sym, lambda@Lambda(_, _, _))
        if sym.name == u.TermName(name) => CG.Lambda(sym, lambda)
    }).head

  private def funparVertex(name: String, tree: u.Tree): CG.FunPar =
    tree.collect({
      case ParDef(sym, _)
        if sym.name == u.TermName(name) => CG.FunPar(sym)
    }).head

  private def libfunVertex[T: u.TypeTag](name: String): CG.LibDef = {
    val tpe = api.Type(implicitly[u.TypeTag[T]])
    val sym = tpe.decl(api.TermName(name)).asMethod
    LibDefRegistry(sym).map(ast => CG.LibDef(sym, ast)).get
  }
}
