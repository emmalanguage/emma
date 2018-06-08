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

import api._
import lang.TreeMatchers

import org.scalatest.FreeSpec
import org.scalatest.Matchers
import org.scalatest.prop.PropertyChecks

import java.util.Properties
import java.util.UUID

/**
 * Common methods and mixins for all compier specs
 */
trait BaseCompilerSpec extends FreeSpec with Matchers with PropertyChecks with TreeMatchers {

  val compiler = new RuntimeCompiler()
  import compiler._

  // ---------------------------------------------------------------------------
  // Common transformation pipelines
  // ---------------------------------------------------------------------------

  /** Checks if the given tree compiles, and returns the given tree. */
  lazy val checkCompile: u.Tree => u.Tree = (tree: u.Tree) => {
    if (BaseCompilerSpec.compileSpecPipelines) {
      val wrapped = wrapInClass(tree)
      val showed = u.showCode(wrapped)
      compiler.compile(parse(showed))
    }
    tree
  }

  lazy val idPipeline: u.Expr[Any] => u.Tree = {
    compiler.identity(typeCheck = true) andThen checkCompile
  } compose (_.tree)

  /**
   * Combines a sequence of `transformations` into a pipeline with pre- and post-processing.
   * If the compile-spec-pipelines maven profile is set, then it also checks if the resulting code is
   * valid by compiling it.
   */
  protected def pipeline(
    typeCheck: Boolean = false, withPre: Boolean = true, withPost: Boolean = true
  )(
    transformations: TreeTransform*
  ): u.Tree => u.Tree = {
    TestEval(compiler.pipeline(typeCheck, withPre, withPost)(transformations: _*)) _
  } andThen {
    checkCompile
  }

  /** Evaluets the pipeline and prints the time for nodes marked with `time == true`. */
  object TestEval extends XfrmEval {
    type Dom = u.Tree => Res

    case class Inf(name: String, time: Option[Long])

    val showInf = scalaz.Show.shows[Inf](x => {
      val name = x.name
      val time = x.time.fold("")(t => s" (${t / 1e6}ms)")
      name + time
    })

    val eval = Xfrm.fold(new XfrmAlg[Dom] {
      def seq(name: String, seq: Seq[Dom], time: Boolean): Dom = tree => {
        val tim1 = System.nanoTime
        val rslt = seq.foldLeft(Acc(tree))((acc, fun) => {
          val rslt = fun(acc.tree)
          Acc(rslt.tree, acc.accx :+ rslt.xtra)
        })
        val tim2 = System.nanoTime
        val info = Inf(name, if (time) Some(tim2 - tim1) else None)
        Res(rslt.tree, scalaz.Tree.Node(info, rslt.accx))
      }

      def fun(name: String, fun: Xfrm.Fun, time: Boolean): Dom = tree => {
        val tim1 = System.nanoTime
        val rslt = fun(tree)
        val tim2 = System.nanoTime
        val info = Inf(name, if (time) Some(tim2 - tim1) else None)
        Res(rslt, scalaz.Tree.Leaf(info))
      }
    }) _

    def apply(xfrm: Xfrm)(tree: u.Tree): u.Tree = {
      val rslt = eval(xfrm)(tree)

      for {
        info <- rslt.xtra.flatten
        if info.time.isDefined
      } println(showInf.shows(info))

      rslt.tree
    }
  }

  // ---------------------------------------------------------------------------
  // Common value definitions used in compiler tests
  // ---------------------------------------------------------------------------

  val x = 42
  val y = "The answer to life, the universe and everything"
  val t = (x, y)
  val xs = DataBag(Seq(1, 2, 3))
  val ys = DataBag(Seq(1, 2, 3))

  // ---------------------------------------------------------------------------
  // Utility functions
  // ---------------------------------------------------------------------------

  protected def time[A](f: => A, name: String = "") = {
    val s = System.nanoTime
    val r = f
    val e = System.nanoTime
    println(s"$name time: ${(e - s) / 1e6}ms".trim)
    r
  }

  /** Wraps the given tree in a class and a method whose params are the closure of the tree. */
  protected def wrapInClass(tree: u.Tree): u.Tree = {
    import u.Quasiquote

    val Cls = api.TypeName(UUID.randomUUID().toString)
    val run = api.TermName(RuntimeCompiler.default.runMethod)
    val prs = api.Tree.closure(tree).map { sym =>
      val x = sym.name
      val T = sym.info
      q"val $x: $T"
    }

    q"""
    class $Cls {
      def $run(..$prs) = $tree
    }
    """
  }
}

object BaseCompilerSpec {

  import resource._

  val configLocation = "/test_config.properties"

  /**
   * Whether spec pipelines should try a toolbox compilation at the end, to potentially catch more bugs.
   * (Set by the compile-spec-pipelines maven profile; makes specs run about 5 times slower.)
   */
  lazy val compileSpecPipelines = props
    .getProperty("compile-spec-pipelines")
    .toBoolean

  lazy val props = {
    val p = new Properties()
    for {
      r <- managed(classOf[BaseCompilerSpec].getResourceAsStream(configLocation))
    } p.load(r)
    p
  }
}
