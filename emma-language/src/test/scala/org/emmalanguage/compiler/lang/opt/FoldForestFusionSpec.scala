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
package compiler.lang.opt

import api._
import compiler.BaseCompilerSpec

import scala.util.Random

/** A spec for the fold-fusion optimization. */
class FoldForestFusionSpec extends BaseCompilerSpec {

  import compiler._
  import universe.reify
  import UniverseImplicits._
  import Core.{Lang => core}

  val testPipeline: u.Tree => u.Tree =
    pipeline(typeCheck = true)(
      Core.anf,
      tree => {
        val cfg = CFG.graph(tree)
        time(FoldFusion.foldForestFusion(cfg)(tree), "foldForestFusion")
      },
      Core.dce)

  def defCalls(tree: u.Tree) = tree.collect {
    case core.DefCall(_, method, _, _) => method
  }

  val rand    = new Random()
  val rands   = DataBag(Seq.fill(100)(rand.nextInt()))
  val isPrime = (_: Int) => rand.nextBoolean() // Dummy function

  "fusion of fold trees" - {
    "one-layer banana-fusion" in {
      val fused = testPipeline(reify {
        val rands = this.rands
        (
          rands.isEmpty,
          rands.nonEmpty,
          rands.size,
          rands.min,
          rands.max,
          rands.sum,
          rands.product,
          rands.count(_ % 2 == 0),
          rands.exists(_ < 0),
          rands.forall(_ != 0),
          rands.find(_ > 0),
          rands.bottom(10),
          rands.top(10),
          rands.sample(10),
          rands.reduce(0)(_ * _ % 5),
          rands.reduceOption(_.abs min _.abs)
        )
      }.tree)

      val calls = defCalls(fused)
      calls.count(API.DataBag.foldOps) should be (1)
      calls.count(API.DataBag.monadOps) should be (0)
      calls.filter(API.DataBag.foldOps) should contain only API.DataBag.fold1
    }

    "one-layer cata-fusion" in {
      // Note that without CSE, `this.rands` is considered different in every instance.
      val fused = testPipeline(reify {
        val sqSum = rands.map(x => x * x).sum
        val minPos = rands.withFilter(_ > 0).min
        val nearPrimes = rands.flatMap(x => DataBag(x - 5 to x + 5)).count(isPrime)
        (sqSum, minPos, nearPrimes)
        sqSum
      }.tree)

      val calls = defCalls(fused)
      calls.count(API.DataBag.foldOps) should be (1)
      calls.count(API.DataBag.monadOps) should be (0)
      calls.filter(API.DataBag.foldOps) should contain only API.DataBag.fold1
    }

    "two-layer banana-fusion" in {
      val fused = testPipeline(reify {
        val rands = this.rands
        val squares = rands.map(x => x * x)
        val sqSum = squares.sum
        val sqPrimes = squares.map(_ + 1).count(isPrime)
        val minPos = rands.withFilter(_ > 0).min
        (sqSum, sqPrimes, minPos)
      }.tree)

      val calls = defCalls(fused)
      calls.count(API.DataBag.foldOps) should be (1)
      calls.count(API.DataBag.monadOps) should be (0)
      calls.filter(API.DataBag.foldOps) should contain only API.DataBag.fold1
    }

    "two-layer cata-fusion" in {
      val fused = testPipeline(reify {
        rands.withFilter(_ > 0).map(x => x * x).sum
      }.tree)

      val calls = defCalls(fused)
      calls.count(API.DataBag.foldOps) should be (1)
      calls.count(API.DataBag.monadOps) should be (0)
      calls.filter(API.DataBag.foldOps) should contain only API.DataBag.fold1
    }

    "preserving data dependencies" in {
      // Note that only `min` and `max` were fused,
      // resp. `map` and `count` were squashed below.
      val fused = testPipeline(reify {
        val rands = this.rands
        val min = rands.min
        val max = rands.max
        val range = (max - min).toDouble
        val scaled = rands.map(x => (x - min) / range)
        scaled.count(_ <= 0.5)
      }.tree)

      val calls = defCalls(fused)
      calls.count(API.DataBag.foldOps) should be (2)
      calls.count(API.DataBag.monadOps) should be (0)
      calls.filter(API.DataBag.foldOps) should contain only API.DataBag.fold1
    }

    "more than 22 independent folds" in {
      import universe._
      // More than 22 folds can still be fused via nested tuples.
      val fused = testPipeline(q"""
        val nums = org.emmalanguage.api.DataBag(1 to 100)
        val id   = identity[Int] _
        val sum  = implicitly[Numeric[Int]].plus _
        Seq(..${for (i <- 1 to 25) yield q"nums.fold($i)(id, sum)"})
      """)

      val calls = defCalls(fused)
      calls.count(API.DataBag.foldOps) should be (1)
      calls.count(API.DataBag.monadOps) should be (0)
      calls.filter(API.DataBag.foldOps) should contain only API.DataBag.fold1
    }
  }
}
