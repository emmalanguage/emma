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
package runtime

import data.plan.Plan
import eu.stratosphere.emma.api.ParallelizedDataBag
import eu.stratosphere.emma.ir._
import eu.stratosphere.emma.runtime.{Context, RuntimePlugin}

import scala.reflect.runtime.universe._

import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue}

class GuiPlugin(parties: Int = 1) extends RuntimePlugin {
  private var latch = new CountDownLatch(parties)
  val executionPlanQueue = new LinkedBlockingQueue[Plan]
  def resume() = latch.countDown()

  def handleLogicalPlan(root: Combinator[_], name: String, ctx: Context, closure: Any*) = {
    executionPlanQueue.put(new Plan(name, getExecutionPlan(root), ctx.srcPositions))
    latch.await()
    latch = new CountDownLatch(parties)
  }

  def getExecutionPlan[A](root: Combinator[A]): String = root match {
    case Read(location: String, _) =>
      buildJson(root, "Read", s"path:\n$location", "")
    case Write(location: String, _, xs) =>
      buildJson(root, "Write", s"path:\n$location", getExecutionPlan(xs))
    case TempSource(_) =>
      buildJson(root, "TempSource", "", "")
    case TempSink(name: String, xs) =>
      buildJson(root, s"TempSink\n($name)", "", getExecutionPlan(xs))
    case Map(f, xs) =>
      buildJson(root, "Map", s"function:\n$f", getExecutionPlan(xs))
    case FlatMap(f, xs) =>
      buildJson(root, "FlatMap", s"function:\n$f", getExecutionPlan(xs))
    case Filter(p, xs) =>
      buildJson(root, "Filter", s"predicate:\n$p", getExecutionPlan(xs))
    case EquiJoin(kx, ky, f, xs, ys) =>
      buildJson(root, "EquiJoin", s"Kx:\n$kx\nKy:\n$ky\nfunction:\n$f",
        s"${getExecutionPlan(xs)}, ${getExecutionPlan(ys)}")
    case Cross(f, xs, ys) =>
      buildJson(root, "Cross", s"function:\n$f",
        s"${getExecutionPlan(xs)}, ${getExecutionPlan(ys)}")
    case Group(key, xs) =>
      buildJson(root, "Group", s"key:\n$key", getExecutionPlan(xs))
    case Fold(emp, sng, uni, xs) =>
      buildJson(root, "Fold", s"empty:\n$emp\nsingle:\n$sng\nunion:\n$uni",
        getExecutionPlan(xs))
    case FoldGroup(key, emp, sng, uni, xs) =>
      buildJson(root, "FoldGroup", s"key:\n$key\nempty:\n$emp\nsingle:\n$sng\nunion:\n$uni",
        getExecutionPlan(xs))
    case Distinct(xs) =>
      buildJson(root, "Distinct", "", getExecutionPlan(xs))
    case Union(xs, ys) =>
      buildJson(root, "Union", "", s"${getExecutionPlan(xs)}, ${getExecutionPlan(ys)}")
    case Diff(xs, ys) =>
      buildJson(root, "Diff", "", s"${getExecutionPlan(xs)}, ${getExecutionPlan(ys)}")
    case Scatter(xs) =>
      buildJson(root, "Scatter", "", "")
    case StatefulCreate(xs) =>
      buildJson(root, "StatefulCreate", "", getExecutionPlan(xs))
    case StatefulFetch(name, _) =>
      buildJson(root, s"StatefulFetch\n($name)", "", "")
    case UpdateWithZero(name, _, udf) =>
      buildJson(root, s"Update\n($name)", s"function:\n$udf",
        getExecutionPlan(Read(name, null)))
    case UpdateWithOne(name, _, upd, key, udf) =>
      buildJson(root, s"UpdateWith\n($name)", s"key:\n$key\nfunction:\n$udf",
        s"${getExecutionPlan(Read(name, null))}, ${getExecutionPlan(upd)}")
    case UpdateWithMany(name, _, upd, key, udf) =>
      buildJson(root, s"UpdateWith*\n($name)", s"key:\n$key\nfunction:\n$udf",
        s"${getExecutionPlan(Read(name, null))}, ${getExecutionPlan(upd)}")
  }

  def buildJson(root: Combinator[_], name: String, toolTipText: String, parents: String): String = {
    val text = Literal(Constant(toolTipText)).toString()
    val json = name match {
      case "Read" =>
        s""""label": "$name", "type": "INPUT", "tooltip": $text"""
      case "Write" =>
        s""""label": "$name", "type": "INPUT", "tooltip": $text"""
      case "TempSource" =>
        val t = root.asInstanceOf[TempSource[_]]
        if (t.ref != null)
          s""""label": "TempSource (${t.ref.asInstanceOf[ParallelizedDataBag[_, _]].name})", "tooltip":$text"""
        else
          s""""label":"TempSource", "tooltip":$text"""
      case _ =>
        s""""label":"$name", "tooltip":$text"""
    }

    s"""{"id": "${System.identityHashCode(root)}", $json, "parents": [$parents]}"""
  }
}
