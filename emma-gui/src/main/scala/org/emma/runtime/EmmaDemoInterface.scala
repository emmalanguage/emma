package org.emma.runtime

import java.util.concurrent.CountDownLatch

import eu.stratosphere.emma.api.ParallelizedDataBag
import eu.stratosphere.emma.ir._
import eu.stratosphere.emma.runtime.{Context, RuntimePlugin}
import org.emma.data.plan.Plan

import scala.collection.mutable
import scala.reflect.runtime.universe._

class EmmaDemoInterface extends RuntimePlugin{
  var blockingLatch:CountDownLatch = null
  var executionPlanJson: mutable.Stack[Plan] = new mutable.Stack[Plan]()

  def setBlockingLatch(blocker: CountDownLatch) {
    blockingLatch = blocker
  }

  def getBlockingLatch():CountDownLatch = blockingLatch

  def getExecutionPlanJson(): mutable.Stack[Plan] = executionPlanJson

  override def handleLogicalPlan(root: Combinator[_], name: String, ctx: Context, closure: Any*): Unit = {
    if (blockingLatch != null) {
      executionPlanJson.push(new Plan(name, getExecutionPlan(root), ctx.srcPositions))
      blockingLatch.await()
    }
  }

  def getExecutionPlan(root:Combinator[_]) : String = root match {
    case Read(location: String, _) => buildJson(root, "Read", "path: \n"+root.asInstanceOf[Read[_]].location, "")
    case Write(location: String, _, xs: Combinator[_]) => buildJson(root, "Write", "path: \n"+root.asInstanceOf[Write[_]].location, getExecutionPlan(xs))
    case TempSource(ref) => buildJson(root, "TempSource", "", "")
    case TempSink(name: String, xs: Combinator[_]) => buildJson(root, "TempSink ("+name+")", "", getExecutionPlan(xs))
    case Map(function: String, xs: Combinator[_]) => buildJson(root, "Map", "function: \n"+function, getExecutionPlan(xs))
    case FlatMap(function: String, xs: Combinator[_]) => buildJson(root, "FlatMap", "function: \n"+function, getExecutionPlan(xs))
    case Filter(p:String, xs: Combinator[_]) => buildJson(root, "Filter", "predicate: \n"+p, getExecutionPlan(xs))
    case EquiJoin(keyx:String, keyy:String, function:String, xs: Combinator[_], ys: Combinator[_]) => buildJson(root, "EquiJoin", "keyX: \n"+keyx+"\nkeyY: \n"+keyy+"\nfunction: \n"+function, getExecutionPlan(xs)+", "+getExecutionPlan(ys))
    case Cross(function:String, xs: Combinator[_], ys: Combinator[_]) => buildJson(root, "Cross", "function: \n"+function, getExecutionPlan(xs)+", "+getExecutionPlan(ys))
    case Group(key:String, xs: Combinator[_]) => buildJson(root, "Group", "key: \n"+key, getExecutionPlan(xs))
    case Fold(empty: String, sng: String, union: String, xs: Combinator[_]) => buildJson(root, "Fold", "empty: \n"+empty+"\nsingle: \n"+sng+"\nunion: \n"+union, getExecutionPlan(xs))
    case FoldGroup(key: String, empty: String, sng: String, union: String, xs: Combinator[_]) => buildJson(root, "FoldGroup", "key: \n"+key+"\nempty: \n"+empty+"\nsingle: \n"+sng+"\nunion: \n"+union, getExecutionPlan(xs))
    case Distinct(xs: Combinator[_]) => buildJson(root, "Distinct", "", getExecutionPlan(xs))
    case Union(xs: Combinator[_], ys: Combinator[_]) => buildJson(root, "Union", "", getExecutionPlan(xs)+", "+getExecutionPlan(ys))
    case Diff(xs: Combinator[_], ys: Combinator[_]) => buildJson(root, "Diff", "", getExecutionPlan(xs)+", "+getExecutionPlan(ys))
    case Scatter(xs: Combinator[_]) => buildJson(root, "Scatter", "", getExecutionPlan(xs))
    case StatefulCreate(xs: Combinator[_]) => buildJson(root, "StatefulCreate", "", getExecutionPlan(xs))
    case _ => {
      System.err.println("Unsupported class: "+root.getClass.getName)
      s"[${root.getClass.getName}}]"
    }
  }

  def buildJson(root:Combinator[_], name: String, toolTipText: String, parents: String): String = {
    val text = Literal(Constant(toolTipText)).toString()
    val json = name match {
      case "Read" => s""""label": "$name", "type":"INPUT", "tooltip":$text"""
      case "Write" => s""""label": "$name", "type":"INPUT", "tooltip":$text"""
      case "TempSource" => {
        val t = root.asInstanceOf[TempSource[_]]
        if (t.ref != null)
          s""""label": "TempSource (${t.ref.asInstanceOf[ParallelizedDataBag[_, _]].name})", "tooltip":$text"""
        else
          s""""label":"TempSource", "tooltip":$text"""
      }
      case _ => s""""label":"$name", "tooltip":$text"""
    }

    s"""{"id":"${System.identityHashCode(root)}", $json, "parents":[$parents]}"""
  }
}
