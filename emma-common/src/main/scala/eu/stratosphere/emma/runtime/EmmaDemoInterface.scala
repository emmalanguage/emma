package eu.stratosphere.emma.runtime

import java.util.concurrent.CountDownLatch

import eu.stratosphere.emma.api.ParallelizedDataBag
import eu.stratosphere.emma.ir._

import scala.collection.mutable

trait EmmaDemoInterface {
  var blockingLatch:CountDownLatch = null
  var executionPlanJson: mutable.Stack[Plan] = new mutable.Stack[Plan]()

  def setBlockingLatch(blocker: CountDownLatch) {
    blockingLatch = blocker
  }

  def getBlockingLatch():CountDownLatch = blockingLatch

  def getExecutionPlanJson: mutable.Stack[Plan] = executionPlanJson

  def getExecutionPlan(root:Combinator[_]) : String = root match {
    case Read(location: String, _) => buildJson(root, "Read", "")
    case Write(location: String, _, xs: Combinator[_]) => buildJson(root, "Write", getExecutionPlan(xs))
    case TempSource(ref) => buildJson(root, "TempSource", "")
    case TempSink(name: String, xs: Combinator[_]) => buildJson(root, "TempSink ("+name+")", getExecutionPlan(xs))
    case Map(_, xs: Combinator[_]) => buildJson(root, "Map", getExecutionPlan(xs))
    case FlatMap(_, xs: Combinator[_]) => buildJson(root, "FlatMap", getExecutionPlan(xs))
    case Filter(_, xs: Combinator[_]) => buildJson(root, "Filter", getExecutionPlan(xs))
    case EquiJoin(_, _, _, xs: Combinator[_], ys: Combinator[_]) => buildJson(root, "EquiJoin", getExecutionPlan(xs)+", "+getExecutionPlan(ys))
    case Cross(_, xs: Combinator[_], ys: Combinator[_]) => buildJson(root, "Cross", getExecutionPlan(xs)+", "+getExecutionPlan(ys))
    case Group(_, xs: Combinator[_]) => buildJson(root, "Group", getExecutionPlan(xs))
    case Fold(_, _, _, xs: Combinator[_]) => buildJson(root, "Fold", getExecutionPlan(xs))
    case FoldGroup(_, _, _, _, xs: Combinator[_]) => buildJson(root, "FoldGroup", getExecutionPlan(xs))
    case Distinct(xs: Combinator[_]) => buildJson(root, "Distinct", getExecutionPlan(xs))
    case Union(xs: Combinator[_], ys: Combinator[_]) => buildJson(root, "Union", getExecutionPlan(xs)+", "+getExecutionPlan(ys))
    case Diff(xs: Combinator[_], ys: Combinator[_]) => buildJson(root, "Diff", getExecutionPlan(xs)+", "+getExecutionPlan(ys))
    case Scatter(xs: Combinator[_]) => buildJson(root, "Scatter", getExecutionPlan(xs))
    case _ => {
      System.err.println("Unsupported class: "+root.getClass.getName)
      s"[${root.getClass.getName}}]"
    }
  }

  def buildJson(root:Combinator[_], name: String, parents: String): String = {
    val json = name match {
      case "Read" => s"""\"label\": \"$name\", type:\"INPUT\", location:\"${root.asInstanceOf[Read[_]].location}\""""
      case "Write" => s"""\"label\": \"$name\", type:\"INPUT\", location:\"${root.asInstanceOf[Write[_]].location}\""""
      case "TempSource" => {
        val t = root.asInstanceOf[TempSource[_]]
        if (t.ref != null)
          s"""\"label\": \"TempSource (${t.ref.asInstanceOf[ParallelizedDataBag[_, _]].name})\""""
        else
          s"""\"label\":\"TempSource\""""
      }
      case _ => s"""\"label\":\"$name\""""
    }

    s"""{\"id\":\"${System.identityHashCode(root)}\", $json, \"parents\":[$parents]}"""
  }

  /**
   * Constructs only an execution plan if a blocking latch is set in the runtime. If no latch is defined,
   * no action is performed. If a latch is defined, a JSON object is created and the execution is blocked until
   * the latch is released by the runtime.
   * @param name of the plan
   * @param root execution graph root
   */
  def constructExecutionPlanJson(name:String, root:Combinator[_]) = {
    if (blockingLatch != null) {
      executionPlanJson.push(new Plan(name, getExecutionPlan(root)))
      blockingLatch.await()
    }
  }
}

case class Plan(name: String, plan: String)