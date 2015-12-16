package org.emma.server

import java.lang.reflect.Constructor
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import com.google.gson.Gson
import net.sourceforge.argparse4j.inf.Namespace
import org.emma.data.code.ExampleFileLoader
import org.emma.data.plan.{Graph, Plan}
import org.emma.runtime._

import scala.collection.mutable
import scala.util.control.Breaks._

class PlanServlet extends HttpServlet {
  var runner: ExampleRunner = null
  var localRuntime: LocalRuntime.type = null


  protected override def doGet(req: HttpServletRequest, resp: HttpServletResponse) {
    this.localRuntime = LocalRuntime.getInstance
    val action = req.getPathInfo.substring(1, req.getPathInfo.length)

    action match {
      case "run" =>
        localRuntime.nextStep()
        createResponse(resp)
      case "initRuntime" | "loadGraph" =>
        val name: String = req.getParameter("name")
        if (name != null && !name.isEmpty) {
          this.createNewExampleRunner(name)
          createResponse(resp)
        }
      case _ =>
        resp.setStatus(HttpServletResponse.SC_BAD_REQUEST)
        println("Unsupported action: '" + action + "'")
    }
  }

  def createResponse(resp: HttpServletResponse): Unit = {
    val map = new java.util.HashMap[String, Any]
    val plan = waitForPlan()
    if (plan != null) {
      map.put("graph", new Graph(plan))
      map.put("comprehensions", plan.getComprehensions)
    }
    map.put("isLast", !runner.isAlive)

    resp.setHeader("Content-Type", "application/json")
    resp.setStatus(HttpServletResponse.SC_OK)
    resp.getWriter.println((new Gson).toJson(map))
  }

  private def createNewExampleRunner(exampleName: String) = {
    val constructor: Constructor[_] = findConstructorWithNamespaceArgument(exampleName)
    if (runner != null) {
      runner.interrupt()
    }

    runner = new ExampleRunner(localRuntime.getPlanRuntime, getInputParameter(exampleName), constructor)
    runner.start()
  }

  private def waitForPlan(): Plan = {
    val executionPlan: mutable.Stack[Plan] = localRuntime.getRuntimePlugin.getExecutionPlanJson()
    var plan: Plan = null

    breakable {
      while (runner.isAlive) {
        try {
          Thread.sleep(1000)
          while (executionPlan.nonEmpty) {
            //TODO catch all plans if several are returned
            plan = executionPlan.pop()
          }
          if (plan != null)
            break()
        } catch {
          case e: InterruptedException => {
            e.printStackTrace()
          }
        }
      }
    }

    if (plan != null) {
      return plan
    }

    null
  }

  @throws(classOf[ClassNotFoundException])
  private def findConstructorWithNamespaceArgument(fullyQualifiedName: String): Constructor[_] = {
    val example: Class[_] = Class.forName(fullyQualifiedName)
    val constructors: Array[Constructor[_]] = example.getDeclaredConstructors
    var commandConstructor: Constructor[_] = null
    for (constructor <- constructors) {
      val parameterTypes: Array[Class[_]] = constructor.getParameterTypes
      breakable {
        for (parameterType <- parameterTypes) {
          if (parameterType.getClass.isInstance(classOf[Namespace]) && parameterTypes.length == 2) {
            commandConstructor = constructor
            break()
          }
        }
      }
    }
    if (commandConstructor == null) {
      throw new RuntimeException("Cannot find fitting constructor for '" + fullyQualifiedName + "'.")
    }
    commandConstructor
  }

  def getInputParameter(fullyQualifiedName: String): Namespace = {
    (new ExampleFileLoader).getParameters(fullyQualifiedName)
  }
}
