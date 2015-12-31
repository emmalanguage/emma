package org.emma.server

import com.google.gson.Gson

import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.runtime.Engine

import java.lang.reflect.Constructor
import java.util.concurrent.TimeUnit
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import net.sourceforge.argparse4j.inf.Namespace

import org.emma.data.code.ExampleFileLoader
import org.emma.data.plan.{Graph, Plan}
import org.emma.runtime._

class PlanServlet extends HttpServlet {
  var runner: FlinkExampleRunner = _
  val plugin = new GuiPlugin
  val loader = new ExampleFileLoader

  protected override def doGet(req: HttpServletRequest, resp: HttpServletResponse) {
    val action = req.getPathInfo.tail
    action match {
      case "run" =>
        plugin.resume()
        respond(resp)

      case "initRuntime" | "loadGraph" =>
        val name: String = req.getParameter("name")
        if (name != null && name.nonEmpty) {
          runExample(name)
          respond(resp)
        }

      case _ =>
        resp.setStatus(HttpServletResponse.SC_BAD_REQUEST)
        println(s"Unsupported action: '$action'")
    }
  }

  private def respond(resp: HttpServletResponse) = {
    val json = new java.util.HashMap[String, Any]
    for (plan <- waitForPlan()) {
      json.put("graph", new Graph(plan))
      json.put("comprehensions", plan.getComprehensions)
    }

    json.put("isLast", !runner.isAlive)
    resp.setHeader("Content-Type", "application/json")
    resp.setStatus(HttpServletResponse.SC_OK)
    resp.getWriter.println(new Gson().toJson(json))
  }

  private def runExample(name: String) = {
    val constructor = findConstructorOf(name)
    val parameters = loader.getParameters(name)
    if (runner != null && runner.isAlive) runner.interrupt()
    runner = new FlinkExampleRunner(constructor, parameters, plugin :: Nil)
    runner.start()
  }

  private def waitForPlan() = {
    var plan: Plan = null
    if (runner != null) while (runner.isAlive && plan == null)
      // TODO: Replace with `.take()` and use sentinels to mark completion
      plan = plugin.executionPlanQueue.poll(500, TimeUnit.MILLISECONDS)

    Option(plan)
  }

  private def findConstructorOf(algorithm: String) = {
    val clazz = Class.forName(algorithm)
    if (!clazz.getClass.isInstance(classOf[Algorithm]))
      throw new Exception(s"Unsupported class type '$clazz'")

    clazz.getDeclaredConstructors.find { constructor =>
      val parameters = constructor.getParameterTypes
      parameters.length == 2 &&
        parameters(0).getClass.isInstance(classOf[Namespace]) &&
        parameters(1).getClass.isInstance(classOf[Engine])
    }.getOrElse {
      throw new Exception(s"Cannot find fitting constructor for '$algorithm'.")
    }.asInstanceOf[Constructor[Algorithm]]
  }
}
