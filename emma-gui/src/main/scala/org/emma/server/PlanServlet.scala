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
package org.emma.server

import java.io.{BufferedReader, InputStreamReader}
import java.lang.reflect.Constructor
import java.util
import java.util.concurrent.TimeUnit
import java.util.regex.{Matcher, Pattern}
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import com.google.gson.Gson
import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.runtime.{Native, Spark, Flink, Engine}
import net.sourceforge.argparse4j.inf.Namespace
import org.emma.data.code.ExampleFileLoader
import org.emma.data.plan.{Graph, Plan}
import org.emma.runtime._

import scala.util.parsing.json.JSON

class PlanServlet extends HttpServlet {
  var runner: ExampleRunner = _
  val plugin = new GuiPlugin
  val loader = new ExampleFileLoader

  protected override def doGet(req: HttpServletRequest, resp: HttpServletResponse) : Unit = {
    val action = req.getPathInfo.tail
    action match {
      case "run" =>
        plugin.resume()
        respond(resp)

      case "loadGraph" =>
        val name: String = req.getParameter("name")
        val runtime: String = req.getParameter("runtime")
        if (name != null && name.nonEmpty) {
          if (runtime != null && runtime.nonEmpty) {
            runExample(name, None, runtime)
          } else {
            runExample(name)
          }

          respond(resp)
        }

      case _ =>
        resp.setStatus(HttpServletResponse.SC_BAD_REQUEST)
        println(s"Unsupported action: '$action'")
    }
  }

  /**
    * Receive POST request with input parameters for execution.
    */
  override def doPost(req: HttpServletRequest, resp: HttpServletResponse) : Unit = {
    val action = req.getPathInfo.tail
    val parameters = parseParametersFromBody(req)

    action match {
      case "initRuntime" =>
        val name: String = req.getParameter("name")
        val runtime: String = req.getParameter("runtime")
        if (name != null && name.nonEmpty) {
          if (runtime != null && runtime.nonEmpty) {
            runExample(name, parameters, runtime)
          } else {
            runExample(name, parameters)
          }
          respond(resp)
        }

      case _ =>
        resp.setStatus(HttpServletResponse.SC_BAD_REQUEST)
        println(s"Unsupported action: '$action'")
    }
  }

  private def respond(resp: HttpServletResponse, errorMessage:String = "") = {
    val json = new java.util.HashMap[String, Any]
    for (plan <- waitForPlan()) {
      json.put("graph", new Graph(plan))
      json.put("comprehensions", plan.getComprehensions)
    }

    json.put("isLast", !runner.isAlive)

    if (errorMessage.isEmpty) {
      json.put("error", false)
    } else {
      json.put("error", true)
      json.put("errorMessage", errorMessage)
    }

    resp.setHeader("Content-Type", "application/json")
    resp.setStatus(HttpServletResponse.SC_OK)
    resp.getWriter.println(new Gson().toJson(json))
  }

  private def runExample(name: String, params: Option[Namespace] = None, runtime:String = "Flink") = {
    val constructor = findConstructorOf(name)
    val parameters = params.getOrElse(loader.getParameters(name))

    if (runner != null && runner.isAlive) runner.interrupt()

    val engine = runtime match {
      case "Flink" => new Flink
      case "Spark" => new Spark
      case _ => new Native
    }

    runner = new ExampleRunner(constructor, parameters, plugin :: Nil, engine)
    runner.start()
  }

  private def waitForPlan() = {
    var plan: Plan = null
    if (runner != null) while (runner.isAlive && plan == null) {
      // TODO: Replace with `.take()` and use sentinels to mark completion
      plan = plugin.executionPlanQueue.poll(500, TimeUnit.MILLISECONDS)
    }

    Option(plan)
  }

  private def findConstructorOf(algorithm: String) = {
    val clazz = Class.forName(algorithm)
    if (!clazz.getClass.isInstance(classOf[Algorithm])) {
      throw new Exception(s"Unsupported class type '$clazz'")
    }

    clazz.getDeclaredConstructors.find { constructor =>
      val parameters = constructor.getParameterTypes
      parameters.length == 2 &&
        parameters(0).getClass.isInstance(classOf[Namespace]) &&
        parameters(1).getClass.isInstance(classOf[Engine])
    }.getOrElse {
      throw new Exception(s"Cannot find fitting constructor for '$algorithm'.")
    }.asInstanceOf[Constructor[Algorithm]]
  }

  def parseParametersFromBody(req:HttpServletRequest): Option[Namespace] = {
    val bufferedReader = new BufferedReader(new InputStreamReader(req.getInputStream))
    var line = ""

    var json = ""
    do {
      json += line
      line = bufferedReader.readLine()
    } while (line != null)

    class CC[T] { def unapply(a:Any):Option[T] = Some(a.asInstanceOf[T]) }
    object M extends CC[Map[String, Any]]

    var parameters : Namespace = new Namespace(new util.HashMap[String, AnyRef])

    for {
      Some(M(map)) <- List(JSON.parseFull(json)) if map.nonEmpty
    } yield {
      val attributes: util.HashMap[String, Any] = new util.HashMap[String, Any]()
      for (key <- map.keys) {
        val Some(value) = map.get(key)
        extractParameter(attributes, key, value.toString)
      }

      parameters = new Namespace(attributes.asInstanceOf[util.HashMap[String, AnyRef]])
    }

    if(parameters.getAttrs.isEmpty) None else Some(parameters)
  }

  private def extractParameter(attributes: util.HashMap[String, Any], key: String, value: String) : Unit = {
    var p: Pattern = Pattern.compile("^\\d+$")
    var m: Matcher = p.matcher(value)
    if (m.matches) {
      attributes.put(key, value.toInt)
      return
    }
    p = Pattern.compile("^\\d+\\.\\d+$")
    m = p.matcher(value)
    if (m.matches) {
      attributes.put(key, value.toDouble)
      return
    }
    p = Pattern.compile("true|false")
    m = p.matcher(value)
    if (m.matches) {
      attributes.put(key, value.toBoolean)
      return
    }
    attributes.put(key, value)
  }
}
