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
package compiler.tools

import compiler.Common
import compiler.lang.cf.ControlFlow
import compiler.lang.core.Core

import resource._

import java.io.FileWriter
import java.nio.file.Path

trait GraphTools extends Common with ControlFlow {
  self: Core =>

  import UniverseImplicits._

  object CytoscapeGraphJsonProtocol extends spray.json.DefaultJsonProtocol {
    import spray.json._

    case class NodeData(id: String, label: String, parent: Option[String])
    case class EdgeData(id: String, source: String, target: String, label: String)

    sealed trait Element

    case class NodeElement
    (
      data: NodeData,
      group: String = "nodes",
      style: Option[Map[String, String]] = None
    ) extends Element

    case class EdgeElement
    (
      data: EdgeData,
      group: String = "edges",
      style: Option[Map[String, String]] = None
    ) extends Element

    implicit val nodeDataFormat = jsonFormat3(NodeData)
    implicit val edgeDataFormat = jsonFormat4(EdgeData)
    implicit val nodeElementFormat = jsonFormat3(NodeElement)
    implicit val edgeElementFormat = jsonFormat3(EdgeElement)

    implicit object ElementJsonFormat extends RootJsonFormat[Element] {
      def write(c: Element) = c match {
        case n: NodeElement => n.toJson
        case e: EdgeElement => e.toJson
      }

      def read(value: JsValue) = {
        value.asJsObject.getFields("group") match {
          case Seq(JsString(group)) if group == "nodes" => value.convertTo[NodeElement]
          case Seq(JsString(group)) if group == "edges" => value.convertTo[EdgeElement]
          case x => throw new IllegalArgumentException(s"Unable to convert to `${classOf[Element].getSimpleName}`: $x")
        }
      }
    }
  }

  object GraphTools {

    import Core.{Lang => core}
    import CytoscapeGraphJsonProtocol._
    import spray.json._

    val cs = Comprehension.Syntax(API.DataBag.sym)

    def mkGraph(tree: u.Tree): Iterable[Element] = {
      val graph = ControlFlow.cfg(tree)
      val mkLabel = label(graph) _

      val nestFlipped = (for {
        parent <- graph.nest.nodes
        child <- graph.nest.successors(parent)
      } yield child -> parent).toMap

      val nodes = for {
        n <- graph.uses.keys
      } yield NodeElement(
        data = NodeData(
          id = n.name.toString,
          label = mkLabel(n),
          parent = nestFlipped.get(n).map(_.name.toString)
        )
      )

      val edges = for {
        from <- graph.data.nodes
        to <- graph.data.predecessors(from)
      } yield {
        val source = from.name.toString
        val target = to.name.toString
        val label = to.name.toString
        EdgeElement(
          data = EdgeData(
            id = s"${source}_to_$target",
            source = source,
            target = target,
            label = label
          )
        )
      }
      nodes ++ edges
    }

    private[emmalanguage] def mkJsonGraph(id: String, tree: u.Tree): JsValue =
      JsObject(id -> mkGraph(tree).toJson)

    private[emmalanguage] def writeJsonGraph(basePath: Path, id: String, tree: u.Tree): u.Tree = {
      basePath.toFile.mkdirs() // make parent
      val targetPath: Path = basePath.resolve(s"$id.json")
      for(pw <- managed(new FileWriter(targetPath.toAbsolutePath.toString))) {
        pw.write(mkJsonGraph(id, tree).toString())
      }
      tree
    }

    /**
     * Renders a tree in Cytoscape graph json format.
     *
     * The method returns the identical tree while writing the graph as an effect.
     *
     * @param basePath the base path of the file
     * @param name the filename
     * @return a function returning the identity of the tree
     */
    def renderGraph(basePath: Path)(name: String): u.Tree => u.Tree =
      writeJsonGraph(basePath, name, _)

    private[emmalanguage] def label(graph: FlowGraph[u.TermSymbol])(sym: u.TermSymbol): String =
      graph.data.label(sym).map(_.rhs) match {
        case Some(core.Atomic(x)) => u.showCode(x)
        case Some(core.DefCall(_, method, _, _)) => method.name.decodedName.toString
        case Some(core.Lambda(_, _, _)) => "Lambda"
        case Some(cs.Comprehension(_, _)) => "For-Comprehension"
        case Some(_) => s"[Unknown Symbol: ${sym.name.decodedName}]"
        case None => s"Closure (${sym.name.decodedName})"
      }
  }
}
