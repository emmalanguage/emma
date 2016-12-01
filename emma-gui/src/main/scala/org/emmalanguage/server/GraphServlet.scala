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
package server

import org.apache.commons.io.FilenameUtils

import java.io.File
import java.nio.file.Path
import java.nio.file.Files
import java.nio.file.Paths
import javax.servlet.http._

class GraphServlet(path: Path) extends HttpServlet {
  protected override def doGet(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    import HttpServletResponse.SC_OK
    req.getRequestURI.split('/').last match {
      case "graph-json" =>
        val directory = new File(path.toFile.toString)
        val contents = directory
          .listFiles()
          .filter(file => file.isFile && FilenameUtils.getExtension(file.getName) == "json")
          .map(f => Files.readAllBytes(Paths.get(f.getAbsolutePath)))
          .map(bytes => new String(bytes)).mkString(",")
        resp.setStatus(SC_OK)
        resp.setHeader("Content-Type", "application/json")
        resp.getWriter.println(s"[$contents]")
      case _ =>
        resp.setStatus(SC_OK)
        req.getRequestDispatcher("/cytoscape-template.html").forward(req, resp)
    }
  }
}
