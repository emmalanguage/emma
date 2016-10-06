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
package util

import scala.tools.reflect.ToolBox

private[emmalanguage] object Toolbox {

  import java.nio.file.{Files, Paths}

  final private lazy val codeGenDirDefault = Paths
    .get(sys.props("java.io.tmpdir"), "emma", "codegen")
    .toAbsolutePath.toString

  /** The directory where the toolbox will store runtime-generated code. */
  final private lazy val codeGenDir = {
    val path = Paths.get(sys.props.getOrElse("emma.codegen.dir", codeGenDirDefault))
    // Make sure that generated class directory exists
    Files.createDirectories(path)
    path.toAbsolutePath.toString
  }

  final val mirror = scala.reflect.runtime.currentMirror

  final val universe = mirror.universe

  final lazy val toolbox = mirror.mkToolBox(options = s"-d $codeGenDir")

}
