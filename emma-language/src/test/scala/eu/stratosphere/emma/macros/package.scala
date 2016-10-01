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
package eu.stratosphere.emma

import eu.stratosphere.emma.api.Algorithm
import eu.stratosphere.emma.macros.program.comprehension.TestMacros

import java.io.File

import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox

package object macros {
  import scala.language.experimental.macros

  def parallelize[T](expr: T): Algorithm[T] = macro TestMacros.parallelize[T]
  def comprehend[T](expr: T): Unit = macro TestMacros.comprehend[T]
  def reComprehend[T](expr: T): Algorithm[T] = macro TestMacros.reComprehend[T]

  def mkToolBox(options: String = s"-cp $toolboxClasspath") =
    currentMirror.mkToolBox(options = options)

  def toolboxClasspath = {
    val file = new File(sys.props("emma.test.macro.toolboxcp"))
    if (!file.exists) sys.error(s"Output directory ${file.getAbsolutePath} does not exist")
    file.getAbsolutePath
  }
}
