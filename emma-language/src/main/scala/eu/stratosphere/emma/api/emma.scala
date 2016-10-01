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
package eu.stratosphere.emma.api

import eu.stratosphere.emma.macros.program.WorkflowMacros
import org.emmalanguage.macros.utility.UtilMacros

import scala.language.experimental.macros

// TODO: Add more detailed documentation with examples.
object emma {

  // -----------------------------------------------------
  // program macros
  // -----------------------------------------------------

  final def parallelize[T](e: T): Algorithm[T] =
    macro WorkflowMacros.parallelize[T]

  final def comprehend[T](e: T): Unit =
    macro WorkflowMacros.comprehend[T]

  final def visualize[T](e: T): T =
    macro UtilMacros.visualize[T]

  final def prettyPrint[T](e: T): String =
    macro UtilMacros.prettyPrint[T]
}
