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
package compiler

import com.typesafe.config.Config

trait LabyrinthCompiler
  extends LabyrinthNormalization
    with LabyrinthLabynization {

  override lazy val implicitTypes: Set[u.Type] = API.implicitTypes ++ //todo: like in FlinkCompiler.FlinkAPI
    Seq(api.Type[org.apache.flink.streaming.api.scala.StreamExecutionEnvironment])

  def transformations(implicit cfg: Config): Seq[TreeTransform] = Seq(
    // lifting
    Lib.expand,
    Core.lift,
    // optimizations
    Core.cse iff "emma.compiler.opt.cse" is true,
    Optimizations.foldFusion iff "emma.compiler.opt.fold-fusion" is true,
    // backend
    Comprehension.combine,
    Core.unnest,
    // labyrinth transformations
    labyrinthNormalize,
    Core.unnest,
    labyrinthLabynize,
    Core.unnest
  ) filterNot (_ == noop)

}