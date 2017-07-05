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
package compiler.lang.cf

import compiler.Common
import compiler.lang.core.Core

import quiver.Graph

/** Backend-related (but backend-agnostic) transformations. */
private[compiler] trait ControlFlow extends Common with CFG {
  self: Core =>

  case class FlowGraph[V]
  (
    uses: Map[V, Int],
    nest: Graph[V, Unit, Unit],
    ctrl: Graph[V, u.DefDef, Unit],
    data: Graph[V, u.ValDef, Unit]
  )

  object ControlFlow {

    /** Delegates to [[CFG.graph]]. */
    lazy val cfg: u.Tree => FlowGraph[u.TermSymbol] = CFG.graph
  }
}
