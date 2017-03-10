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
package compiler.lang.backend

import compiler.Common
import compiler.lang.core.Core

/** Backend-related (but backend-agnostic) transformations. */
trait Backend extends Common
  with Caching
  with Order
  with Specialization {
  this: Core =>

  import UniverseImplicits._

  object Backend {

    /** Delegates to [[Specialization.specialize]]. */
    def specialize(backend: BackendAPI) =
      Specialization.specialize(backend)

    /** Delegates to [[Caching.addCacheCalls]]. */
    lazy val addCacheCalls = Caching.addCacheCalls
  }
}
