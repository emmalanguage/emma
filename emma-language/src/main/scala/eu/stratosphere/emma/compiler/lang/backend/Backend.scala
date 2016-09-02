package eu.stratosphere.emma
package compiler.lang.backend

import compiler.Common
import compiler.lang.core.Core

/** Backend-related (but backend-agnostic) transformations. */
trait Backend extends Common
  with Order {
  this: Core =>

  import UniverseImplicits._

  object Backend {

    /** Delegates to [[Order.disambiguate]]. */
    lazy val order = Order.disambiguate

  }
}
