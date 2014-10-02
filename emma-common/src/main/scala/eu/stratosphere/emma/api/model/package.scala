package eu.stratosphere.emma.api

import scala.annotation.StaticAnnotation

package object model {

  import scala.language.experimental.macros

  // -----------------------------------------------------
  // traits
  // -----------------------------------------------------

  trait Identity[K] {
    def identity: K
  }

  // -----------------------------------------------------
  // annotations
  // -----------------------------------------------------

  final class id extends StaticAnnotation {}
}
