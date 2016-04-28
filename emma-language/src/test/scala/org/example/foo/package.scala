package org.example

package object foo {

  def bar: Boolean = true

  class Bar(x: Int)

  object Bar {

    class Baz(val x: Int)

    object Baz {
      def apply(x: Int): Baz = new Baz(x)
    }

    def apply(x: Int): Baz = new Baz(x)
  }

}
