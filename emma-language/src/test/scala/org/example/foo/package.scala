package org.example

package object foo {

  def bar: Boolean = true

  class Bar[T](x: Int) {
    val y: T = null.asInstanceOf[T]
  }

  object Bar {

    class Baz(val x: Int)

    object Baz {
      def apply(x: Int): Baz = new Baz(x)
    }

    def apply(x: Int): Baz = new Baz(x)
  }

}
