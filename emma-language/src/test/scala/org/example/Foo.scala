package org.example

object Foo {

  object Bar {

    class Baz(val x: Int)

    object Baz {
      def apply(x: Int): Baz = new Baz(x)
    }

  }

}
