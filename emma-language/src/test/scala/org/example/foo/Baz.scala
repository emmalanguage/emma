package org.example.foo

class Baz(val x: Int)

object Baz {
  def apply(x: Int): Baz = new Baz(x)
}
