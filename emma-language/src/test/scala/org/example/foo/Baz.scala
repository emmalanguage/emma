package org.example.foo

class Baz[T:Numeric](val x: T)

object Baz {
  def apply[T: Numeric](x: T) = {
    val n = implicitly[Numeric[T]]
    new Baz(n.toInt(x))
  }
}
