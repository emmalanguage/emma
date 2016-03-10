package eu.stratosphere.emma.streaming.api

case class Timed[+A](val t: Int, val v: A) {

  def map[B](f: (A) => B) = new Timed(t, f(v))

}
