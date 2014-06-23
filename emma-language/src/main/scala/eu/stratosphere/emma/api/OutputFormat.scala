package eu.stratosphere.emma.api

//TODO: as abstract class
class OutputFormat[T] {

  def write(t: T): Boolean = ???
}