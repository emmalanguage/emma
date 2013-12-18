package eu.stratosphere.emma

//TODO: as abstract class
class OutputFormat[T] {

  def write(t: T): Boolean = ???
}