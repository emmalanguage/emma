package eu.stratosphere.emma.api

//TODO: as abstract class
class InputFormat[T] {

  def read(bytes: Seq[Byte]): Seq[T] = ???

  def split(url: String, numOfSplits: Int): List[Seq[Byte]] = ???
}