package eu.stratosphere.emma.api

trait CSVConverters[T] {
  def fromCSV(value: Array[String]): T
  def toCSV(values: T, separator: Char): Array[String]
}
