package eu.stratosphere
package emma.api

/** Serialization format for encoding to / decoding from CSV records. */
trait CSVConverters[T] {

  /** Parses and returns the value encoded in `record`. */
  def fromCSV(record: Array[String]): T

  /** Serializes `value` using `sep` as field delimiter. */
  def toCSV(value: T, sep: Char): Array[String]
}

object CSVConverters {

  def apply[T](from: Array[String] => T)
    (to: (T, Char) => Array[String]): CSVConverters[T] = {

    new CSVConverters[T] {

      override def fromCSV(record: Array[String]): T =
        from(record)

      override def toCSV(value: T, sep: Char): Array[String] =
        to(value, sep)
    }
  }
}
