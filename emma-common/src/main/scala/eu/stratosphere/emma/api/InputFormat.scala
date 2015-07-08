package eu.stratosphere.emma.api

import java.io.{InputStream, InputStreamReader}

import au.com.bytecode.opencsv.CSVReader

abstract class InputFormat[T] {
  def read(is: InputStream): Seq[T]
}

class CSVInputFormat[T: CSVConvertors](val separator: Char) extends InputFormat[T] {

  val convert = implicitly[CSVConvertors[T]]

  def this() = this('\t')

  override def read(is: InputStream): Seq[T] = {
    val reader = new CSVReader(new InputStreamReader(is), separator)

    val sb = Seq.newBuilder[T]
    var obj = reader.readNext()
    while (obj != null) {
      sb += convert.fromCSV(obj)
      obj = reader.readNext()
    }

    reader.close()
    sb.result()
  }
}