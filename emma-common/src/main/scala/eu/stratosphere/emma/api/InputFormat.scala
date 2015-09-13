package eu.stratosphere.emma.api

import java.io.{InputStream, InputStreamReader}

import au.com.bytecode.opencsv.CSVReader

import scala.io.Source

abstract class InputFormat[T] {
  def read(is: InputStream): Seq[T]
}

class CSVInputFormat[T: CSVConverters](val separator: Char) extends InputFormat[T] {

  val convert = implicitly[CSVConverters[T]]

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

class TextInputFormat[T <: String](val separator: Char) extends InputFormat[T] {

  override def read(is: InputStream): Seq[T] = {
    val reader = Source.fromInputStream(is)

    val sb = Seq.newBuilder[T]
    for(line <-  reader.getLines()){
      sb += line.asInstanceOf[T]
    }

    reader.close()
    sb.result()
  }
}
