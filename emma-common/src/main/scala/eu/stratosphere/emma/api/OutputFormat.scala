package eu.stratosphere.emma.api

import java.io.{OutputStream, OutputStreamWriter}

import au.com.bytecode.opencsv.CSVWriter

abstract class OutputFormat[T] {
  def write(in: DataBag[T], os: OutputStream): Unit
}

class CSVOutputFormat[T: CSVConverters](val separator: Char) extends OutputFormat[T] {

  val convert = implicitly[CSVConverters[T]]

  def this() = this('\t')

  override def write(in: DataBag[T], os: OutputStream): Unit = {
    val writer = new CSVWriter(new OutputStreamWriter(os), separator, CSVWriter.NO_QUOTE_CHARACTER)

    in.fold()(x => writer.writeNext(convert.toCSV(x, separator)), (_, _) => ())

    writer.close()
  }
}