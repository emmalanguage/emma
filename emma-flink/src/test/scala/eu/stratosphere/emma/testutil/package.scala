package eu.stratosphere.emma

import java.io.InputStream
import java.nio.file.{Files, Paths, StandardCopyOption}

package object testutil {

  def materializeResource(resourcePath: String) = {
    val is = Option[InputStream](getClass.getResourceAsStream(resourcePath))

    if (is.isEmpty) throw new RuntimeException(s"Cannot find resource at path '$resourcePath'")

    val outputPath = Paths.get(s"${System.getProperty("java.io.tmpdir")}/emma/$resourcePath")
    Files.createDirectories(outputPath.getParent)
    Files.copy(is.get, outputPath, StandardCopyOption.REPLACE_EXISTING)
    outputPath.toString
  }

  def tempOutputPath(suffix: String) = Paths.get(s"${System.getProperty("java.io.tmpdir")}/emma/$suffix").toString
}
