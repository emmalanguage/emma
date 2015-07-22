package eu.stratosphere.emma.examples.text

import eu.stratosphere.emma.api.DataBag
import eu.stratosphere.emma.runtime
import eu.stratosphere.emma.testutil._

import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets

import java.io.File

import org.junit.runner.RunWith

import org.scalatest._
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class WordCountTest extends FunSuite with Matchers {
  // default parameters
  val dir        = "/text/"
  val path       = tempPath(dir)
  val rt         = runtime.default()
  val text       = "To be or not to Be"

  deleteRecursive(Paths.get(path).toFile)

  // initialize resources
  new File(path).mkdirs()
  Files.write(Paths.get(s"$path/hamlet.txt"), text.getBytes(StandardCharsets.UTF_8))

  test("Counts Words") {
    new WordCount(s"$path/hamlet.txt", s"$path/output.txt", rt).run()

    val actual = DataBag(fromPath(s"$path/output.txt"))
    val expected   = DataBag(text.toLowerCase.split("\\W+").groupBy(x => x).toSeq.map(x => s"${x._1}\t${x._2.size}"))

    compareBags(actual.fetch(), expected.fetch())
  }
}
