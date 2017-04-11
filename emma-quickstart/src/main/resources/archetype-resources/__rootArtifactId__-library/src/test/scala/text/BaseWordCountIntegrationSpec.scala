#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}
package text

import org.emmalanguage.api._
import org.emmalanguage.io.csv.CSV
import org.emmalanguage.test.util._
import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

trait BaseWordCountIntegrationSpec extends FlatSpec with Matchers with BeforeAndAfter {

  val codegenDir = tempPath("codegen")
  val dir = "/text/"
  val path = tempPath(dir)
  val text = "To be or not to Be"

  before {
    new File(codegenDir).mkdirs()
    new File(path).mkdirs()
    addToClasspath(new File(codegenDir))
    Files.write(Paths.get(s"${symbol_dollar}path/hamlet.txt"), text.getBytes(StandardCharsets.UTF_8))
  }

  after {
    deleteRecursive(new File(codegenDir))
    deleteRecursive(new File(path))
  }

  "WordCount" should "count words" in {
    wordCount(s"${symbol_dollar}path/hamlet.txt", s"${symbol_dollar}path/output.txt", CSV())

    val act = DataBag(fromPath(s"${symbol_dollar}path/output.txt"))
    val exp = DataBag(text.toLowerCase.split("${symbol_escape}${symbol_escape}W+").groupBy(x => x).toSeq.map(x => s"${symbol_dollar}{x._1}${symbol_escape}t${symbol_dollar}{x._2.length}"))

    compareBags(act.fetch(), exp.fetch())
  }

  def wordCount(input: String, output: String, csv: CSV): Unit
}
