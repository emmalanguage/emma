#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}
package algorithms.text

import org.emmalanguage.api._
import org.emmalanguage.test.util._
import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import java.io.File

trait BaseWordCountIntegrationSpec extends FlatSpec with Matchers with BeforeAndAfter {

  val codegenDir = tempPath("codegen")
  val dir = "/text"
  val path = tempPath(dir)

  before {
    new File(codegenDir).mkdirs()
    new File(path).mkdirs()
    addToClasspath(new File(codegenDir))
    materializeResource(s"${symbol_dollar}dir/jabberwocky.txt")
  }

  after {
    deleteRecursive(new File(codegenDir))
    deleteRecursive(new File(path))
  }

  it should "count words" in {
    wordCount(s"${symbol_dollar}path/jabberwocky.txt", s"${symbol_dollar}path/wordcount-output.txt", CSV())

    val act = DataBag(fromPath(s"${symbol_dollar}path/wordcount-output.txt"))
    val exp = DataBag({
      val words = for {
        line <- fromPath(s"${symbol_dollar}path/jabberwocky.txt")
        word <- line.toLowerCase.split("${symbol_escape}${symbol_escape}W+")
        if word != ""
      } yield word

      for {
        (word, occs) <- words.groupBy(x => x).toSeq
      } yield s"${symbol_dollar}word${symbol_escape}t${symbol_dollar}{occs.length}"
    })

    act.collect() should contain theSameElementsAs exp.collect()
  }

  def wordCount(input: String, output: String, csv: CSV): Unit
}
