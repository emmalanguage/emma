#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}
package algorithms.graphs

import algorithms.graphs.model.Edge

import org.emmalanguage.api._
import org.emmalanguage.test.util._
import org.emmalanguage.util
import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import resource._

import java.io.File
import java.io.PrintWriter

trait BaseTransitiveClosureIntegrationSpec extends FlatSpec with Matchers with BeforeAndAfter {

  val codegenDir = tempPath("codegen")
  val dir = "/graphs/trans-closure"
  val path = tempPath(dir)

  before {
    new File(codegenDir).mkdirs()
    new File(path).mkdirs()
    addToClasspath(new File(codegenDir))
    generateInput(s"${symbol_dollar}path/edges.tsv")
  }

  after {
    deleteRecursive(new File(codegenDir))
    deleteRecursive(new File(path))
  }

  it should "compute the transitive closure of a directed graph" in {
    val act = transitiveClosure(s"${symbol_dollar}path/edges.tsv", CSV())
    val exp = expectedClosure()

    act should contain theSameElementsAs exp
  }

  def transitiveClosure(input: String, csv: CSV): Set[Edge[Long]]

  lazy val paths = {
    val S = 3415434314L
    val P = 5

    val ws = shuffle(P)(util.RanHash(S, 0)).map(_.toLong)
    val xs = shuffle(P)(util.RanHash(S, 1)).map(_.toLong + P)
    val ys = shuffle(P)(util.RanHash(S, 2)).map(_.toLong + P * 2)
    val zs = shuffle(P)(util.RanHash(S, 3)).map(_.toLong + P * 3)

    ws zip xs zip ys zip zs
  }

  private def generateInput(path: String): Unit = {
    val edges = {
      for {
        (((w, x), y), z) <- paths
        e <- Seq(Edge(w, x), Edge(x, y), Edge(y, z))
      } yield e
    }.distinct

    for (pw <- managed(new PrintWriter(new File(path))))
      yield for (e <- edges.sortBy(_.src)) pw.write(s"${symbol_dollar}{e.src}${symbol_escape}t${symbol_dollar}{e.dst}${symbol_escape}n")
  }.acquireAndGet(_ => ())

  private def expectedClosure(): Set[Edge[Long]] = {
    for {
      (((w, x), y), z) <- paths
      e <- Seq(Edge(w, x), Edge(x, y), Edge(y, z), Edge(w, y), Edge(x, z), Edge(w, z))
    } yield e
  }.toSet

  private def shuffle(n: Int)(r: util.RanHash): Array[Int] = {
    val xs = (1 to n).toArray
    for {
      i <- 0 to (n - 2)
    } {
      val j = r.nextInt(i + 1)
      val t = xs(i)
      xs(i) = xs(j)
      xs(j) = t
    }
    xs
  }
}
