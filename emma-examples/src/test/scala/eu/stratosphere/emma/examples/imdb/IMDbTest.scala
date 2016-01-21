package eu.stratosphere.emma.examples.imdb

import eu.stratosphere.emma.testutil._

import java.io.File

import org.junit.experimental.categories.Category
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@Category(Array(classOf[ExampleTest]))
@RunWith(classOf[JUnitRunner])
class IMDbTest extends FunSuite with Matchers with BeforeAndAfter {
  // default parameters
  val dir = "/cinema"
  val path = tempPath(dir)

  before {
    new File(s"$path/output").mkdirs()
    materializeResource(s"$dir/imdb.csv")
    materializeResource(s"$dir/berlinalewinners.csv")
    materializeResource(s"$dir/canneswinners.csv")
  }

  after {
    deleteRecursive(new File(path))
  }

  test("IMDb") (withRuntime() { rt =>
    new IMDb(path, s"$path/output", rt).run()
  })
}
