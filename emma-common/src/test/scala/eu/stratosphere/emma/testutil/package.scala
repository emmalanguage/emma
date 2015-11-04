package eu.stratosphere.emma

import java.io.InputStream
import java.nio.file.{Files, Paths, StandardCopyOption}

import eu.stratosphere.emma.api.Algorithm
import org.apache.commons.io.FilenameUtils

package object testutil {

  /**
   * Copies the resource located at the given path to the emma temp folder.
   *
   * @param resourcePath The resource to be copied
   * @return The path to the materialized version of the resource.
   */
  def materializeResource(resourcePath: String) = {
    val is = Option[InputStream](getClass.getResourceAsStream(resourcePath))

    if (is.isEmpty) throw new RuntimeException(s"Cannot find resource at path '$resourcePath'")

    val outputPath = Paths.get(s"${System.getProperty("java.io.tmpdir")}/emma/$resourcePath")
    Files.createDirectories(outputPath.getParent)
    Files.copy(is.get, outputPath, StandardCopyOption.REPLACE_EXISTING)
    outputPath.toString
  }

  /**
   * Creates a demp output path with the given `suffix`.
   */
  def tempPath(suffix: String) = FilenameUtils.separatorsToUnix(Paths.get(s"${System.getProperty("java.io.tmpdir")}/emma/$suffix").toString)

  /**
   * Reads The contents from file (or folder containing a list of files) as a string.
   * @param path The path of the file (or folder containing a list of files) to be read.
   * @return A sorted list of the read contents.
   */
  def fromPath(path: String): List[String] = fromPath(new java.io.File(path))

  /**
   * Reads The contents from file (or folder containing a list of files) as a string.
   * @param path The path of the file (or folder containing a list of files) to be read.
   * @return A sorted list of the read contents.
   */
  def fromPath(path: java.io.File): List[String] = {
    val entries = if (path.isDirectory)
      path.listFiles.filter(x => !(x.getName.startsWith(".") || x.getName.startsWith("_")))
    else
      Array(path)
    (entries flatMap (x => scala.io.Source.fromFile(x.getAbsolutePath).getLines().toStream.toList)).toList.sorted
  }

  /**
   * Compares the contents of two bags.
   *
   * @param exp The bag containing the expected contents.
   * @param act The bag containing the actual contents.
   * @tparam T The type of the bag's elements.
   */
  def compareBags[T](exp: Seq[T], act: Seq[T]) = {
    assert((exp diff act) == Seq.empty[String], s"Unexpected elements in result: $exp")
    assert((act diff exp) == Seq.empty[String], s"Unseen elements in result: $act")
  }

  def deleteRecursive(path: java.io.File): Boolean = {
    val ret = if (path.isDirectory) {
      path.listFiles().toSeq.foldLeft(true)((r, f) => deleteRecursive(f))
    } else { /* regular file */
      true
    }
    ret && path.delete()
  }

  /**
   * Execution wrapper for a piece of code that depends on a runtime object.
   * Closes the runtime session after the code is executed.
   *
   * @param rt The runtime to use.
   * @param f  A lambda that depends on the initialized runtime and returns a value of type T.
   * @tparam T The type of the returned value.
   * @return The result of `f` applied to the initialized `rt`.
   */
  def withRuntime[T](rt: runtime.Engine = runtime.default())(f: runtime.Engine => T): T =
    try f(rt) finally rt.closeSession()

  /** Syntax sugar for testing [[Algorithm]]s. */
  implicit class AlgorithmVerification[A](val alg: Algorithm[A]) extends AnyVal {

    /**
      * Run this [[Algorithm]] on the provided `engine` and on the native runtime and ensure that
      * the results match.
      *
      * @param engine The runtime to test.
      */
    def verifyWith(engine: runtime.Engine): Unit =
      withRuntime(engine) { _ =>
        withRuntime(runtime.Native()) { native =>
          // compute the algorithm using the original code and the runtime under test
          val actual = alg.run(engine)
          val expected = alg.run(native)
          // assert that the result contains the expected values
          assert(actual == expected, s"""
            |actual != expected
            |actual: $actual
            |expected: $expected
            |""".stripMargin)
        }
      }
  }
}
