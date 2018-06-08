/*
 * Copyright © 2014 TU Berlin (emma@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.emmalanguage
package test

import org.apache.commons.io.FilenameUtils
import resource._

import scala.util.control.NonFatal

import java.io.File
import java.net.URL
import java.net.URLClassLoader
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardCopyOption

object util {

  def basePath =
    s"${System.getProperty("java.io.tmpdir")}/emma"

  /**
   * Copies the resource located at the given path to the emma temp folder.
   *
   * @param resourcePath The resource to be copied
   * @return The path to the materialized version of the resource.
   */
  def materializeResource(resourcePath: String) = (for {
    is <- managed(getClass.getResourceAsStream(resourcePath))
  } yield {
    val outputPath = Paths.get(s"$basePath/$resourcePath")
    Files.createDirectories(outputPath.getParent)
    Files.copy(is, outputPath, StandardCopyOption.REPLACE_EXISTING)

    outputPath.toString
  }).acquireAndGet(identity)

  /** Creates a temp output path with the given `suffix`. */
  def tempPath(suffix: String) = FilenameUtils
    .separatorsToUnix(Paths.get(s"$basePath/$suffix").toString)

  /**
   * Reads The contents from file (or folder containing a list of files) as a string.
   *
   * @param path The path of the file (or folder containing a list of files) to be read.
   * @return A sorted list of the read contents.
   */
  def fromPath(path: String): List[String] = fromPath(new java.io.File(path))

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

  /**
   * Reads The contents from file (or folder containing a list of files) as a string.
   *
   * @param path The path of the file (or folder containing a list of files) to be read.
   * @return A sorted list of the read contents.
   */
  def fromPath(path: java.io.File): List[String] = {
    val entries =
      if (path.isDirectory) path.listFiles.filter(x => !(x.getName.startsWith(".") || x.getName.startsWith("_")))
      else Array(path)
    (entries flatMap (x => scala.io.Source.fromFile(x.getAbsolutePath).getLines().toStream.toList)).toList.sorted
  }

  /** Deletes a file recursively. */
  def deleteRecursive(path: java.io.File): Boolean = {
    val ret = if (path.isDirectory) {
      path.listFiles().toSeq.foldLeft(true)((_, f) => deleteRecursive(f))
    } else /* regular file */ {
      true
    }
    ret && path.delete()
  }

  /** Adds a [[File]] to the classpath. */
  def addToClasspath(f: File): Unit =
    addToClasspath(f.toURI.toURL)

  /** Adds a [[URL]] to the classpath. */
  def addToClasspath(u: URL): Unit = {
    try {
      val clsldr = ClassLoader.getSystemClassLoader.asInstanceOf[URLClassLoader]
      val method = classOf[URLClassLoader].getDeclaredMethod("addURL", classOf[URL])
      method.setAccessible(true)
      method.invoke(clsldr, u)
    } catch {
      case NonFatal(t) =>
        throw new java.io.IOException("Error, could not add URL to system classloader", t)
    }
  }
}
