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
package io

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path => HadoopPath}

import java.io._
import java.net.URI
import java.nio.file.Paths

/** An abstract interface for local IO support. */
abstract class ScalaSupport[A, F <: Format] {

  /** The concrete class of the underlying format. */
  def format: F

  /** Invoked by the `read` methods of local [[api.DataBag]] implementations. */
  private[emmalanguage] def read(path: String): TraversableOnce[A]

  /** Invoked by the `write` methods of local [[api.DataBag]] implementations. */
  private[emmalanguage] def write(path: String)(xs: Traversable[A]): Unit

  protected def inpStream(uri: URI): InputStream = uri.getScheme match {
    case "hdfs" =>
      val path = new HadoopPath(uri)
      val deFS = new URI(uri.toString.replace(uri.getPath, "/"))
      FileSystem.get(deFS, new Configuration()).open(path)
    case _ =>
      val path = Paths.get(ensureFileScheme(uri))
      new FileInputStream(path.toFile)
  }

  protected def outStream(uri: URI): OutputStream = uri.getScheme match {
    case "hdfs" =>
      val path = new HadoopPath(uri)
      val deFS = new URI(uri.toString.replace(uri.getPath, "/"))
      FileSystem.get(deFS, new Configuration()).create(path, true)
    case _ =>
      val path = Paths.get(ensureFileScheme(uri))
      deleteRecursive(path.toFile)
      new FileOutputStream(path.toFile)
  }

  protected def deleteRecursive(path: File): Boolean = {
    val ret = if (path.isDirectory) {
      path.listFiles().toSeq.foldLeft(true)((r, f) => deleteRecursive(f))
    } else /* regular file */ {
      true
    }
    ret && path.delete()
  }

  private def ensureFileScheme(uri: URI): URI =
    if (uri.getScheme == "file") uri
    else fileRoot.resolve(uri)

  private val fileRoot = new URI("file:///")
}
