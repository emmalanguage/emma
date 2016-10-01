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
package eu.stratosphere.emma.codegen.flink

import java.net.URI
import java.util.UUID

import eu.stratosphere.emma.macros.RuntimeUtil
import eu.stratosphere.emma.runtime.logger
import org.apache.commons.io.FilenameUtils
import org.apache.flink.core.fs.{FileSystem, Path}
import scala.collection.mutable

import scala.reflect.runtime.{universe => ru}
import scala.tools.reflect.ToolBox

class TempResultsManager(val tb: ToolBox[ru.type], val sessionID: UUID = UUID.randomUUID)
    extends RuntimeUtil {

  import syntax._

  // get the path where the toolbox will place temp results
  private val sessionDir = {
    val tempDir = sys.props("java.io.tmpdir") ->>
      { FilenameUtils.separatorsToUnix } ->>
      { s => s"file:///$s/emma/temp" } ->>
      { sys.props.getOrElse("emma.temp.dir", _) + "/" }

    new URI(tempDir).normalize()
      .resolve(s"$sessionID/")
  }

  private val fs = FileSystem.get(sessionDir)
  private var results = mutable.Set.empty[UUID]
  private var refs = mutable.Map.empty[String, UUID]

  // initialization logic
  {
    logger.info(s"Initializing storage folder for $sessionID at $sessionDir")
    fs.mkdirs(new Path(sessionDir))
  }

  sys addShutdownHook {
    logger.info(s"Removing temp results for session $sessionID")
    fs.delete(new Path(sessionDir), true)
  }

  /** Creates a fresh UUID, assigns it to the given `name`, and returns the temp path associated with it. */
  def assign(name: String): String = {
    val uuid = UUID.randomUUID()
    results += uuid
    refs += name -> uuid
    logger.info(s"Assigning $name to ${refs(name)}")
    sessionDir.resolve(uuid.toString).toString
  }

  /** Resolves UUID currently referenced by the given `name` and returns the temp path associated with it. */
  def resolve(name: String): String = {
    logger.info(s"Resolving $name to ${refs(name)}")
    sessionDir.resolve(refs(name).toString).toString
  }

  /** Garbage collect UUIDs that are no longer referenced. */
  def gc(): Unit = {
    val orphans = results diff refs.values.toSet
    for (orphan <- orphans) {
      logger.info(s"Removing orphaned dataset with UUID $orphan")
      fs.delete(new Path(sessionDir.resolve(orphan.toString)), true)
      results -= orphan
    }
  }
}
