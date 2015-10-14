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

class TempResultsManager(
  val tb: ToolBox[ru.type],
  val sessionID: UUID = UUID.randomUUID) extends RuntimeUtil {

  // get the path where the toolbox will place temp results
  private val sessionDir = {
    val tempDir = "java.io.tmpdir"       ->>
      { System.getProperty }             ->>
      { FilenameUtils.separatorsToUnix } ->>
      { s => s"file:///$s/emma/temp" }   ->>
      { System.getProperty("emma.temp.dir", _) + "/" }

    tempDir                              ->>
      { new URI(_).normalize() }         ->>
      { _.resolve(sessionID.toString + "/") }
  }

  private val fs      = FileSystem.get(sessionDir)
  private var results = mutable.Set.empty[UUID]
  private var refs    = mutable.Map.empty[String, UUID]

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
