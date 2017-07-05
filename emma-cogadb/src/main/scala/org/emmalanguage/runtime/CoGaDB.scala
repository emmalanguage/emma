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
package runtime

import api.Meta
import compiler.RuntimeCompiler
import compiler.lang.cogadb._
import io.csv._

import net.liftweb.json._
import resource._

import sys.process._

import java.io._
import java.nio.file.Files
import java.nio.file.Path

/** CoGaDB runtime. */
class CoGaDB private(coGaDBPath: Path, configPath: Path) {

  import CoGaDB.ExecutionException
  import CoGaDB.csv
  import CoGaDB.csvConverterForType
  import CoGaDB.schemaForType

  /** The underlying CoGaDB process. */
  val inst = Seq(coGaDBPath.toString, configPath.toString).run(true)
  /** The tmp path for this CoGaDB session. */
  val tempPath = Files.createTempDirectory("emma-cogadbd").toAbsolutePath
  /** A stream of dataflow names to be consumed by the session. */
  var dflNames = Stream.iterate(0)(_ + 1).map(i => f"dataflow$i%04d").toIterator

  /** Destroy the CoGaDB instance on shutdown. */
  sys.addShutdownHook(destroy())

  /**
   * Import a Scala [[Seq]] as a fresh tabe in [[CoGaDB]].
   *
   * @return An [[ast.TableScan]] over the imported table.
   */
  def importSeq[A: Meta](seq: Seq[A]): ast.TableScan = {
    val dflName = dflNames.next()
    val datPath = tempPath.resolve(s"$dflName.csv")

    // write `seq` into a fresh CSV file
    CSVScalaSupport[A](csv).write(datPath.toString)(seq)
    // create an "import from csv" dataflow
    val impDfl = ast.ImportFromCsv(dflName, datPath.toString, "|", schemaForType.fields())
    // import dataflow must be wrapped by a STORE_TABLE operator in CoGaDB, in order to be materialized
    val matDfl = ast.MaterializeResult(dflName, false, impDfl)
    // execute the dataflow
    execute(matDfl, dflName)
    // return a ast.TableScan for the imported table
    ast.TableScan(dflName.toUpperCase())
  }

  /**
   * Export a [[CoGaDB]] table represented by an [[ast.TableScan]].
   *
   * @return A Scala [[Seq]] containing the table elements.
   */
  def exportSeq[A: Meta](dfl: ast.Op): Seq[A] = {
    val dflName = dflNames.next()
    val datPath = tempPath.resolve(s"$dflName.csv")

    // create an "export to csv" dataflow
    val expDfl = ast.ExportToCsv(datPath.toString, "|", dfl)
    // execute the dataflow
    execute(expDfl, dflName)
    // read CSV data as a `seq`
    CSVScalaSupport[A](csv).read(datPath.toString).toStream
  }

  private def execute(dfl: ast.Op, dflName: String) =
    try {
      (s"echo execute_query_from_json ${saveJson(dfl, dflName)}" #| "nc localhost 8000").!!
    } catch {
      case e: Exception => throw ExecutionException(s"Cannot execute CoGaDB query `$dflName`: $e", e)
    }

  private def saveJson(dfl: ast.Op, dflName: String): Path = {
    val path = tempPath.resolve(s"$dflName.json")

    (for {
      fw <- managed(new FileWriter(path.toFile))
      bw <- managed(new BufferedWriter(fw))
    } yield {
      bw.write(prettyRender(asJson(ast.Root(dfl))))
    }).acquireAndGet(_ => ())

    path
  }

  // ---------------------------------------------------------------------------
  // Destruction logic
  // ---------------------------------------------------------------------------

  def destroy(): Unit = {
    inst.destroy()
    //deleteRecursive(tempPath.toFile)
  }

  /** Deletes a file recursively. */
  private def deleteRecursive(path: java.io.File): Boolean = {
    val ret =
      if (!path.isDirectory) true /* regular file */
      else path.listFiles().toSeq.foldLeft(true)((_, f) => deleteRecursive(f))

    ret && path.delete()
  }
}

object CoGaDB {

  case class ExecutionException(message: String, cause: Throwable) extends RuntimeException

  val csv = CSV(delimiter = '|', quote = Some(' '), skipRows = 1)

  def apply(coGaDBPath: Path, configPath: Path): CoGaDB =
    new CoGaDB(coGaDBPath.resolve("bin/cogadbd"), configPath)

  // ---------------------------------------------------------------------------
  // Memoized, Runtime Generated CSVConverter[T] and Schema[T] instances
  // ---------------------------------------------------------------------------

  val compiler = new RuntimeCompiler()

  import compiler.u._

  private lazy val csvConverterForTypeMemo = collection.mutable.Map.empty[Any, Any]
  private lazy val schemaForTypeMemo = collection.mutable.Map.empty[Any, Any]

  /** Implicit memoized synthesizer for [[CSVConverter]][T] instances unsing runtime reflection. */
  implicit private def csvConverterForType[T: Meta]: CSVConverter[T] = {
    val ttag = implicitly[Meta[T]].ttag
    csvConverterForTypeMemo.getOrElseUpdate(
      ttag,
      compiler.eval[CSVConverter[T]](
        q"implicitly[org.emmalanguage.io.csv.CSVConverter[${ttag.tpe.asInstanceOf[Type]}]]")
    ).asInstanceOf[CSVConverter[T]]
  }

  /** Implicit memoized synthesizer for [[Schema]][T] instances unsing runtime reflection. */
  implicit private def schemaForType[T: Meta]: Schema[T] = {
    val ttag = implicitly[Meta[T]].ttag
    schemaForTypeMemo.getOrElseUpdate(
      ttag,
      compiler.eval[Schema[T]](
        q"implicitly[org.emmalanguage.compiler.lang.cogadb.Schema[${ttag.tpe.asInstanceOf[Type]}]]")
    ).asInstanceOf[Schema[T]]
  }
}