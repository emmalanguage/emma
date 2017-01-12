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

import resource._

import api._
import compiler.lang.cogadb._
import io.csv._

import net.liftweb.json._

import sys.process._

import java.io._
import java.nio.file.Files
import java.nio.file.Path

/** CoGaDB runtime. */
class CoGaDB private(coGaDBPath: Path, configPath: Path) {

  //FIXME: @harrygav: why use csvWithHeader in read and just `csv` in write?
  import CoGaDB.csv
  import CoGaDB.csvWithHeader

  val inst = Seq(coGaDBPath.toString, configPath.toString).run(true)
  val tempPath = Files.createTempDirectory("emma-cogadbd").toAbsolutePath

  var dflNames = Stream.iterate(0)(_ + 1).map(i => f"dataflow$i%04d").toIterator
  var srcNames = Stream.iterate(0)(_ + 1).map(i => f"source$i%04d").toIterator

  sys.addShutdownHook(destroy())

  /**
   * Execute a the dataflow rooted at `df` and write the result in a fresh table in CoGaDB.
   *
   * TODO: @harrygav: this needs to be refactored
   *
   * @return A table scan over the newly created table.
   */
  def execute(df: ast.Op): ast.TableScan = df match {
    case df@ast.TableScan(_, _) =>
      df // just a table scan, nothing to do
    case df@ast.MaterializeResult(tableName, _, _) =>
      // already wrapped in a `MaterializeResult` by the client
      eval(df)
      ast.TableScan(tableName)
    case _ =>
      // default case
      val dflName = dflNames.next()
      eval(ast.MaterializeResult(dflName, false, df))
      ast.TableScan(dflName)
  }

  def write[A](
    seq: Seq[A]
  )(
    implicit c: CSVConverter[A], m: Meta[A], schema: Schema[A]
  ): ast.TableScan = {
    // write into csv
    val dstName = srcNames.next()
    val dstPath = tempPath.resolve(s"$dstName.csv")

    DataBag(seq).writeCSV(dstPath.toString, csv)

    val read = ast.MaterializeResult(
      dstName,
      false,
      ast.ImportFromCsv(dstName, dstPath.toString, "|", schema.fields()))

    execute(read)

    ast.TableScan(dstName)
  }

  def read[A](df: ast.TableScan)(implicit converter: CSVConverter[A]): Seq[A] = {
    // write into csv
    val srcName = df.tableName
    val srcPath = tempPath.resolve(s"$srcName.csv")

    //@FIXME: This does not make sense, need to refactor eval and execute
    execute(ast.ExportToCsv(srcPath.toString, "|", df))

    DataBag.readCSV[A](srcPath.toString, csv).fetch()
  }

  def destroy(): Unit = {
    inst.destroy()
    deleteRecursive(tempPath.toFile)
  }

  /** Deletes a file recursively. */
  private def deleteRecursive(path: java.io.File): Boolean = {
    val ret =
      if (!path.isDirectory) true /* regular file */
      else path.listFiles().toSeq.foldLeft(true)((_, f) => deleteRecursive(f))

    ret && path.delete()
  }

  /** TODO: @harrygav: this needs to be refactored */
  private def eval(df: ast.MaterializeResult) =
    try {
      println((s"echo execute_query_from_json ${saveJson(df)}" #| "nc localhost 8000").!!)
    } catch {
      //FIXME @harrygav: NEVER catch exceptions without handling them properly
      //FIXME @harrygav: cannot assume that the Exception is due to the cited reason
      case _: Exception => println("Warning: no process listening on port 8000")
    }

  // FIXME: This is not used, if not needed at the moment or in the future, please remove
  /** Run a general command to cogadb */
  private def !(cmd: String, params: String) =
    try {
      println((s"$cmd $params" #| "nc localhost 8000").!!)
    } catch {
      //FIXME @harrygav: NEVER catch exceptions without handling them properly
      //FIXME @harrygav: cannot assume that the Exception is due to the cited reason
      case _: Exception => println("Warning: no process listening on port 8000")
    }

  private def saveJson(df: ast.MaterializeResult): Path = {
    val path = tempPath.resolve(s"${df.tableName}.json")

    for {
      fw <- managed(new FileWriter(path.toFile))
      bw <- managed(new BufferedWriter(fw))
    } yield {
      bw.write(prettyRender(asJson(ast.Root(df))))
    }

    path
  }
}

object CoGaDB {

  val csv = CSV(delimiter = '|', quote = Some(' '))
  val csvWithHeader = CSV(delimiter = '|', quote = Some(' '), header = true)

  def apply(coGaDBPath: Path, configPath: Path): CoGaDB =
    new CoGaDB(coGaDBPath.resolve("bin/cogadbd"), configPath)
}