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
package api

import runtime.CoGaDB
import test.util._
import test.schema.Literature._
import api.Meta.Projections._

import org.scalatest.BeforeAndAfter
import org.scalatest.FreeSpec
import org.scalatest.Matchers
import org.scalatest.prop.PropertyChecks


import java.io.File
import java.nio.file.Paths

class CoGaDBTableSpec extends FreeSpec with Matchers with BeforeAndAfter with PropertyChecks with DataBagEquality {


  val dir = "/cogadb"
  val path = tempPath("/cogadb")
  val coGaDBPath = Paths.get(Option(System.getenv("COGADB_HOME")) getOrElse "/tmp/cogadb/")
  val configPath = Paths.get(materializeResource(s"$dir/tpch.coga"))

  before {
    new File(path).mkdirs()
  }

  after {
    deleteRecursive(new File(path))
  }

  protected def setupCoGaDB(): CoGaDB =
    CoGaDB(coGaDBPath, configPath)

  protected def destroyCoGaDB(cogadb: CoGaDB): Unit =
    cogadb.destroy()

  // ---------------------------------------------------------------------------
  // abstract trait methods
  // ---------------------------------------------------------------------------

  /** The type of the backend context associated with the refinement type (e.g. SparkContext). */
  type BackendContext = CoGaDB

  /** The [[DataBag]] refinement type under test (e.g. SparkRDD). */
  type TestBag[A] = CoGaDBTable[A]

  /** The [[DataBag]] refinement companion type under test (e.g. SparkRDD). */
  val TestBag: DataBagCompanion[BackendContext] = CoGaDBTable

  /** A system-specific suffix to append to test files. */
  def suffix: String = "cogadb"

  /** A function providing a backend context instance which lives for the duration of `f`. */
  def withBackendContext[T](f: BackendContext => T): T = {
    val cogadb = setupCoGaDB()
    val result = f(cogadb)
    destroyCoGaDB(cogadb)
    result
  }

  "monad ops" - {
    "map" in withBackendContext { implicit ctx =>
      val act = TestBag(hhCrts.map(c => (c.book.title,c.name)))
        .map(c => c._1)

      val exp = hhCrts
        .map(c => c.name)

      act shouldEqual DataBag(exp)
    }
  }
}
