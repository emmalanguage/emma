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

import cogadb.CoGaDB
import test.util._

import org.scalatest._

import java.io.File
import java.nio.file.Paths

trait CoGaDBSpec extends BeforeAndAfter {
  this: Suite =>

  val dir = "/cogadb"
  val path = tempPath("/cogadb")
  val coGaDBPath = Paths.get(Option(System.getenv("COGADB_HOME")) getOrElse "/home/harry/falcontest/falcon/build")
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

  protected def withCoGaDB[R](f: CoGaDB => R): R = {
    val cogadb = setupCoGaDB()
    val result = f(cogadb)
    destroyCoGaDB(cogadb)
    result
  }
}
