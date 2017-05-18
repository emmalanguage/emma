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


//import org.apache.log4j.Logger
import runtime.CoGaDB


import java.nio.file.Paths
import java.nio.file.Path


trait CoGaDBAware {

 // Logger.getLogger("org.apache.spark").setLevel(org.apache.log4j.Level.WARN)

  protected trait CoGaDBConfig {
    val cogaDBPath: Path
    val configPath: Path
  }

  protected val defaultCoGaDBConfig = new CoGaDBConfig {
    val cogaDBPath = Paths.get(Option(System.getenv("COGADB_HOME")) getOrElse "/tmp/cogadb")
    val configPath = Paths.get(getClass.getResource("/default.coga").toURI)
  }

  protected lazy val defaultCoGaDBRuntime =
    setupCoGaDB()


  protected def setupCoGaDB(): CoGaDB =
    CoGaDB(defaultCoGaDBConfig.cogaDBPath, defaultCoGaDBConfig.configPath)

  protected def destroyCoGaDB(cogadb: CoGaDB): Unit =
    cogadb.destroy()

  protected def coGaDBRuntime(c: CoGaDBConfig): CoGaDB =
    setupCoGaDB()

  def withDefaultCoGaDBRuntime[T](f: CoGaDB => T): T = {
    val cogadb = setupCoGaDB()
    val result = f(cogadb)
    destroyCoGaDB(cogadb)
    result
  }

}
