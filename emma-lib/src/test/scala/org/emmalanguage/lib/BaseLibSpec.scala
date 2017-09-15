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
package lib

import api.DataBagEquality
import test.util._

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import java.io.File

trait BaseLibSpec extends FlatSpec with Matchers with BeforeAndAfterAll with DataBagEquality {

  type ResourceInitializer = () => Unit

  def tempPaths: Seq[String] = Seq.empty

  def resources: Seq[ResourceInitializer] = Seq.empty

  override protected def beforeAll(): Unit = {
    for (path <- tempPaths)
      new File(tempPath(path)).mkdirs()
    for (init <- resources)
      init()
  }

  override protected def afterAll(): Unit = {
    for (path <- tempPaths)
      deleteRecursive(new File(tempPath(path)))
  }
}
