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
package compiler

import Memo.memoizeTypeInfo

import org.apache.flink.api.scala.createTypeInformation

class LabyrinthCodegenIntegrationSpec extends BaseCodegenIntegrationSpec
  with LabyrinthCompilerAware
  with LabyrinthAware {

  import compiler._

  def withBackendContext[T](f: Env => T): T =
    withDefaultFlinkStreamEnv(f)

  memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[test.schema.Movies.ImdbMovie]], createTypeInformation)
  memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[Seq[test.schema.Movies.ImdbMovie]]], createTypeInformation)
  memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[List[(String, Int)]]], createTypeInformation)
  memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[(String, Int)]], createTypeInformation)
  memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[scala.collection.immutable.IndexedSeq[(Int, Int)]]],
    createTypeInformation)
  memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[org.emmalanguage.api.Group[Int,Int]]], createTypeInformation)
  memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[(String, Long, Double, Double, Double)]], createTypeInformation)
  memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[(Int, Int, Long)]], createTypeInformation)
  memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[scala.util.Either[Int,Int]]], createTypeInformation)
  memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[(Int, Int, Int)]], createTypeInformation)
  memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[scala.collection.immutable.Range]], createTypeInformation)
  memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[scala.util.Either[(Int, Int),Int]]], createTypeInformation)
  memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[((Int, Int), Int)]], createTypeInformation)
  memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[
    org.emmalanguage.api.Group[Int,(Int, Option[Double], Option[Double], Long)]]], createTypeInformation)
  memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[org.emmalanguage.api.Group[(Int, Int),Long]]],
    createTypeInformation)

  "test simple" in withBackendContext(implicit env => {
    verify(u.reify {
      val xs = 1
    })
  })
}
