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

import io.csv.{CSV, CSVConverter}

class ScalaTraversableSpec extends DataBagSpec {

  override type Bag[A] = ScalaTraversable[A]
  override type BackendContext = Unit

  override def withBackendContext[T](f: BackendContext => T): T =
    f(Unit)

  override def Bag[A: Meta](implicit unit: BackendContext): Bag[A] =
    ScalaTraversable[A]

  override def Bag[A: Meta](seq: Seq[A])(implicit unit: BackendContext): Bag[A] =
    ScalaTraversable(seq)

  override def readCSV[A: Meta : CSVConverter](path: String, format: CSV)(implicit unit: BackendContext): DataBag[A] =
    ScalaTraversable.readCSV(path, format)
}
