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
package eu.stratosphere.emma.runtime

import java.util.UUID

trait AbstractStatefulBackend[S, K] {

  private val STATEFUL_BASE_PATH = String.format("%s/emma/stateful", System.getProperty("java.io.tmpdir"))

  private val uuid = UUID.randomUUID()
  private val fileNameBase = s"$STATEFUL_BASE_PATH/$uuid"
  protected var seqNumber = 0

  protected def currentFileName() = s"${fileNameBase}_$seqNumber"
  protected def oldFileName() = s"${fileNameBase}_${seqNumber - 1}"

}
