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
package examples

import api._

class ClickCountDiffsIntegrationSpec extends BaseClickCountDiffsIntegrationSpec with FlinkAware {

  override def clickCountDiffs(baseName: String, numDays: Int): Unit = {
    implicit val flink = org.apache.flink.api.scala.ExecutionEnvironment.getExecutionEnvironment
    flink.setParallelism(1)
    emma.onFlink {
      ClickCountDiffs(baseName, numDays)
    }
  }
}
