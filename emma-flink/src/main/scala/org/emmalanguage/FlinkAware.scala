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

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.log4j.Logger

trait FlinkAware {

  Logger.getLogger("org.apache.flink").setLevel(org.apache.log4j.Level.WARN)
  Logger.getLogger("org.apache.flink.api.common.io.BinaryInputFormat").setLevel(org.apache.log4j.Level.ERROR)

  protected trait FlinkConfig

  protected val defaultFlinkConfig = new FlinkConfig {}

  protected lazy val defaultFlinkEnv =
    flinkEnv(defaultFlinkConfig)

  protected def flinkEnv(c: FlinkConfig): ExecutionEnvironment = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableSysoutLogging()
    env
  }

  protected def withDefaultFlinkEnv[T](f: ExecutionEnvironment => T): T =
    f(defaultFlinkEnv)
}
