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
package eu.stratosphere.emma.examples

import net.sourceforge.argparse4j.inf.{Namespace, Subparser}

import eu.stratosphere.emma.runtime

object Algorithm {

  object Command {
    // argument names
    val KEY_RT_TYPE = "rt-type"
    val KEY_RT_HOST = "rt-host"
    val KEY_RT_PORT = "rt-port"
    // default values
    val KEY_RT_TYPE_DEFAULT = "flink-remote"
    val KEY_RT_HOST_DEFAULT = "localhost"
    val KEY_RT_PORT_DEFAULT = 6123
  }

  abstract class Command[A <: Algorithm](implicit val m: scala.reflect.Manifest[A]) {

    /**
     * Algorithm key.
     */
    def name: String

    /**
     * Algorithm name.
     */
    def description: String

    /**
     * Algorithm subparser configuration.
     *
     * @param parser The subparser for this algorithm.
     */
    def setup(parser: Subparser): Unit = {
      // runtime type
      parser.addArgument(s"--${Command.KEY_RT_TYPE}")
        .`type`[String](classOf[String])
        .choices("native", "flink", "spark")
        .dest(Command.KEY_RT_TYPE)
        .metavar("RT")
        .help(s"runtime type (default ${Command.KEY_RT_TYPE_DEFAULT}})")

      parser.setDefault(Command.KEY_RT_TYPE, Command.KEY_RT_TYPE_DEFAULT)
    }

    /**
     * Create an instance of the algorithm.
     *
     * @param ns The parsed arguments to be passed to the algorithm constructor.
     * @return
     */
    def instantiate(ns: Namespace): Algorithm = {
      // instantiate runtime
      val rt = runtime.factory(ns.get[String](Command.KEY_RT_TYPE)).default()
      // instantiate and return algorithm
      val constructor = m.runtimeClass.getConstructor(classOf[Namespace], classOf[runtime.Engine])
      constructor.newInstance(ns, rt).asInstanceOf[Algorithm]
    }
  }

}

abstract class Algorithm(val rt: runtime.Engine) {

  def run(): Unit
}
