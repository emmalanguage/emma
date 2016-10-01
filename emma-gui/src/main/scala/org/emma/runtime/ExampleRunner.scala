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
package org.emma.runtime

import java.lang.reflect.{InvocationTargetException, Constructor}
import java.security.InvalidParameterException

import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.runtime.{Flink, Engine, RuntimePlugin}

import net.sourceforge.argparse4j.inf.Namespace

class ExampleRunner(constructor: Constructor[Algorithm], args: Namespace,
    plugins: Seq[RuntimePlugin] = Nil, runtime: Engine = new Flink) extends Thread {

  override def run() : Unit = {
    runtime.plugins = plugins
    try constructor.newInstance(args, runtime).run() catch {
      case e: InvocationTargetException if e.getCause.isInstanceOf[ClassCastException] =>
        throw new InvalidParameterException("Given parameter types do not match the example constructor.")
      case e: InterruptedException => ; //expected
      case e: Exception => e.printStackTrace()
    }
  }
}
