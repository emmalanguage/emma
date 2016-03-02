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
      case e: Exception => e.printStackTrace()
    }
  }
}
