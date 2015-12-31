package org.emma.runtime

import java.lang.reflect.Constructor

import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.runtime.{RuntimePlugin, Flink}

import net.sourceforge.argparse4j.inf.Namespace

class FlinkExampleRunner(constructor: Constructor[Algorithm], args: Namespace,
    plugins: Seq[RuntimePlugin] = Nil) extends Thread {

  override def run() {
    val runtime = new Flink
    runtime.plugins = plugins
    try constructor.newInstance(args, runtime).run() catch {
      case _: InterruptedException => System.err.println("Execution stopped")
      case ex: Exception => ex.printStackTrace()
    }
  }
}
