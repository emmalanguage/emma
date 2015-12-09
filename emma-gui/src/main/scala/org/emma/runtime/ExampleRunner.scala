package org.emma.runtime

import java.lang.reflect.Constructor

import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.runtime.Engine

import net.sourceforge.argparse4j.inf.Namespace

class ExampleRunner extends Thread {
  var localRuntime: Engine = null
  var parameter: Namespace = null
  var constructor: Constructor[_] = null

  def this(localRuntime: Engine, parameter: Namespace, constructor: Constructor[_]) = {
    this()
    this.setName("ExampleRunner")
    this.localRuntime = localRuntime
    this.parameter = parameter
    this.constructor = constructor
  }

  override def run() {
    var exampleObject: Algorithm = null
    try {
      exampleObject = constructor.newInstance(parameter, localRuntime).asInstanceOf[Algorithm]
    } catch {
      case e: Any => e.printStackTrace()
    }

    try {
      exampleObject.run()
    } catch {
      case e: Exception => {
        if (e.isInstanceOf[InterruptedException]) {
          System.err.println("Execution stopped")
        }
        else {
          throw e
        }
      }
    }
  }
}
