package org.emma.runtime

import java.util.concurrent.CountDownLatch
import eu.stratosphere.emma.runtime.{Engine, RuntimePlugin, Flink}

object LocalRuntime {
  private var instance: LocalRuntime.type = null
  private var runtimePlugin: EmmaDemoInterface = null
  private var planRuntime: Engine = null

  def getInstance: LocalRuntime.type = {
    if (instance == null) {
      runtimePlugin = new EmmaDemoInterface()
      runtimePlugin.setBlockingLatch(new CountDownLatch(1))
      planRuntime = new Flink
      planRuntime.plugins = List[RuntimePlugin](runtimePlugin)
      instance = this
    }
    instance
  }

  def getRuntimePlugin: EmmaDemoInterface = {
    runtimePlugin
  }

  def getPlanRuntime: Engine = {
    planRuntime
  }

  def nextStep() {
    val latch: CountDownLatch = runtimePlugin.getBlockingLatch()
    if (latch != null) {
      latch.countDown()
      runtimePlugin.setBlockingLatch(new CountDownLatch(1))
    }
  }
}
