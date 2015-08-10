package ${package}

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.runtime.Native
import org.junit.Test

class JobTest() {

  @Test
  def integrationTest() = {
    Job.main(Array())
  }
}