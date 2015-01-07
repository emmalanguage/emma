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
    val KEY_RT_TYPE_DEFAULT = "flink"
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
        .choices("local", "flink", "spark")
        .dest(Command.KEY_RT_TYPE)
        .metavar("RT")
        .help(s"runtime type (default ${Command.KEY_RT_TYPE_DEFAULT}})")
      // runtime host
      parser.addArgument(s"--${Command.KEY_RT_HOST}")
        .`type`[String](classOf[String])
        .dest(Command.KEY_RT_HOST)
        .metavar("HOST")
        .help(s"runtime host (default ${Command.KEY_RT_HOST_DEFAULT}})")
      // runtime port
      parser.addArgument(s"--${Command.KEY_RT_PORT}")
        .`type`[Integer](classOf[Integer])
        .dest(Command.KEY_RT_PORT)
        .metavar("PORT")
        .help(s"runtime port (default ${Command.KEY_RT_PORT_DEFAULT}})")

      parser.setDefault(Command.KEY_RT_TYPE, Command.KEY_RT_TYPE_DEFAULT)
      parser.setDefault(Command.KEY_RT_HOST, Command.KEY_RT_HOST_DEFAULT)
      parser.setDefault(Command.KEY_RT_PORT, Command.KEY_RT_PORT_DEFAULT)
    }

    /**
     * Create an instance of the algorithm.
     *
     * @param ns The parsed arguments to be passed to the algorithm constructor.
     * @return
     */
    def instantiate(ns: Namespace): Algorithm = {
      // instantiate runtime
      val rt = runtime.factory(
        ns.get[String](Command.KEY_RT_TYPE),
        ns.get[String](Command.KEY_RT_HOST),
        ns.get[Int](Command.KEY_RT_PORT))
      // instantiate and return algorithm
      val constructor = m.runtimeClass.getConstructor(classOf[Namespace], classOf[runtime.Engine])
      constructor.newInstance(ns, rt).asInstanceOf[Algorithm]
    }
  }

}

abstract class Algorithm(val rt: runtime.Engine) {

  def run(): Unit
}

