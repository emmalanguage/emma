//package eu.stratosphere.emma.examples.datamining.clustering
//
//import eu.stratosphere.emma.examples.Algorithm
//import net.sourceforge.argparse4j.inf.{Namespace, Subparser}
//
//object HAC {
//
//  object Command {
//    // argument names
//    val KEY_K = "K"
//    val KEY_INPUT = "input-file"
//    val KEY_OUTPUT = "output-file"
//    val KEY_DIMENSIONS = "dimensions"
//
//    // argument defaults
//    val DEF_DIMENSIONS = 3
//  }
//
//  class Command extends Algorithm.Command[KMeans] {
//
//    // algorithm names
//    override def name = "k-means++"
//
//    override def description = "K-Means++ Clustering"
//
//    override def setup(parser: Subparser) = {
//      // add arguments
//      parser.addArgument(Command.KEY_INPUT)
//        .`type`[String](classOf[String])
//        .dest(Command.KEY_INPUT)
//        .metavar("INPUT")
//        .help("input file")
//      parser.addArgument(Command.KEY_OUTPUT)
//        .`type`[String](classOf[String])
//        .dest(Command.KEY_OUTPUT)
//        .metavar("OUTPUT")
//        .help("output file ")
//
//      // add options (prefixed with --)
//      parser.addArgument(s"--${Command.KEY_DIMENSIONS}")
//        .`type`[Integer](classOf[Integer])
//        .dest(Command.KEY_DIMENSIONS)
//        .metavar("N")
//        .help("input dimensions")
//
//      // add defaults for options
//      parser.setDefault(Command.KEY_DIMENSIONS, Command.DEF_DIMENSIONS)
//    }
//  }
//
//}
//
//class HAC(ns: Namespace) extends Algorithm(ns) {
//
//  case class Point(pid: Int, payload: Map[Int, Int])
//
//  import eu.stratosphere.emma._
//
//  def run() {
////    val algorithm = desugar /* workflow */ {
////      val points = read("file:///tmp/points.txt", new InputFormat[Point])
////
////      var clusters = points.groupBy(x => x.pid)
////      var minDistance, theta = 0.5
////
////      do {
////        val distances = for (c1 <- clusters; c2 <- clusters) yield {
////          (c1, c2, (for (p1 <- c1; p2 <- c2) yield cosineDistance(p1, p2)).min())
////        }
////
////        val (c1, c2, d) = distances.minBy(_._3) // result is not unique!!!
////
////        clusters = clusters minus (DataSet(c1) union DataSet(c2)) union DataSet(c1 union c2)
////        minDistance = d
////      } while (Math.abs(minDistance) < theta)
////
////      write("file:///tmp/clusters.txt", new OutputFormat[DataBag[Point]])(clusters)
////    }
//
//    // runtime.factory(address).execute(algorithm)
//  }
//
//  def cosineDistance(p1: Point, p2: Point): Double = ???
//}
