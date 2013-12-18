package eu.stratosphere.emma.examples.datamining.kmeans

import _root_.net.sourceforge.argparse4j.inf.Subparser
import _root_.scala.util.Random
import _root_.eu.stratosphere.emma.examples.Algorithm
import _root_.eu.stratosphere.emma._

object KMeans {

  // argument names
  val KEY_K = "K"

  // constnats
  val Seed = 5431423142056L

  class Config extends Algorithm.Config[KMeans] {

    // algorithm names
    override val CommandName = "k-means++"
    override val Name = "K-Means++ Clustering"

    override def setup(parser: Subparser) = {
      // get common setup
      super.setup(parser)

      // add options (prefixed with --)
      parser.addArgument(s"-${KMeans.KEY_K}")
        .`type`[Integer](classOf[Integer])
        .dest(KMeans.KEY_K)
        .metavar("K")
        .help("number of clusters")

      // add defaults for options
      parser.setDefault(KMeans.KEY_K, new Integer(3))
    }
  }

}

class PointsInputFormat(val dimensions: Int) extends InputFormat[List[Double]] {

}

class KMeans(val args: Map[String, Object]) extends Algorithm(args) {

  def run() = {

    //    val dataflow = dataflow
    {

      // algorithm specific parameters
      val K = arguments.get(KMeans.KEY_K).get.asInstanceOf[Int]
      val random = new Random(KMeans.Seed)

      val points = read[List[Double]](inputPath, new PointsInputFormat(dimensions))
      val centers = for (i <- 0 to K) yield for (j <- 0 to dimensions) yield random.nextDouble()
    }

    // dataflow.execute(master.location, master.host)
  }
}

