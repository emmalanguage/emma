package eu.stratosphere.emma.examples.datamining.clustering

import java.lang.{Double => JDouble}
import eu.stratosphere.emma.api._
import eu.stratosphere.emma.api.model._
import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.runtime.Engine
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}
import breeze.linalg.{sum, DenseVector}

class KMeans(k: Int, epsilon: Double, iterations: Int, input: String, output: String, rt: Engine)
    extends Algorithm(rt) {

  import eu.stratosphere.emma.examples.datamining.clustering.KMeans.Schema._

  def this(ns: Namespace, rt: Engine) = this(
    ns.get[Int](KMeans.Command.k),
    ns.get[Double](KMeans.Command.epsilon),
    ns.get[Int](KMeans.Command.iterations),
    ns.get[String](KMeans.Command.input),
    ns.get[String](KMeans.Command.output),
    rt)

  val algorithm = emma.parallelize {
    // read input
    val points = for (line <- read(input, new TextInputFormat[String]('\n'))) yield {
      val record = line.split("\t")
      Point(record.head.toLong, DenseVector(record.tail.map { _.toDouble }))
    }

    val dimensions = points.find { _ => true }.get.pos.length
    var bestSolution = DataBag(Seq.empty[Solution])
    var minSqrDist = 0.0
    val zeroVec = DenseVector.zeros[Double](dimensions)

    for (i <- 1 to iterations) {
      // initialize forgy cluster means
      var change = 0.0
      var centroids = DataBag(points.random(k))

      // initialize solution
      var solution = for (p <- points) yield {
        val closestCentroid = centroids.minBy { (m1, m2) =>
          val diff1 = p.pos - m1.pos
          val diff2 = p.pos - m2.pos
          (diff1.t * diff1) < (diff2.t * diff2)
        }.get

        val diff    = p.pos - closestCentroid.pos
        val sqrDiff = diff.t * diff
        Solution(p, closestCentroid.id, sqrDiff)
      }

      do {
        // update means
        val newMeans = for (cluster <- solution.groupBy { _.cluster }) yield {
          val sum = cluster.values.fold(zeroVec)( _.point.pos, _ + _ )
          val cnt = cluster.values.count().toDouble //FIXME Long is converted to Double here
          Point(cluster.key, sum :/ cnt)
        }

        // compute change between the old and the new means
        change = (for {
          mean <- centroids
          newMean <- newMeans
          if mean.id == newMean.id
        } yield sum((mean.pos - newMean.pos) :* (mean.pos - newMean.pos))).sum()

        // update solution: re-assign clusters
        solution = for (s <- solution) yield {
          val closestMean = centroids.minBy { (m1, m2) =>
            val diff1 = s.point.pos - m1.pos
            val diff2 = s.point.pos - m2.pos
            (diff1.t * diff1) < (diff2.t * diff2)
          }.get

          val diff    = s.point.pos - closestMean.pos
          val sqrDiff = diff.t * diff
          s.copy(cluster = closestMean.id, sqrDiff = sqrDiff)
        }

        // use new means for the next iteration
        centroids = newMeans
      } while (change > epsilon)

      val sumSqrDist = solution.map { _.sqrDiff }.sum()
      if (i <= 1 || sumSqrDist < minSqrDist) {
        minSqrDist = sumSqrDist
        bestSolution = solution
      }
    }

    // write result
    val result = for (s <- bestSolution) yield (s.point.id, s.cluster)
    write(output, new CSVOutputFormat[(PID, PID)])(result)
    result
  }

  def run() = algorithm.run(rt)
}

object KMeans {

  object Command {
    // argument names
    val k = "K"
    val epsilon = "epsilon"
    val iterations = "iterations"
    val input = "input-file"
    val output = "output-file"
  }

  class Command extends Algorithm.Command[KMeans] {

    // algorithm names
    override def name = "k-means"

    override def description = "K-Means Clustering"

    override def setup(parser: Subparser) = {
      // basic setup
      super.setup(parser)

      // add arguments
      parser.addArgument(Command.k)
        .`type`[Integer](classOf[Integer])
        .dest(Command.k)
        .metavar("K")
        .help("number of clusters")

      parser.addArgument(Command.epsilon)
        .`type`[JDouble](classOf[JDouble])
        .dest(Command.epsilon)
        .metavar("EPSILON")
        .help("termination threshold")

      parser.addArgument(Command.iterations)
        .`type`[Integer](classOf[Integer])
        .dest(Command.iterations)
        .metavar("ITERATIONS")
        .help("number of repeated iterations")

      parser.addArgument(Command.input)
        .`type`[String](classOf[String])
        .dest(Command.input)
        .metavar("INPUT")
        .help("input file")

      parser.addArgument(Command.output)
        .`type`[String](classOf[String])
        .dest(Command.output)
        .metavar("OUTPUT")
        .help("output file")
    }
  }

  object Schema {
    type PID = Long

    case class Point(@id id: PID, pos: DenseVector[Double]) extends Identity[PID] {
      def identity = id
    }

    case class Solution(point: Point, cluster: PID, sqrDiff: Double = 0)
  }
}

