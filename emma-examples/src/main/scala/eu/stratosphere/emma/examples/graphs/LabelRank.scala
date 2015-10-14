package eu.stratosphere.emma.examples.graphs

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.api.model._
import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.runtime.Engine
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}

import scala.collection.mutable
import scala.math.pow

class LabelRank(
      input:           String,
      output:          String,
      maxOscillations: Int,
      inflation:       Double,
      cutoff:          Double,
      similarity:      Double,
      rt:              Engine)
    extends Algorithm(rt) {
  import eu.stratosphere.emma.examples.graphs.LabelRank._
  import Schema._

  require(maxOscillations > 0, "maxOscillations should be positive")
  require(inflation       > 1, "inflation should be >1")
  require(0 < cutoff     && cutoff     < 1, "cutoff should be between 0 and 1")
  require(0 < similarity && similarity < 1, "similarity should be between 0 and 1")

  def this(ns: Namespace, rt: Engine) = this(
    ns get LabelRank.Command.keyInput,
    ns get LabelRank.Command.keyOutput,
    ns get LabelRank.Command.keyOscillations,
    ns get LabelRank.Command.keyInflation,
    ns get LabelRank.Command.keyCutoff,
    ns get LabelRank.Command.keySimilarity,
    rt)

  def run() = algorithm run rt

  val algorithm = emma.parallelize {
    // algorithm parameters
    val inf = inflation
    val cut = cutoff
    val sim = similarity
    // initialize graph
    val directed  = read(input, new CSVInputFormat[Edge[Vid]])
    val reversed  = for (Edge(src, dst) <- directed) yield Edge(dst, src)
    val vertexIds = directed.map(_.src).distinct()
    val loops     = for (v <- vertexIds) yield Edge(v, v)
    val edges     = (directed plus reversed plus loops).distinct()
    val vertices  = for (ge <- edges.groupBy(_.src))
      yield Vertex(ge.key, ge.values.count())

    var labels    = for {
      e <- edges
      v <- vertices
      if v.id == e.src
    } yield Label(v.id, e.dst, 1.0 / v.degree)

    val oscillating = mutable.Map.empty[Long, Long] withDefaultValue 0
    var changes     = 1l

    while (changes > 0 && oscillating(changes) < maxOscillations) {
      val propagation = for {
        e  <- edges
        li <- labels
        if li.vertex   == e.src
        lj <- labels
        if lj.identity == e.dst -> li.label
      } yield Label(li.vertex, li.label, lj.prob)

      val propLabels = for (gp <- propagation.groupBy(_.identity)) yield {
        val sum = gp.values.map(_.prob).sum()
        val cnt = gp.values.count()
        Label(gp.key._1, gp.key._2, sum / cnt)
      }

      val inflation = for (gi <- propLabels.groupBy(_.vertex))
        yield gi.key -> gi.values.map(l => pow(l.prob, inf)).sum()

      inflation.count()

      val infLabels = for {
        pl <- propLabels
        in <- inflation
        if pl.vertex == in._1
        p   = pow(pl.prob, inf) / in._2
        if p >= cut // cutoff
      } yield pl.copy(prob = p)

      val maxLabels = for (gm <- infLabels.groupBy(_.vertex))
        yield gm.key -> gm.values.fold(Map.empty[Lid, Double])(
          l => Map(l.label -> l.prob), mergeMax(_, _)).keySet

      val condUpdates = for {
        e  <- edges
        li <- maxLabels
        if li._1 == e.src
        lj <- maxLabels
        if lj._1 == e.dst
      } yield (li._1, li._2, lj._2)

      val condSums = for (gc <- condUpdates.groupBy(_._1))
        yield gc.key -> gc.values.map {
          case (_, ls1, ls2) => if (ls1 subsetOf ls2) 1 else 0
        }.sum()

      condSums.count()

      labels = for {
        l <- infLabels
        v <- vertices
        if v.id == l.vertex
        s <- condSums
        if v.id == s._1
        if s._2 <= sim * v.degree
      } yield identity(l)

      changes = labels.count()
      oscillating(changes) += 1
    }

    labels = for (gl <- labels.groupBy(_.vertex)) yield
      Label(gl.key, gl.values.maxBy(_.prob < _.prob).get.label, 1.0)

    write(output, new CSVOutputFormat[Label]) { labels }
    labels
  }
}

object LabelRank {
  class Command extends Algorithm.Command[LabelRank] {
    import Command._

    override def name = "lr"

    override def description =
      "Detect communities in a (social) graph based on label propagation."

    override def setup(parser: Subparser) = {
      super.setup(parser)

      parser.addArgument(keyInput)
        .`type`(classOf[String])
        .dest(keyInput)
        .metavar("INPUT")
        .help("graph edges")

      parser.addArgument(keyOutput)
        .`type`(classOf[String])
        .dest(keyOutput)
        .metavar("OUTPUT")
        .help("labelled vertices")

      parser.addArgument(keyOscillations)
        .`type`(classOf[Int])
        .dest(keyOscillations)
        .metavar("MAX_OSCILLATIONS")
        .help("maximum number of oscillations if not converging")

      parser.addArgument(keyInflation)
        .`type`(classOf[Double])
        .dest(keyInflation)
        .metavar("INFLATION")
        .help("exponent to increase the difference between labels")

      parser.addArgument(keyCutoff)
        .`type`(classOf[Double])
        .dest(keyCutoff)
        .metavar("CUTOFF")
        .help("labels with probability below this are cut off")

      parser.addArgument(keySimilarity)
        .`type`(classOf[Double])
        .dest(keySimilarity)
        .metavar("SIMILARITY")
        .help("criteria for accepting updates to labels")
    }
  }

  object Command {
    val keyInput        = "input"
    val keyOutput       = "output"
    val keyOscillations = "max-oscillations"
    val keyInflation    = "inflation"
    val keyCutoff       = "cutoff"
    val keySimilarity   = "similarity"
  }

  object Schema {
    import Ordering.Implicits._
    
    type Vid = Long
    type Lid = Long

    case class Vertex(@id id: Vid, degree: Long)
        extends Identity[Vid] { def identity = id }

    case class Label(@id vertex: Vid, @id label: Lid, prob: Double)
        extends Identity[(Vid, Lid)] { def identity = vertex -> label }
    
    def mergeMax[K, V: Ordering](m1: Map[K, V], m2: Map[K, V]) = {
      val mm  = (m1 mergeWith m2) { case (_, v1, v2) => v1 max v2 }
      val max = mm.values.max
      mm filter { case (_, v) => v == max }
    }
    
    implicit class Merge[K, V](val self: Map[K, V]) extends AnyVal {
      def merge(that: Map[K, V]) =
        mergeWith(that) { case (_, _, v) => v }
      
      def mergeWith(that: Map[K, V])(f: (K, V, V) => V) =
        (that foldLeft self) { case (map, (k, v)) =>
          if (map contains k) map + (k -> f(k, map(k), v)) else map
        }
    }
  }
}
