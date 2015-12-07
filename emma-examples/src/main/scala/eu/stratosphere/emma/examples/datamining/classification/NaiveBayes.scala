package eu.stratosphere.emma.examples.datamining.classification

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.runtime.Engine
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}
import org.apache.spark.util.Vector

/**
 * Trains a Naive Bayes Classifier based on the input data set.
 * It support two model Types: Bernoulli and Multinomial
 *
 * @author Behrouz Derakhshan
 */
class NaiveBayes(training: String, lambda: Double, modelType: String, rt: Engine)
  extends Algorithm(rt) {

  import eu.stratosphere.emma.examples.datamining.classification.NaiveBayes.Schema._
  import eu.stratosphere.emma.examples.datamining.classification.NaiveBayes._

  def this(ns: Namespace, rt: Engine) = this(
    ns.get[String](NaiveBayes.Command.TRAINING),
    ns.get[Double](NaiveBayes.Command.LAMBDA),
    ns.get[String](NaiveBayes.Command.MODEL_TYPE),
    rt)

  val algorithm = emma.parallelize {
    
    val data = for (line <- read(training, new TextInputFormat[String]('\n'))) yield {
      val record = line.split(",").map(_.toDouble).toList
      LabeledVector(record.head, new Vector(record.slice(1, record.size).toArray))
    }

    // TODO: replace with take(n)
    val dimension = data.find { _ => true }.get.vector.length

    val aggregated = for (group <- data.groupBy(_.label)) yield {
      val count = group.values.count()
      val sum = group.values.fold(Vector.zeros(dimension))(_.vector, _ + _)
      (group.key, count, sum)
    }

    val numDocuments = data.size()
    val numLabels = aggregated.length().toInt

    val priorDenom = Math.log(numDocuments + numLabels * lambda)

    val model = for {(label, count, vecSum ) <- aggregated} yield {
      val priors = math.log(count + lambda) - priorDenom
      val evidenceDenom = modelType match {
        case MULTINOMIAL => math.log(vecSum.sum + lambda * dimension)
        case BERNOULI => math.log(count + 2.0 * lambda)
        case _ =>
          throw new UnknownError(s"Invalid modelType: $modelType.")
      }
      val evidences = Vector.apply(vecSum.elements.map(e => math.log(e + lambda) - evidenceDenom))
      (label, priors, evidences)
    }
    model
  }


  def run = algorithm.run(rt)
}

object NaiveBayes {
  final val MULTINOMIAL: String = "multinomial"
  final val BERNOULI: String = "bernoulli"

  object Command {
    // argument names
    val TRAINING = "training"
    val LAMBDA = "lambda"
    val MODEL_TYPE = "model_type"

  }

  class Command extends Algorithm.Command[NaiveBayes] {

    // algorithm names
    override def name = "naive-bayes"

    override def description = "naive bayes training"

    override def setup(parser: Subparser) = {
      // basic setup
      super.setup(parser)

      parser.addArgument(Command.LAMBDA)
        .`type`[Double](classOf[Double])
        .dest(Command.LAMBDA)
        .metavar("EPSILON")
        .help("termination threshold")

      parser.addArgument(Command.TRAINING)
        .`type`[String](classOf[String])
        .dest(Command.TRAINING)
        .metavar("INPUT")
        .help("training data set")

      parser.addArgument(Command.MODEL_TYPE)
        .`type`[String](classOf[String])
        .dest(Command.MODEL_TYPE)
        .metavar("MODEL TYPE")
        .help("model type (bernoulli, multinomial)")

    }
  }

  object Schema {

    case class LabeledVector(label: Double, vector: Vector)

  }

}
