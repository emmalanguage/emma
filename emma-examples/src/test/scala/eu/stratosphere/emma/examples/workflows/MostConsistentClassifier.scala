package eu.stratosphere.emma.examples.workflows

import breeze.linalg._

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.runtime.Engine

import scala.collection.mutable
import scala.util.hashing.MurmurHash3

/**
  * Workflow from the original Emma paper (with slight modifications).
  *
  * From a list of given classifiers, finds the one which most consistently marks emails originating
  * from a blacklisted server as spam.
  *
  * @param input Input path
  * @param rt The runtime to use as parallel co-processor
  */
class MostConsistentClassifier(input: String, numClassifiers: Int, rt: Engine)
    extends Algorithm(rt) {

  import MostConsistentClassifier._

  val algorithm = emma.parallelize {
    // read and pre-process input data
    // 1) emails
    val emails = read(s"$input/emails", new CSVInputFormat[Email])

    // term frequency
    val tf = for (email <- emails)
      yield email -> featureHash(wordCount(email.content).mapValues { _.toDouble })

    // 2) blacklisted servers
    val blacklist = for {
      server <- read(s"$input/servers", new CSVInputFormat[Server])
      if server.isBlacklisted
    } yield server

    // 3) spam classifiers
    val classifiers = for (model <- 1 to numClassifiers) yield {
      val features = read(s"$input/classifier-$model", new CSVInputFormat[(String, Double)])
      Classifier(model, featureHash(features.fetch()))
    }

    // for each different classifier
    val modelsWithPrecision = for (classifier <- classifiers) yield {
      val spam = for {
        (email, features) <- tf
        if classifier.predict(features)
      } yield email

      // spam emails coming from a blacklisted server are considered consistent
      val consistent = for {
        email <- spam
        if blacklist.exists { _.ip == email.ip }
      } yield email

      // calculate the classification precision with respect to the blacklist
      val precision = spam.count() / consistent.count().toDouble
      classifier.model -> precision
    }

    // find the most consistent model
    modelsWithPrecision.maxBy { case (_, p) => p }
  }

  def run() = algorithm.run(rt)
}

object MostConsistentClassifier {

  type IP = Int
  type Model = Long
  type FeatureVector = SparseVector[Double]

  case class Email(ip: IP, content: String)
  case class Server(ip: IP, isBlacklisted: Boolean)
  case class Classifier(model: Model, weights: FeatureVector) {
    def predict(features: FeatureVector): Boolean =
      (features.dot(weights): Double) > 0.5
  }

  val stopWords = Set("the", "i", "a", "an", "at", "are", "am", "for", "and", "or", "is", "there",
    "it", "this", "that", "on", "was", "by", "of", "to", "in", "to", "message", "not", "be", "with",
    "you", "have", "as", "can")

  def wordCount(text: String, minLength: Int = 3): Map[String, Int] =
    text.split(' ').toVector
      .map { _.replaceAll("""\W+""", "").toLowerCase }
      .filter { _.length >= minLength }
      .filterNot(stopWords.contains)
      .groupBy(identity)
      .mapValues { _.length }

  def featureHash(features: Iterable[(String, Double)], signed: Boolean = true): FeatureVector = {
    val hashed = new mutable.HashMap[Int, Double].withDefaultValue(0)
    for ((k, v) <- features) {
      val h = MurmurHash3.stringHash(k)
      hashed(h) = hashed(h) + { if (signed) h.signum * v else v }
    }

    SparseVector(hashed.size)(hashed.toSeq: _*)
  }
}