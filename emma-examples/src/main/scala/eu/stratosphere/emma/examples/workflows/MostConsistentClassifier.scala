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
    val numEmails = emails.count().toDouble

    // term frequency
    val tf = for {
      Email(id, _, content) <- emails
      (term, count) <- DataBag(wordCount(content).toSeq)
    } yield (id, term, 1 + math.log(count))

    val idf = for (byTerm <- tf.groupBy { case (_, term, _) => term })
      yield byTerm.key -> math.log(numEmails / byTerm.values.count())

    val tfIdf = for {
      (doc, term, freq) <- tf
      (word, inverse) <- idf
      if word == term
    } yield (doc, term, freq * inverse)

    val features = for (byDoc <- tfIdf.groupBy { case (doc, _, _) => doc }) yield {
      val bag = byDoc.values.map { case (_, term, freq) => term -> freq }
      byDoc.key -> featureHash(bag.fetch())
    }

    // 2) blacklisted servers
    val blacklist = for {
      server <- read(s"$input/servers", new CSVInputFormat[Server])
      if server.isBlacklisted
    } yield server

    // 3) spam classifiers
    val classifiers = for (model <- 1 to numClassifiers) yield {
      val bag = read(s"$input/classifier-$model", new CSVInputFormat[(String, Double)])
      Classifier(model, featureHash(bag.fetch()))
    }

    // for each different classifier
    val modelsWithPrecision = for (classifier <- classifiers) yield {
      val spam = for {
        (doc, vec) <- features
        if classifier.predict(vec)
      } yield doc

      // spam emails coming from a blacklisted server are considered consistent
      val consistent = for {
        Email(id, ip, _) <- emails
        doc <- spam
        if id == doc
        if blacklist.exists { _.ip == ip }
      } yield doc

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
  type EmailId = String
  type ModelId = Long
  type FeatureVector = SparseVector[Double]

  val threshold = 0.5

  case class Email(id: EmailId, ip: IP, content: String)
  case class Server(ip: IP, isBlacklisted: Boolean)
  case class Classifier(model: ModelId, weights: FeatureVector) {
    def predict(features: FeatureVector): Boolean =
      (features dot weights: Double) > threshold
  }

  def wordCount(text: String, minLength: Int = 3): Map[String, Int] =
    text.split(' ').toVector
      .map { _.replaceAll("""\P{Alnum}""", "").toLowerCase }
      .filter { _.length >= minLength }
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