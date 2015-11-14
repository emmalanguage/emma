package eu.stratosphere.emma.examples.workflows

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.runtime.Engine

/**
  * Workflow from the original Emma paper (with slight modifications).
  *
  * From a list of given classifiers, finds the one which most consistently marks emails originating from a
  * blacklisted server as spam.
  *
  * Prints out the
  *
  * @param input Input path
  * @param rt The runtime to use as parallel co-processor
  */
class MostConsistentClassifier(
      input: String,
      rt:    Engine)
    extends Algorithm(rt) {
  import MostConsistentClassifier.{Classifier, Email, Model, Server, Features}

  def extractFeatures(email: String): Features = {
    email // TODO
  }

  val algorithm = emma.parallelize {
    // read and pre-process input data
    // 1) emails
    val emails = for {
      email <- read(s"$input/emails", new CSVInputFormat[Email])
    } yield email -> extractFeatures(email.text)
    // 2) blacklisted servers
    val blacklist = for {
      server <- read(s"$input/servers", new CSVInputFormat[Server])
      if server.isBlacklisted
    } yield server
    // 3) spam classifiers
    val spamClassifiers = for {
      (a, b) <- Seq((0.5, 0.5), (0.43, 32.0), (0.23, 79.0))
    } yield Classifier(Model(a, b))

    // for each different classifier
    val classifiersWithHits = for (classifier <- spamClassifiers) yield {
      // find out emails classified as spam
      val spam = for {
        (email, features) <- emails
        if classifier predict features // TODO
      } yield email

      // spam emails coming from a blacklisted server are considered consistent
      val consistent = for {
        email <- spam
        if blacklist exists (_.ip == email.ip)
      } yield email

      // calculate the classification precision with respect to the blacklist
      val precision = consistent.count() / (spam.count() * 1.0)

      // output the
      classifier -> precision
    }

    // find the most consistent classifier
    val (Classifier(m), p) = classifiersWithHits maxBy { case (_, p) => p }
    println(
      s"""
         |Most consistent classifier found:
         | - model     = $m
         | - precision = $p
       """.stripMargin)
  }

  def run() = algorithm run rt
}

object MostConsistentClassifier {

  type Features = String

  case class Email(
    ip:             Int,
    text:           String
  )

  case class Server(
    ip:             Int,
    isBlacklisted:  Boolean
  )

  case class Classifier(model: Model) {
    def predict(features: Features) = false // TODO
  }

  case class Model(
    a: Double,
    b: Double)
}