package eu.stratosphere.emma.examples.graphs

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.api.model._
import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.runtime.Engine

import net.sourceforge.argparse4j.inf.{ Namespace, Subparser }

class BeliefPropagation(
      input:         String,
      output:        String,
      epsilon:       Double,
      maxIterations: Int,
      rt:            Engine)
    extends Algorithm(rt) {
  import eu.stratosphere.emma.examples.graphs.BeliefPropagation.Schema._

  def this(ns: Namespace, rt: Engine) = this(
    ns get BeliefPropagation.Command.keyInput,
    ns get BeliefPropagation.Command.keyOutput,
    ns get BeliefPropagation.Command.keyEpsilon,
    ns get BeliefPropagation.Command.keyMaxIterations,
    rt)

  def run() = {
    val algorithm = /* emma.parallelize */ {
      // algorithm arguments
      val _epsilon       = epsilon
      val _maxIterations = maxIterations
      // read inputs
      val evidence  = read(s"$input/variables", new CSVInputFormat[Variable ])
      val potential = read(s"$input/potential", new CSVInputFormat[Potential])

      // aggregate and normalize variables
      val aggVars = for {
        gav <- evidence groupBy { _.identity }
      } yield {
        val (v, s) = gav.key
        val avg    = gav.values.map(_.prior).sum() / gav.values.count()
        Variable(v, s, avg)
      }

      val normVars = for {
        gnv <- aggVars groupBy { _.id }
      } yield gnv.key -> gnv.values.map(_.prior).sum()

      val variables = for {
        v <- aggVars
        n <- normVars
        if v.id == n._1
      } yield v.copy(prior = v.prior / n._2)

      // aggregate and normalize edges
      val outEdges = for {
        goe <- potential groupBy { _.undirectedId }
      } yield {
        val (v1, v2, s1, s2) = goe.key
        val avg = goe.values.map(_.prob).sum() / goe.values.count()
        Potential(v1, v2, s1, s2, avg)
      }

      val normEdges = for {
        gne <- outEdges groupBy { e => (e.var1, e.var2) }
      } yield gne.key -> gne.values.map(_.prob).sum()

      val normOut = for {
        e <- outEdges
        n <- normEdges
        if (e.var1, e.var2) == n._1
      } yield e.copy(prob = e.prob / n._2)

      val normIn = for {
        Potential(v1, v2, s1, s2, p) <- normOut
      } yield Potential(v2, v1, s2, s1, p)

      val edges = normIn plus normOut

      val beliefs = stateful[Belief, (VarId, StateId)] {
        for (Variable(v, s, p) <- variables) yield Belief(v, s, p)
      }

      val messages = stateful[Message, (VarId, VarId, StateId)] {
        (for (Potential(v1, v2, _, s2, _) <- edges)
          yield Message(v1, v2, s2, 1)).distinct()
      }

      var iterations = 0
      var converged  = false
      while (iterations < _maxIterations && !converged) {
        iterations += 1

        beliefs.updateWith(messages.bag())(
          m       => (m.dst, m.state),
          (b, ms) => {
            b.previous = b.marginal
            b.marginal = ms.map(_.prob).product()
            DataBag()
          })

        beliefs.updateWith(variables) { (b, v) =>
          b.marginal *= v.prior
          DataBag()
        }

        val normBeliefs = for {
          gnb <- beliefs.bag() groupBy { _.variable }
        } yield gnb.key -> gnb.values.map(_.marginal).sum()

        val updates = for {
          b <- beliefs.bag()
          n <- normBeliefs
          if b.variable == n._1
        } yield b.copy(marginal = n._2)

        beliefs.updateWith(updates) { (b, u) =>
          b.marginal /= u.marginal
          DataBag()
        }

        converged = beliefs.bag() forall { b =>
          (b.marginal - b.previous).abs < _epsilon
        }

        val products = for {
          gp <- messages.bag() groupBy { m => (m.dst, m.state) }
        } yield {
          val (v, s) = gp.key
          val prob   = gp.values.map(_.prob).product()
          Belief(v, s, prob)
        }

        messages.updateWith(edges)(
          e       => (e.var1, e.var2, e.state2),
          (m, es) => {
            m.prob = (for {
              e <- es
              v <- variables
              if v.identity == (e.var1, e.state1)
              p <- products
              if p.identity == (e.var1, e.state1)
              m <- messages.bag()
              if m.identity == (e.var2, e.var1, e.state1)
            } yield {
              v.prior * e.prob * p.marginal / m.prob
            }).sum()

            DataBag()
          })
      }

      val marginals = for (Belief(v, s, p, _) <- beliefs.bag())
        yield Variable(v, s, p)

      write(output, new CSVOutputFormat[Variable]) { marginals }
    }

//    algorithm.run(rt)
  }
}

object BeliefPropagation {

  class Command extends Algorithm.Command[BeliefPropagation] {
    import Command._

    override def name = "bp"

    override def description =
      "Infer the approximate marginal probabilities of a pairwise MRF."

    override def setup(parser: Subparser) = {
      super.setup(parser)

      parser.addArgument(keyInput)
        .`type`(classOf[String])
        .dest(keyInput)
        .metavar("RATINGS")
        .help("ratings file")

      parser.addArgument(keyOutput)
        .`type`(classOf[String])
        .dest(keyOutput)
        .metavar("OUTPUT")
        .help("recommendations file")

      parser.addArgument(keyEpsilon)
        .`type`(classOf[Double])
        .dest(keyEpsilon)
        .metavar("EPSILON")
        .help("convergence threshold")

      parser.addArgument(keyMaxIterations)
        .`type`(classOf[Int])
        .dest(keyMaxIterations)
        .metavar("MAX_ITERATIONS")
        .help("maximal number of iterations")
    }
  }

  object Command {
    val keyInput         = "input"
    val keyOutput        = "output"
    val keyEpsilon       = "epsilon"
    val keyMaxIterations = "max-iterations"
  }

  object Schema {
    type VarId   = String
    type StateId = Short

    case class Variable(
        @id id:    VarId,
        @id state: StateId,
            prior: Double
    ) extends Identity[(VarId, StateId)] {
      def identity = (id, state)
    }

    case class Potential(
        @id var1:   VarId,
        @id var2:   VarId,
        @id state1: StateId,
        @id state2: StateId,
            prob:   Double
    ) extends Identity[(VarId, VarId, StateId, StateId)] {
      require(var1 != var2)

      def identity = (var1, var2, state1, state2)

      def undirectedId: (VarId, VarId, StateId, StateId) =
        if   (var1 <= var2) identity
        else (var2, var1, state2, state1)
    }

    case class Belief(
        @id variable: VarId,
        @id state:    StateId,
        var marginal: Double,
        var previous: Double = 0
    ) extends Identity[(VarId, StateId)] {
      def identity = (variable, state)
    }

    case class Message(
        @id src:   VarId,
        @id dst:   VarId,
        @id state: StateId,
        var prob:  Double
    ) extends Identity[(VarId, VarId, StateId)] {
      def identity = (src, dst, state)
    }
  }
}
