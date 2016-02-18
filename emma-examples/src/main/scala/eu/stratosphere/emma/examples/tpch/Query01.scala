package eu.stratosphere.emma.examples.tpch

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.runtime.Engine
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}


/**
 * Original query:
 *
 * {{{
 * select
 *     l_returnflag,
 *     l_linestatus,
 *     sum(l_quantity) as sum_qty,
 *     sum(l_extendedprice) as sum_base_price,
 *     sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
 *     sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
 *     avg(l_quantity) as avg_qty,
 *     avg(l_extendedprice) as avg_price,
 *     avg(l_discount) as avg_disc,
 *     count(*) as count_order
 * from
 *     lineitem
 * where
 *     l_shipdate <= date '1998-12-01' - interval ':DELTA' day (3)
 * group by
 *     l_returnflag,
 *     l_linestatus
 * order by
 *     l_returnflag,
 *     l_linestatus;
 * }}}
 *
 * @param inPath Base input path
 * @param outPath Output path
 * @param delta Query parameter `DELTA`
 */
class Query01(inPath: String, outPath: String, delta: Int, rt: Engine, truncate: Boolean = false) extends Algorithm(rt) {

  import eu.stratosphere.emma.examples.tpch.Query01.Schema._

  def this(ns: Namespace, rt: Engine) = this(
    ns.get[String](Query01.Command.KEY_INPUT),
    ns.get[String](Query01.Command.KEY_OUTPUT),
    ns.get[Int](Query01.Command.KEY_DELTA),
    rt)

  def run() = {

    val alg = emma.parallelize {

      val tr = (v: Double) =>
        if (truncate) BigDecimal(v).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble
        else v

      // compute join part of the query
      val l = for {
        l <- read(s"$inPath/lineitem.tbl", new CSVInputFormat[Lineitem]('|'))
        if l.shipDate <= "1996-12-01"
      } yield l

      // aggregate and compute the final result
      val r = for {
        g <- l.groupBy(l => new GrpKey(l.returnFlag, l.lineStatus))
      } yield {
          // compute base aggregates
          val sumQty = g.values.map(_.quantity).sum
          val sumBasePrice = g.values.map(_.extendedPrice).sum
          val sumDiscPrice = g.values.map(l => l.extendedPrice * (1 - l.discount)).sum
          val sumCharge = g.values.map(l => l.extendedPrice * (1 - l.discount) * (1 + l.tax)).sum
          val countOrder = g.values.size
          // compute result
          Result(
            g.key.returnFlag,
            g.key.lineStatus,
            sumQty,
            tr(sumBasePrice),
            tr(sumDiscPrice),
            tr(sumCharge),
            avgQty = sumQty / countOrder,
            avgPrice = tr(sumBasePrice / countOrder),
            avgDisc = tr(sumDiscPrice / countOrder),
            countOrder)
        }

      // write out the result
      write(outPath, new CSVOutputFormat[Result]('|'))(r)
    }

    alg.run(rt)
  }
}

object Query01 {

  class Command extends Algorithm.Command[Query01] {

    // algorithm names
    override def name = "tpch-q1"

    override def description = "TPC-H Query-1"

    override def setup(parser: Subparser) = {
      // basic setup
      super.setup(parser)

      // add arguments
      parser.addArgument(Command.KEY_INPUT)
        .`type`[String](classOf[String])
        .dest(Command.KEY_INPUT)
        .metavar("INPATH")
        .help("base input file")
      parser.addArgument(Command.KEY_OUTPUT)
        .`type`[String](classOf[String])
        .dest(Command.KEY_OUTPUT)
        .metavar("OUTPUT")
        .help("output file")
      parser.addArgument(Command.KEY_DELTA)
        .`type`[Int](classOf[Int])
        .dest(Command.KEY_DELTA)
        .metavar("DELTA")
        .help("delta")
    }
  }

  object Command {
    // argument names
    val KEY_INPUT = "input"
    val KEY_OUTPUT = "output"
    val KEY_DELTA = "delta"
  }

  object Schema {

    case class GrpKey(
                       returnFlag: String,
                       lineStatus: String) {}

    case class Result(
                       returnFlag: String,
                       lineStatus: String,
                       sumQty: Int,
                       sumBasePrice: Double,
                       sumDiscPrice: Double,
                       sumCharge: Double,
                       avgQty: Double,
                       avgPrice: Double,
                       avgDisc: Double,
                       countOrder: Long) {}

  }

}

