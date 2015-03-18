package eu.stratosphere.emma.examples.tpch

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.runtime.Engine
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}

object Query03 {

  object Command {
    // argument names
    val KEY_INPUT = "input"
    val KEY_OUTPUT = "output"
    val KEY_SEGMENT = "segment"
    val KEY_DATE = "date"
  }

  class Command extends Algorithm.Command[Query03] {

    // algorithm names
    override def name = "tpch-q3"

    override def description = "TPC-H Query-3"

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
      parser.addArgument(Command.KEY_SEGMENT)
        .`type`[String](classOf[String])
        .dest(Command.KEY_SEGMENT)
        .metavar("SEGMENT")
        .help("segment")
      parser.addArgument(Command.KEY_DATE)
        .`type`[String](classOf[String])
        .dest(Command.KEY_DATE)
        .metavar("DATE")
        .help("date")
    }
  }

  object Schema {
    case class GrpKey(orderKey: Int, orderDate: String, shipPriority: Int) {}
    case class Join(orderKey: Int, extendedPrice: Double, discount: Double, orderDate: String, shipPriority: Int) {}
    case class Result(orderKey: Int, revenue: Double, orderDate: String, shipPriority: Int) {}
  }
}

/**
 * Original query:
 *
 * {{{
 * select
 *     l_orderkey,
 *     sum(l_extendedprice * (1 - l_discount)) as revenue,
 *     o_orderdate,
 *     o_shippriority
 * from
 *     customer,
 *     orders,
 *     lineitem
 * where
 *     c_mktsegment = ':SEGMENT'
 *     and c_custkey = o_custkey
 *     and l_orderkey = o_orderkey
 *     and o_orderdate < date ':DATE'
 *     and l_shipdate > date ':DATE'
 * group by
 *     l_orderkey,
 *     o_orderdate,
 *     o_shippriority
 * order by
 *     revenue desc,
 *     o_orderdate;
 * }}}
 *
 * @param inPath Base input path
 * @param outPath Output path
 * @param segment Query parameter `SEGMENT`
 * @param date Query parameter `DATE`
 */
class Query03(inPath: String, outPath: String, segment: String, date: String, rt: Engine, val truncate: Boolean = false) extends Algorithm(rt) {

  import eu.stratosphere.emma.examples.tpch.Query03.Schema._

  def this(ns: Namespace, rt: Engine) = this(
    ns.get[String](Query03.Command.KEY_INPUT),
    ns.get[String](Query03.Command.KEY_OUTPUT),
    ns.get[String](Query03.Command.KEY_SEGMENT),
    ns.get[String](Query03.Command.KEY_DATE),
    rt)

  def run() = {

    val alg = emma.parallelize {

      // FIXME: cannot directly reference enclosing class parameters
      val _truncate = truncate
      val _date = date
      val _segment = segment

      // compute join part of the query
      val join = for (
        c <- read(s"$inPath/customer.tbl", new CSVInputFormat[Customer]('|')); if c.mktSegment == _segment;
        o <- read(s"$inPath/orders.tbl", new CSVInputFormat[Order]('|')); if o.orderDate < _date; if c.custKey == o.custKey;
        l <- read(s"$inPath/lineitem.tbl", new CSVInputFormat[Lineitem]('|')); if l.shipDate > _date; if l.orderKey == o.orderKey)
      yield
        new Join(l.orderKey, l.extendedPrice, l.discount, o.orderDate, o.shipPriority)

      // aggregate and compute the final result
      val rslt = for (
        g <- join.groupBy(x => new GrpKey(x.orderKey, x.orderDate, x.shipPriority)))
      yield {

        def tr(v: Double) = if (_truncate) BigDecimal(v).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble else v

        new Result(
          g.key.orderKey,
          tr(g.values.map(x => x.extendedPrice * (1 - x.discount)).sum()),
          g.key.orderDate,
          g.key.shipPriority)
      }

      // write out the result
      write(outPath, new CSVOutputFormat[Result]('|'))(rslt)
    }

    alg.run(rt)
  }
}

