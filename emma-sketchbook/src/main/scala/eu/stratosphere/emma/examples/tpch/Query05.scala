package eu.stratosphere.emma.examples.tpch

import java.text.SimpleDateFormat

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.runtime.Engine
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}
import org.apache.commons.lang3.time.DateUtils

object Query05 {

  object Command {
    // argument names
    val KEY_INPUT = "input"
    val KEY_OUTPUT = "output"
    val KEY_REGION = "region"
    val KEY_DATE = "date"
  }

  class Command extends Algorithm.Command[Query05] {

    // algorithm names
    override def name = "tpch-q5"

    override def description = "TPC-H Query-5"

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
      parser.addArgument(Command.KEY_REGION)
        .`type`[String](classOf[String])
        .dest(Command.KEY_REGION)
        .metavar("REGION")
        .help("region")
      parser.addArgument(Command.KEY_DATE)
        .`type`[String](classOf[String])
        .dest(Command.KEY_DATE)
        .metavar("DATE")
        .help("date")
    }
  }

  object Schema {

    case class GrpKey(name: String) {}

    case class Join(name: String, extendedPrice: Double, discount: Double) {}

    case class Result(name: String, revenue: Double) {}

  }

}

/**
 * Original query:
 *
 * {{{
 * select
 *    n_name,
 *    sum(l_extendedprice * (1 - l_discount)) as revenue
 * from
 *    customer,
 *    orders,
 *    lineitem,
 *    supplier,
 *    nation,
 *    region
 * where
 *    c_custkey = o_custkey
 *    and l_orderkey = o_orderkey
 *    and l_suppkey = s_suppkey
 *    and c_nationkey = s_nationkey
 *    and s_nationkey = n_nationkey
 *    and n_regionkey = r_regionkey
 *    and r_name = '[REGION]'
 *    and o_orderdate >= date '[DATE]'
 *    and o_orderdate < date '[DATE]' + interval '1' year
 * group by
 *    n_name
 * order by
 *    revenue desc;
 * }}}
 *
 * @param inPath Base input path
 * @param outPath Output path
 * @param region Query parameter `REGION`
 * @param date Query parameter `DATE`
 */
class Query05(inPath: String, outPath: String, region: String, date: String, rt: Engine, val truncate: Boolean = false) extends Algorithm(rt) {

  import eu.stratosphere.emma.examples.tpch.Query05.Schema._

  def this(ns: Namespace, rt: Engine) = this(
    ns.get[String](Query05.Command.KEY_INPUT),
    ns.get[String](Query05.Command.KEY_OUTPUT),
    ns.get[String](Query05.Command.KEY_REGION),
    ns.get[String](Query05.Command.KEY_DATE),
    rt)

  def run() = {

    val alg = emma.parallelize {

      // FIXME: cannot directly reference enclosing class parameters
      val _truncate = truncate
      val _date = date
      val _region = region

      val df = new SimpleDateFormat("yyyy-MM-dd")
      val nextYear = df.format(DateUtils.addYears(df.parse(_date), 1))

      // compute join part of the query
      val join = for (
        c <- read(s"$inPath/customer.tbl", new CSVInputFormat[Customer]('|'));
        o <- read(s"$inPath/orders.tbl", new CSVInputFormat[Order]('|')); if c.custKey == o.custKey; if o.orderDate >= _date; if o.orderDate < nextYear;
        l <- read(s"$inPath/lineitem.tbl", new CSVInputFormat[Lineitem]('|')); if l.orderKey == o.orderKey;
        s <- read(s"$inPath/supplier.tbl", new CSVInputFormat[Supplier]('|')); if l.suppKey == s.suppKey; if c.nationKey == s.nationKey;
        n <- read(s"$inPath/nation.tbl", new CSVInputFormat[Nation]('|')); if s.nationKey == n.nationKey;
        r <- read(s"$inPath/region.tbl", new CSVInputFormat[Region]('|')); if n.regionKey == r.regionKey; if r.name == _region)
      yield
        new Join(n.name, l.extendedPrice, l.discount)

      // aggregate and compute the final result
      val rslt = for (
        g <- join.groupBy(x => new GrpKey(x.name)))
      yield {

        def tr(v: Double) = if (_truncate) BigDecimal(v).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble else v

        new Result(
          g.key.name,
          tr(g.values.map(x => x.extendedPrice * (1 - x.discount)).sum()))
      }

      // write out the result
      write(outPath, new CSVOutputFormat[Result]('|'))(rslt)
    }

    alg.run(rt)
  }
}

