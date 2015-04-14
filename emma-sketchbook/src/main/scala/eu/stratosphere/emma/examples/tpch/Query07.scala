package eu.stratosphere.emma.examples.tpch

import java.text.SimpleDateFormat
import java.util.Calendar

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.runtime.Engine
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}

object Query07 {

  object Command {
    // argument names
    val KEY_INPUT = "input"
    val KEY_OUTPUT = "output"
    val KEY_NATION1 = "nation1"
    val KEY_NATION2 = "nation2"
  }

  class Command extends Algorithm.Command[Query07] {

    // algorithm names
    override def name = "tpch-q7"

    override def description = "TPC-H Query-7"

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
      parser.addArgument(Command.KEY_NATION1)
        .`type`[String](classOf[String])
        .dest(Command.KEY_NATION1)
        .metavar("NATION1")
        .help("nation1")
      parser.addArgument(Command.KEY_NATION2)
        .`type`[String](classOf[String])
        .dest(Command.KEY_NATION2)
        .metavar("NATION2")
        .help("nation2")
    }
  }

  object Schema {

    case class GrpKey(suppNation: String, custNation: String, year: Integer) {}

    case class Subquery(suppNation: String, custNation: String, year: Integer, volume: Double) {}

    case class Result(suppNation: String, custNation: String, year: Integer, revenue: Double) {}
  }
}

/**
 * Original query:
 *
 * {{{
 * select
 *    supp_nation,
 *    cust_nation,
 *    l_year, sum(volume) as revenue
 * from (
 *    select
 *      n1.n_name as supp_nation,
 *      n2.n_name as cust_nation,
 *      extract(year from l_shipdate) as l_year,
 *      l_extendedprice * (1 - l_discount) as volume
 *    from
 *      supplier,
 *      lineitem,
 *      orders,
 *      customer,
 *      nation n1,
 *      nation n2
 *    where
 *      s_suppkey = l_suppkey
 *      and o_orderkey = l_orderkey
 *      and c_custkey = o_custkey
 *      and s_nationkey = n1.n_nationkey
 *      and c_nationkey = n2.n_nationkey
 *      and (
 *        (n1.n_name = '[NATION1]' and n2.n_name = '[NATION2]')
 *        or (n1.n_name = '[NATION2]' and n2.n_name = '[NATION1]')
 *      )
 *      and l_shipdate between date '1995-01-01' and date '1996-12-31'
 * ) as shipping
 * group by
 *    supp_nation,
 *    cust_nation,
 *    l_year
 * order by
 *    supp_nation,
 *    cust_nation,
 *    l_year;
 * }}}
 *
 * @param inPath Base input path
 * @param outPath Output path
 * @param nation1 Query parameter `NATION1`
 * @param nation2 Query parameter `NATION2`
 */
class Query07(inPath: String, outPath: String, nation1: String, nation2: String, rt: Engine, val truncate: Boolean = false) extends Algorithm(rt) {

  import eu.stratosphere.emma.examples.tpch.Query07.Schema._

  def this(ns: Namespace, rt: Engine) = this(
    ns.get[String](Query07.Command.KEY_INPUT),
    ns.get[String](Query07.Command.KEY_OUTPUT),
    ns.get[String](Query07.Command.KEY_NATION1),
    ns.get[String](Query07.Command.KEY_NATION2),
    rt)

  def run() = {

    val alg = emma.parallelize {

      // cannot directly reference the parameter
      val _truncate = truncate
      val _nation1 = nation1
      val _nation2 = nation2

      val df = new SimpleDateFormat("yyyy-MM-dd")
      val calendar = Calendar.getInstance()



      val subquery = for (
        s <- read(s"$inPath/supplier.tbl", new CSVInputFormat[Supplier]('|'));
        l <- read(s"$inPath/lineitem.tbl", new CSVInputFormat[Lineitem]('|')); if s.suppKey == l.suppKey; if l.shipDate >= "1995-01-01"; if l.shipDate <= "1996-12-31";
        o <- read(s"$inPath/orders.tbl", new CSVInputFormat[Order]('|')); if o.orderKey == l.orderKey;
        c <- read(s"$inPath/customer.tbl", new CSVInputFormat[Customer]('|')); if c.custKey == o.custKey;
        n1 <- read(s"$inPath/nation.tbl", new CSVInputFormat[Nation]('|')); if s.nationKey == n1.nationKey;
        n2 <- read(s"$inPath/nation.tbl", new CSVInputFormat[Nation]('|')); if c.nationKey == n2.nationKey;
        if ((n1.name == _nation1 && n2.name == _nation2) || (n1.name == _nation2 && n2.name == _nation1))
      )
      yield {
        calendar.setTime(df.parse(l.shipDate))
        new Subquery(n1.name, n2.name, calendar.get(Calendar.YEAR), l.extendedPrice * (1 - l.discount))
      }
      // aggregate and compute the final result

      val rslt = for (
        g <- subquery.groupBy(x => new GrpKey(x.suppNation, x.custNation, x.year)))
      yield {
        def tr(v: Double) = if (_truncate) BigDecimal(v).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble else v

        new Result(
          g.key.suppNation,
          g.key.custNation,
          g.key.year,
          tr(g.values.map(x => x.volume).sum()))
      }

      // write out the result
      write(outPath, new CSVOutputFormat[Result]('|'))(rslt)
    }

    alg.run(rt)
  }
}

