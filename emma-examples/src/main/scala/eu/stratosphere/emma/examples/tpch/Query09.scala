package eu.stratosphere.emma.examples.tpch

import java.text.SimpleDateFormat
import java.util.Calendar

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.runtime.Engine
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}

object Query09 {

  object Command {
    // argument names
    val KEY_INPUT = "input"
    val KEY_OUTPUT = "output"
    val KEY_COLOR = "color"
  }

  class Command extends Algorithm.Command[Query09] {

    // algorithm names
    override def name = "tpch-q9"

    override def description = "TPC-H Query-9"

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
      parser.addArgument(Command.KEY_COLOR)
        .`type`[String](classOf[String])
        .dest(Command.KEY_COLOR)
        .metavar("COLOR")
        .help("color")
    }
  }

  object Schema {

    case class Subquery(nation: String, year: Integer, amount: Double) {}

    case class GrpKey(nation:String, year: Integer) {}

    case class Result(nation: String, year: Integer, sumProfit: Double) {}

  }

}

/**
 * Original query:
 *
 * {{{
 * select
 *    nation,
 *    o_year,
 *    sum(amount) as sum_profit
 * from (
 *    select
 *      n_name as nation,
 *      extract(year from o_orderdate) as o_year,
 *      l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
 *    from
 *      part,
 *      supplier,
 *      lineitem,
 *      partsupp,
 *      orders,
 *      nation
 *    where
 *      s_suppkey = l_suppkey
 *      and ps_suppkey = l_suppkey
 *      and ps_partkey = l_partkey
 *      and p_partkey = l_partkey
 *      and o_orderkey = l_orderkey
 *      and s_nationkey = n_nationkey
 *      and p_name like '%[COLOR]%'
 * ) as profit
 * group by
 *    nation,
 *    o_year
 * order by
 *    nation,
 *    o_year desc;
 * }}}
 *
 * @param inPath Base input path
 * @param outPath Output path
 * @param color Query parameter `COLR`
 */
class Query09(inPath: String, outPath: String, color: String, rt: Engine, val truncate: Boolean = false) extends Algorithm(rt) {

  import eu.stratosphere.emma.examples.tpch.Query09.Schema._

  def this(ns: Namespace, rt: Engine) = this(
    ns.get[String](Query09.Command.KEY_INPUT),
    ns.get[String](Query09.Command.KEY_OUTPUT),
    ns.get[String](Query09.Command.KEY_COLOR),
    rt)

  def run() = {

    val alg = emma.parallelize {

      // cannot directly reference the parameter
      val _truncate = truncate
      val _color = color

      val df = new SimpleDateFormat("yyyy-MM-dd")
      val calendar = Calendar.getInstance()

      val subquery = for (
        p <- read(s"$inPath/part.tbl", new CSVInputFormat[Part]('|'));  if p.name.indexOf(_color) != -1;
        s <- read(s"$inPath/supplier.tbl", new CSVInputFormat[Supplier]('|'));
        l <- read(s"$inPath/lineitem.tbl", new CSVInputFormat[Lineitem]('|')); if s.suppKey == l.suppKey; if p.partKey == l.partKey;
        ps <- read(s"$inPath/partsupp.tbl", new CSVInputFormat[PartSupp]('|')); if ps.suppKey == l.suppKey; if ps.partKey == l.partKey;
        o <- read(s"$inPath/orders.tbl", new CSVInputFormat[Order]('|')); if o.orderKey == l.orderKey;
        n <- read(s"$inPath/nation.tbl", new CSVInputFormat[Nation]('|')); if s.nationKey == n.nationKey
      )
      yield {
        calendar.setTime(df.parse(o.orderDate))
        new Subquery(n.name, calendar.get(Calendar.YEAR), amount = l.extendedPrice * (1 - l.discount) - ps.supplyCost * l.quantity)
      }

      // aggregate and compute the final result
      val rslt = for (
        g <- subquery.groupBy(x => new GrpKey(x.nation, x.year)))
      yield {
        def tr(v: Double) = if (_truncate) BigDecimal(v).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble else v

        new Result(
          g.key.nation,
          g.key.year,
          tr(g.values.map(x => x.amount).sum())
        )
      }

      // write out the result
      write(outPath, new CSVOutputFormat[Result]('|'))(rslt)
    }

    alg.run(rt)
  }
}

