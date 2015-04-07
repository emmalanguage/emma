package eu.stratosphere.emma.examples.tpch.query07

import java.text.SimpleDateFormat
import java.util.Calendar

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.examples.tpch._
import eu.stratosphere.emma.runtime.Engine
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}
import org.apache.commons.lang3.time.DateUtils

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

}

/**
 * Original query:
 *
 * {{{
 * select
 *    sum(l_extendedprice*l_discount) as revenue
 * from
 *    lineitem
 *  where
 *    l_shipdate >= date '[DATE]'
 *    and l_shipdate < date '[DATE]' + interval '1' year
 *    and l_discount between [DISCOUNT] - 0.01 and [DISCOUNT] + 0.01
 *    and l_quantity < [QUANTITY];
 * }}}
 *
 * @param inPath Base input path
 * @param outPath Output path
 * @param nation1 Query parameter `NATION1`
 * @param nation2 Query parameter `NATION2`
 */
class Query07(inPath: String, outPath: String, nation1: String, nation2: String, rt: Engine, val truncate: Boolean = false) extends Algorithm(rt) {

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

