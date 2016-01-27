package eu.stratosphere.emma.examples.tpch

import java.text.SimpleDateFormat
import java.util.Calendar

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.runtime.Engine
import net.sourceforge.argparse4j.inf.{Subparser, Namespace}

/**
  * Original query:
  *
  * {{{
  * select
  *   l_shipmode,
  *   sum(case
  *     when o_orderpriority ='1-URGENT'
  *           or o_orderpriority ='2-HIGH'
  *       then 1
  *       else 0
  *     end) as high_line_count,
  *     sum(case
  *         when o_orderpriority <> '1-URGENT'
  *             and o_orderpriority <> '2-HIGH'
  *       then 1
  *       else 0
  *     end) as low_line_count
  * from
  *     orders,
  *     lineitem
  * where
  *     o_orderkey = l_orderkey
  *     and l_shipmode in (':SHIPMODE1', ':SHIPMODE2')
  *     and l_commitdate < l_receiptdate
  *     and l_shipdate < l_commitdate
  *     and l_receiptdate >= date ':DATE'
  *     and l_receiptdate < date ':DATE' + interval '1' year
  * group by
  *     l_shipmode
  * order by
  *     l_shipmode;
  * }}}
  *
  * @param inPath    Base input path
  * @param outPath   Output path
  * @param shipmode1 Query parameter `SHIPMODE1`
  * @param shipmode2 Query parameter `SHIPMODE2`
  * @param date      Query parameter `DATE`
  */
class Query12(inPath: String, outPath: String, shipmode1: String, shipmode2: String, date: String, rt: Engine) extends Algorithm(rt) {

  import eu.stratosphere.emma.examples.tpch.Query12.Schema._

  def this(ns: Namespace, rt: Engine) = this(
    ns.get[String](Query12.Command.KEY_INPUT),
    ns.get[String](Query12.Command.KEY_OUTPUT),
    ns.get[String](Query12.Command.KEY_SHIPMODE1),
    ns.get[String](Query12.Command.KEY_SHIPMODE2),
    ns.get[String](Query12.Command.KEY_DATE),
    rt
  )

  def run() = {
    val alg = emma.parallelize {

      val dfm = new SimpleDateFormat("yyyy-MM-dd")
      val cal = Calendar.getInstance();
      cal.setTime(dfm.parse(date));
      cal.add(Calendar.YEAR, 1);
      val nextYear = dfm.format(cal.getTime)

      val join = for {
        l <- read(s"$inPath/lineitem.tbl", new CSVInputFormat[Lineitem]('|'));
        if l.shipMode == shipmode1 || l.shipMode == shipmode2;
        if l.commitDate < l.receiptDate;
        if l.shipDate < l.commitDate;
        if l.receiptDate >= date;
        if l.receiptDate < nextYear;
        o <- read(s"$inPath/orders.tbl", new CSVInputFormat[Order]('|'));
        if o.orderKey == l.orderKey
      } yield
        Join(o.orderPriority, l.shipMode)

      val rslt = for (
        g <- join.groupBy(x => x.shipMode)
      ) yield {
        Result(g.key,
          g.values.count(x => x.orderPriority == "1-URGENT" || x.orderPriority == "2-HIGH"),
          g.values.count(x => x.orderPriority != "1-URGENT" && x.orderPriority != "2-HIGH")
        )
      }
      write(outPath, new CSVOutputFormat[Result]('|'))(rslt)
    }

    alg.run(rt)
  }
}

object Query12 {

  object Command {
    // argument names
    val KEY_INPUT = "input"
    val KEY_OUTPUT = "output"
    val KEY_SHIPMODE1 = "shipmode1"
    val KEY_SHIPMODE2 = "shipmode2"
    val KEY_DATE = "date"
  }

  class Command extends Algorithm.Command[Query12] {

    // algorithm names
    override def name = "tpch-q12"

    override def description = "TPC-H Query-12"

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
      parser.addArgument(Command.KEY_SHIPMODE1)
        .`type`[String](classOf[String])
        .dest(Command.KEY_SHIPMODE1)
        .metavar("SHIPMODE1")
        .help("shipmode1")
      parser.addArgument(Command.KEY_SHIPMODE2)
        .`type`[String](classOf[String])
        .dest(Command.KEY_SHIPMODE2)
        .metavar("SHIPMODE2")
        .help("shipmode2")
      parser.addArgument(Command.KEY_DATE)
        .`type`[String](classOf[String])
        .dest(Command.KEY_DATE)
        .metavar("DATE")
        .help("date")
    }
  }

  object Schema {
    case class Join(orderPriority: String, shipMode: String) {}

    case class Result(shipMode: String, highLineCount: Long, lowLineCount: Long) {}
  }

}


