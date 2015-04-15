package eu.stratosphere.emma.examples.tpch

import java.text.SimpleDateFormat

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.runtime.Engine
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}
import org.apache.commons.lang.time.DateUtils

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
 * @param date Query parameter `DATE`
 * @param discount Query parameter `DISCOUNT`
 * @param quantity Query parameter `QUANTITY`
 */
class Query06(inPath: String, outPath: String, date: String, discount: Double, quantity: Integer, rt: Engine, val truncate: Boolean = false) extends Algorithm(rt) {

  import eu.stratosphere.emma.examples.tpch.Query06.Schema._

  def this(ns: Namespace, rt: Engine) = this(
    ns.get[String](Query06.Command.KEY_INPUT),
    ns.get[String](Query06.Command.KEY_OUTPUT),
    ns.get[String](Query06.Command.KEY_DATE),
    ns.get[Double](Query06.Command.KEY_DISCOUNT),
    ns.get[Integer](Query06.Command.KEY_QUANTITY),
    rt)

  def run() = {

    val alg = emma.parallelize {

      // cannot directly reference the parameter
      val _truncate = truncate
      val _date = date
      val _discount = discount
      val _quantity = quantity

      val df = new SimpleDateFormat("yyyy-MM-dd")
      val nextYear = df.format(DateUtils.addYears(df.parse(_date), 1))

      val select = for (
        l <- read(s"$inPath/lineitem.tbl", new CSVInputFormat[Lineitem]('|'))
        if l.shipDate >= _date; if l.shipDate < nextYear; if l.discount >= (_discount-0.01); if l.discount <= (_discount+0.01); if l.quantity < _quantity)
      yield
        new Select(l.extendedPrice, l.discount)

      // aggregate and compute the final result
      val rslt = for (
        g <- select.groupBy(x => new GrpKey("*")))
      yield {
        def tr(v: Double) = if (_truncate) BigDecimal(v).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble else v
        new Result(
          tr(g.values.map(x => x.extendedPrice * x.discount).sum()))
      }

      // write out the result
      write(outPath, new CSVOutputFormat[Result]('|'))(rslt)
    }

    alg.run(rt)
  }
}

object Query06 {

  object Command {
    // argument names
    val KEY_INPUT = "input"
    val KEY_OUTPUT = "output"
    val KEY_DATE = "date"
    val KEY_DISCOUNT = "discount"
    val KEY_QUANTITY = "quantity"
  }

  class Command extends Algorithm.Command[Query06] {

    // algorithm names
    override def name = "tpch-q6"

    override def description = "TPC-H Query-6"

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
      parser.addArgument(Command.KEY_DATE)
        .`type`[String](classOf[String])
        .dest(Command.KEY_DATE)
        .metavar("DATE")
        .help("date")
      parser.addArgument(Command.KEY_DISCOUNT)
        .`type`[Double](classOf[Double])
        .dest(Command.KEY_DISCOUNT)
        .metavar("DISCOUNT")
        .help("discount")
      parser.addArgument(Command.KEY_QUANTITY)
        .`type`[Integer](classOf[Integer])
        .dest(Command.KEY_QUANTITY)
        .metavar("QUANTITY")
        .help("quantity")
    }
  }

  object Schema {

    case class GrpKey(name: String) {}

    case class Select(extendedPrice: Double, discount: Double) {}

    case class Result(revenue: Double) {}
  }

}

