package eu.stratosphere.emma.examples.tpch

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.runtime.Engine
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}

/**
  * Created by Clemens Lutz on 23.11.15.
  */

/**
  * Original query
  *
  * SELECT
  *   c_custkey,
  *   c_name,
  *   SUM(l_extendedprice * (1 - l_discount)) AS revenue,
  *   c_acctbal,
  *   n_name,
  *   c_address,
  *   c_phone,
  *   c_comment
  * FROM
  *   customer,
  *   orders,
  *   lineitem,
  *   nation
  * WHERE
  *   c_custkey = o_custkey
  *   AND l_orderkey = o_orderkey
  *   AND o_orderdate >= date '[DATE]'
  *   AND o_orderdate < date '[DATE]' + interval '3' month
  *   AND l_returnflag = 'R'
  *   AND c_nationkey = n_nationkey
  * GROUP BY
  *   c_custkey,
  *   c_name,
  *   c_acctbal,
  *   c_phone,
  *   n_name,
  *   c_address,
  *   c_comment
  * ORDER BY
  *   revenue desc;
  */

class Query10(inPath: String, outPath: String, begin_date: String, end_date: String, rt: Engine, val truncate: Boolean = false) extends Algorithm(rt) {

  import eu.stratosphere.emma.examples.tpch.Query10.Schema._

  def this(ns: Namespace, rt: Engine) = this(
    ns.get[String](Query10.Command.KEY_INPUT),
    ns.get[String](Query10.Command.KEY_OUTPUT),
    ns.get[String](Query10.Command.KEY_BEGIN_DATE),
    ns.get[String](Query10.Command.KEY_END_DATE),
    rt)

  def run() = {

    val alg = emma.parallelize {

      // compute join part of the query
      val join = for (
        o <- read(s"$inPath/orders.tbl", new CSVInputFormat[Order]('|'))
        if begin_date <= o.orderDate && o.orderDate < end_date;
        c <- read(s"$inPath/customer.tbl", new CSVInputFormat[Customer]('|'))
        if c.custKey == o.custKey;
        l <- read(s"$inPath/lineitem.tbl", new CSVInputFormat[Lineitem]('|'))
        if l.returnFlag == "R" && o.orderKey == l.orderKey;
        n <- read(s"$inPath/nation.tbl", new CSVInputFormat[Nation]('|'))
        if c.nationKey == n.nationKey
      )
        yield new Join(c.custKey, c.name, l.extendedPrice, c.accBal, l.discount, n.name, c.address, c.phone, c.comment)

      // aggregate and compute the final result
      val res = for (
        g <- join.groupBy(j => new GrpKey(j.c_custkey, j.c_name, j.c_acctbal, j.customerPhone, j.n_name, j.customerAddress, j.customerComment))
      )
        yield {
          def tr(v: Double) = if (truncate) BigDecimal(v).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble else v

          new Result(
            g.key.c_custkey,
            g.key.c_name,
            tr(g.values.map(x => x.extendedPrice * (1 - x.l_discount)).sum()),
            g.key.c_acctbal,
            g.key.n_name,
            g.key.c_address,
            g.key.c_phone,
            g.key.c_comment
          )
        }

      // write out the result
      write(outPath, new CSVOutputFormat[Result]('|'))(res)
    }
    alg.run(rt)

  }
}


object Query10 extends Algorithm.Command[Query10] {

  // Algorithm key
  override def name = "tpch-q10"

  // Algorithm name
  override def description = "TPC-H Query-10"

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
    parser.addArgument(Command.KEY_BEGIN_DATE)
      .`type`[String](classOf[String])
      .dest(Command.KEY_BEGIN_DATE)
      .metavar("BEGIN_DATE")
      .help("begin date")
    parser.addArgument(Command.KEY_END_DATE)
      .`type`[String](classOf[String])
      .dest(Command.KEY_END_DATE)
      .metavar("END_DATE")
      .help("end date")
  }

  object Command {
    // argument names
    val KEY_INPUT = "input"
    val KEY_OUTPUT = "output"
    val KEY_BEGIN_DATE = "begin_date"
    val KEY_END_DATE = "end_date"
  }

  object Schema {

    case class Join(
                     c_custkey: Int,
                     c_name: String,
                     extendedPrice: Double,
                     c_acctbal: Double,
                     l_discount: Double,
                     n_name: String,
                     customerAddress: String,
                     customerPhone: String,
                     customerComment: String
                   )

    case class GrpKey(
                     c_custkey: Int,
                     c_name: String,
                     c_acctbal: Double,
                     c_phone: String,
                     n_name: String,
                     c_address: String,
                     c_comment: String
                     ) {}

    case class Result(
                       c_custKey: Int,
                       c_name: String,
                       revenue: Double,
                       c_acctbal: Double,
                       n_name: String,
                       c_address: String,
                       c_phone: String,
                       c_comment: String
                     ) {}
  }
}