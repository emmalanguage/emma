/*
 * Copyright © 2014 TU Berlin (emma@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package eu.stratosphere.emma.examples.tpch

import java.text.SimpleDateFormat
import java.util.Calendar

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.runtime.Engine
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}

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

      val tr = (v: Double) =>
        if (truncate) BigDecimal(v).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble
        else v

      val dfm = new SimpleDateFormat("yyyy-MM-dd")
      val cal = Calendar.getInstance()
      cal.setTime(dfm.parse(date))
      cal.add(Calendar.YEAR, 1)
      val nextYear = dfm.format(cal.getTime)

      // compute join part of the query
      val join = for {
        c <- read(s"$inPath/customer.tbl", new CSVInputFormat[Customer]('|'))
        o <- read(s"$inPath/orders.tbl", new CSVInputFormat[Order]('|'))
        if o.orderDate >= date
        if o.orderDate < nextYear
        if c.custKey == o.custKey
        l <- read(s"$inPath/lineitem.tbl", new CSVInputFormat[Lineitem]('|'))
        if l.orderKey == o.orderKey
        s <- read(s"$inPath/supplier.tbl", new CSVInputFormat[Supplier]('|'))
        if l.suppKey == s.suppKey
        n <- read(s"$inPath/nation.tbl", new CSVInputFormat[Nation]('|'))
        if c.nationKey == s.nationKey
        if s.nationKey == n.nationKey
        r <- read(s"$inPath/region.tbl", new CSVInputFormat[Region]('|'))
        if r.name == region
        if n.regionKey == r.regionKey
      } yield Join(n.name, l.extendedPrice, l.discount)

      // aggregate and compute the final result
      val rslt = for {
        g <- join.groupBy(x => new GrpKey(x.name))
      } yield Result(
        g.key.name,
        tr(g.values.map(x => x.extendedPrice * (1 - x.discount)).sum))

      // write out the result
      write(outPath, new CSVOutputFormat[Result]('|'))(rslt)
    }

    alg.run(rt)
  }
}

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

