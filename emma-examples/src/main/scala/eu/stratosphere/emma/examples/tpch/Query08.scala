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
 *    o_year,
 *    sum(case
 *      when nation = '[NATION]'
 *      then volume
 *      else 0
 *     end) / sum(volume) as mkt_share
 * from (
 *    select
 *      extract(year from o_orderdate) as o_year,
 *      l_extendedprice * (1-l_discount) as volume,
 *      n2.n_name as nation
 *    from
 *      part,
 *      supplier,
 *      lineitem,
 *      orders,
 *      customer,
 *      nation n1,
 *      nation n2,
 *      region
 *    where
 *      p_partkey = l_partkey
 *      and s_suppkey = l_suppkey
 *      and l_orderkey = o_orderkey
 *      and o_custkey = c_custkey
 *      and c_nationkey = n1.n_nationkey
 *      and n1.n_regionkey = r_regionkey
 *      and r_name = '[REGION]'
 *      and s_nationkey = n2.n_nationkey
 *      and o_orderdate between date '1995-01-01' and date '1996-12-31'
 *      and p_type = '[TYPE]'
 * ) as all_nations
 * group by
 *    o_year
 * order by
 *    o_year;
 * }}}
 *
 * @param inPath Base input path
 * @param outPath Output path
 * @param nation Query parameter `NATION`
 * @param region Query parameter `REGION`
 * @param typeString Query parameter `TYPE`
 */
class Query08(inPath: String, outPath: String, nation: String, region: String, typeString: String, rt: Engine, val truncate: Boolean = false) extends Algorithm(rt) {

  import eu.stratosphere.emma.examples.tpch.Query08.Schema._

  def this(ns: Namespace, rt: Engine) = this(
    ns.get[String](Query08.Command.KEY_INPUT),
    ns.get[String](Query08.Command.KEY_OUTPUT),
    ns.get[String](Query08.Command.KEY_NATION),
    ns.get[String](Query08.Command.KEY_REGION),
    ns.get[String](Query08.Command.KEY_TYPE),
    rt)

  def run() = {

    val alg = emma.parallelize {

      val tr = (v: Double) =>
        if (truncate) BigDecimal(v).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble
        else v

      val df = new SimpleDateFormat("yyyy-MM-dd")
      val calendar = Calendar.getInstance()

      val subquery = for {
        p  <- read(s"$inPath/part.tbl", new CSVInputFormat[Part]('|'))
        if p.ptype == typeString
        s  <- read(s"$inPath/supplier.tbl", new CSVInputFormat[Supplier]('|'))
        l  <- read(s"$inPath/lineitem.tbl", new CSVInputFormat[Lineitem]('|'))
        if s.suppKey == l.suppKey
        if p.partKey == l.partKey
        o  <- read(s"$inPath/orders.tbl", new CSVInputFormat[Order]('|'))
        if o.orderDate >= "1995-01-01"
        if o.orderDate <= "1996-12-31"
        if l.orderKey == o.orderKey
        c  <- read(s"$inPath/customer.tbl", new CSVInputFormat[Customer]('|'))
        if o.custKey == c.custKey
        n1 <- read(s"$inPath/nation.tbl", new CSVInputFormat[Nation]('|'))
        if c.nationKey == n1.nationKey
        n2 <- read(s"$inPath/nation.tbl", new CSVInputFormat[Nation]('|'))
        if s.nationKey == n2.nationKey
        r  <- read(s"$inPath/region.tbl", new CSVInputFormat[Region]('|'))
        if r.name == region
        if n1.regionKey == r.regionKey
      } yield {
        calendar.setTime(df.parse(o.orderDate))
        Subquery(calendar.get(Calendar.YEAR), l.extendedPrice * (1 - l.discount), n2.name, nation)
      }

      // aggregate and compute the final result
      val rslt = for {
        g <- subquery.groupBy(x => new GrpKey(x.year))
      } yield Result(
        g.key.year,
        tr(g.values.map(x => if (x.nation == x.nationVariable) x.volume else 0).sum /
            g.values.map(x => x.volume).sum))

      // write out the result
      write(outPath, new CSVOutputFormat[Result]('|'))(rslt)
    }

    alg.run(rt)
  }
}

object Query08 {

  object Command {
    // argument names
    val KEY_INPUT = "input"
    val KEY_OUTPUT = "output"
    val KEY_NATION = "nation"
    val KEY_REGION = "region"
    val KEY_TYPE = "type"
  }

  class Command extends Algorithm.Command[Query08] {

    // algorithm names
    override def name = "tpch-q8"

    override def description = "TPC-H Query-8"

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
      parser.addArgument(Command.KEY_NATION)
        .`type`[String](classOf[String])
        .dest(Command.KEY_NATION)
        .metavar("NATION")
        .help("nation")
      parser.addArgument(Command.KEY_REGION)
        .`type`[String](classOf[String])
        .dest(Command.KEY_REGION)
        .metavar("REGION")
        .help("region")
      parser.addArgument(Command.KEY_TYPE)
        .`type`[String](classOf[String])
        .dest(Command.KEY_TYPE)
        .metavar("TYPE")
        .help("type")
    }
  }

  object Schema {

    case class GrpKey(year: Integer) {}

    case class Subquery(year: Integer, volume: Double, nation: String, nationVariable: String) {}

    case class Result(year: Integer, mkt_share: Double) {}
  }
}

