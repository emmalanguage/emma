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

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.runtime.Engine
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}


/**
  * Original query:
  *
  * {{{
  * select
  *    s_acctbal,
  *    s_name,
  *    n_name,
  *    p_partkey,
  *    p_mfgr,
  *    s_address,
  *    s_phone,
  *    s_comment
  * from
  *    part,
  *    supplier,
  *    partsupp,
  *    nation,
  *    region
  * where
  *    p_partkey = ps_partkey
  *    and s_suppkey = ps_suppkey
  *    and p_size = [SIZE]
  *    and p_type like '%[TYPE]'
  *    and s_nationkey = n_nationkey
  *    and n_regionkey = r_regionkey
  *    and r_name = '[REGION]'
  *    and ps_supllycost = (
  *        select
  *            min(ps_supplycost)
  *        from
  *            partsupp,
  *            supplier,
  *            nation,
  *            region
  *        where
  *            p_partkey = ps_partkey
  *            and s_suppkey = ps_suppkey
  *            and s_nationkey = n_nationkey
  *            and n_regionkey = r_regionkey
  *            and r_name = '[REGION]'
  *    )
  * order by
  *    s_acctbal desc,
  *    n_name,
  *    s_name,
  *    p_partkey
  * }}}
  *
  * @param inPath Base input path
  * @param outPath Output path
  * @param size Query parameter `SIZE`
  * @param tpe Query parameter `TYPE`
  * @param region Query parameter `REGION`
  */

class Query02(inPath: String, outPath: String, size: Int, tpe: String, region: String, rt: Engine) extends Algorithm(rt) {

  import eu.stratosphere.emma.examples.tpch.Query02.Schema._

  def this(ns: Namespace, rt: Engine) = this(
    ns.get[String](Query02.Command.KEY_INPUT),
    ns.get[String](Query02.Command.KEY_OUTPUT),
    ns.get[Int](Query02.Command.KEY_SIZE),
    ns.get[String](Query02.Command.KEY_TYPE),
    ns.get[String](Query02.Command.KEY_REGION),
    rt)

  def run() = {

    val alg = emma.parallelize {

      // join tables + filter by region
      val join = for {
        ps <- read(s"$inPath/partsupp.tbl", new CSVInputFormat[PartSupp]('|'))
        s  <- read(s"$inPath/supplier.tbl", new CSVInputFormat[Supplier]('|'))
        if s.suppKey == ps.suppKey
        n  <- read(s"$inPath/nation.tbl", new CSVInputFormat[Nation]('|'))
        if s.nationKey == n.nationKey
        r  <- read(s"$inPath/region.tbl", new CSVInputFormat[Region]('|'))
        if r.name == region
        if n.regionKey == r.regionKey
      } yield (ps, s, n, r)

      // minimal supplier cost for region
      val minSupplierCost = join.map { case (ps, s, n, r) => ps.supplyCost }.min

      // apply filter on size, tpe and name + minimal supplier cost
      val result = for {
        (p          ) <- read(s"$inPath/part.tbl", new CSVInputFormat[Part]('|'))
        (ps, s, n, r) <- join
        if ps.supplyCost == minSupplierCost
        if p.partKey == ps.partKey
        if p.size == size
        if p.ptype.endsWith(tpe)
      } yield Result(
        s.accBal,
        s.name,
        n.name,
        p.partKey,
        p.mfgr,
        s.address,
        s.phone,
        s.comment)

      // write out the result
      write(outPath, new CSVOutputFormat[Result]('|'))(result)
    }

    alg.run(rt)
  }
}

object Query02 {

  object Command {
    // argument names
    val KEY_INPUT = "input"
    val KEY_OUTPUT = "output"
    val KEY_SIZE = "size"
    val KEY_TYPE = "type"
    val KEY_REGION = "region"
  }

  class Command extends Algorithm.Command[Query02] {

    // algorithm names
    override def name = "tpch-q2"

    override def description = "TPC-H Query-2"

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
      parser.addArgument(Command.KEY_SIZE)
        .`type`[Int](classOf[Int])
        .dest(Command.KEY_SIZE)
        .metavar("SIZE")
        .help("size")
      parser.addArgument(Command.KEY_TYPE)
        .`type`[String](classOf[String])
        .dest(Command.KEY_TYPE)
        .metavar("TYPE")
        .help("type")
      parser.addArgument(Command.KEY_REGION)
        .`type`[String](classOf[String])
        .dest(Command.KEY_REGION)
        .metavar("REGION")
        .help("region")
    }
  }

  object Schema {

    case class Result(
      acctbal: Double,
      supplierName: String,
      nationName: String,
      partKey: Int,
      partMfgr: String,
      supplierAddress: String,
      supplierPhone: String,
      supplierComment: String) {}

  }

}
