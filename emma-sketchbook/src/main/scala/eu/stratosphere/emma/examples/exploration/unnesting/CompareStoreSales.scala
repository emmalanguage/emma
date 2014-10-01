package eu.stratosphere.emma.examples.exploration.unnesting

import java.util.Date

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.api.model._
import eu.stratosphere.emma.examples.Algorithm
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}

object CompareStoreSales {

  object Command {
    // argument names
    val KEY_SALES_L = "sales.l"
    val KEY_SALES_R = "sales.r"
    val KEY_OUTPUT = "output"
  }

  class Command extends Algorithm.Command[CompareStoreSales] {

    // algorithm names
    override def name = "compare-sales"

    override def description = "Compare sales of two stores"

    override def setup(parser: Subparser) = {
      // basic setup
      super.setup(parser)

      // add arguments
      parser.addArgument(Command.KEY_SALES_L)
        .`type`[String](classOf[String])
        .dest(Command.KEY_SALES_L)
        .metavar("SALES 1")
        .help("sales file")
      parser.addArgument(Command.KEY_SALES_R)
        .`type`[String](classOf[String])
        .dest(Command.KEY_SALES_R)
        .metavar("SALES 2")
        .help("sales file")
      parser.addArgument(Command.KEY_OUTPUT)
        .`type`[String](classOf[String])
        .dest(Command.KEY_OUTPUT)
        .metavar("OUTPUT")
        .help("output file ")
    }
  }

  // --------------------------------------------------------------------------------------------
  // ----------------------------------- Schema -------------------------------------------------
  // --------------------------------------------------------------------------------------------

  object Schema {

    case class Store(@id id: Long, name: String, area: Long) {}

    case class Product(@id id: Long, name: String, price: Double) {}

    case class SalesHistory(store: Store, date: Date, product: Product, count: Int) {}

    case class GroupKey(store: Store, date: Date) {}

    case class SalesBalance(storeL: Store, storeR: Store, date: Date, balance: Double) {}

  }

}

class CompareStoreSales(salesLUrl: String, salesRUrl: String, outputUrl: String, rt: runtime.Engine) extends Algorithm(rt) {

  def this(ns: Namespace, rt: runtime.Engine) = this(
    ns.get[String](CompareStoreSales.Command.KEY_SALES_L),
    ns.get[String](CompareStoreSales.Command.KEY_SALES_R),
    ns.get[String](CompareStoreSales.Command.KEY_OUTPUT),
    rt)


  def run() = {

    import eu.stratosphere.emma.examples.exploration.unnesting.CompareStoreSales.Schema._

    val algorithm = /* workflow */ {

      val salesL = read(salesLUrl, new InputFormat[SalesHistory])
      val salesR = read(salesRUrl, new InputFormat[SalesHistory])

      val comparison = for (l <- salesL.groupBy(x => GroupKey(x.store, x.date));
                            r <- salesR.groupBy(x => GroupKey(x.store, x.date));
                            if l.key.store.area == r.key.store.area && l.key.date == r.key.date) yield {

        val balance = for (entryL <- l.values;
                           entryR <- r.values;
                           if entryL.product == entryR.product) yield entryL.count * entryL.product.price - entryR.count * entryR.product.price

        SalesBalance(l.key.store, r.key.store, l.key.date, balance.sum())
      }

      write(outputUrl, new OutputFormat[SalesBalance])(comparison)
    }

    // algorithm.run(rt)
  }
}

