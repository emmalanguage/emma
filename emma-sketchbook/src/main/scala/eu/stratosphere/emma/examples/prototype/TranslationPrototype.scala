package eu.stratosphere.emma.examples.prototype

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.runtime
import eu.stratosphere.emma.runtime.Engine
import org.apache.spark.util.Vector

import scala.util.Random

object TranslationPrototype {

  // --------------------------------------------------------------------------------------------
  // ----------------------------------- Schema -------------------------------------------------
  // --------------------------------------------------------------------------------------------

  object Schema {

    type VT = Int

    case class Edge(src: VT, dst: VT) {}

    case class EdgeWithLabel[LT](src: VT, dst: VT, label: LT) {}

  }

  /**
   * Temporary, only for debugging.
   *
   */
  def main(args: Array[String]): Unit = {
    val prototype = new TranslationPrototype(runtime.factory("flink-local", "localhost", 6123))
    prototype.run()
  }
}

class TranslationPrototype(rt: Engine) extends Algorithm(rt) {

  import eu.stratosphere.emma.examples.prototype.TranslationPrototype.Schema._
  import eu.stratosphere.emma.examples.util._

  def run() = edgesMapTest()

  private def edgesMapTestOnFlink() = {

    val a = 2
    val b = 2

    type Int = java.lang.Integer

    {
      import org.apache.flink.api.{java => fjava}
      import org.apache.flink.api.{common => fcommon}
      import org.apache.flink.api.common.{functions => ffns}
      import org.apache.flink.{core => fcore}

      val env = fjava.ExecutionEnvironment.createLocalEnvironment()

      // src
      val edges = {
        val inPath = new fcore.fs.Path(materializeResource("/graphs/dwg.csv"))
        val inputFormat = new fjava.io.CsvInputFormat[fjava.tuple.Tuple3[Int, Int, String]](inPath, "\n", '\t', classOf[Int], classOf[Int], classOf[String])
        val typeInformation = new fjava.typeutils.TupleTypeInfo[fjava.tuple.Tuple3[Int, Int, String]](
          fcommon.typeinfo.BasicTypeInfo.INT_TYPE_INFO,
          fcommon.typeinfo.BasicTypeInfo.INT_TYPE_INFO,
          fcommon.typeinfo.BasicTypeInfo.STRING_TYPE_INFO)
        env.createInput(inputFormat, typeInformation)
      }

      // map
      val result = edges.map(new ffns.MapFunction[fjava.tuple.Tuple3[Int, Int, String], fjava.tuple.Tuple3[Int, Int, String]] {
        override def map(x: fjava.tuple.Tuple3[Int, Int, String]): fjava.tuple.Tuple3[Int, Int, String] = {
          new fjava.tuple.Tuple3[Int, Int, String](a * x.f0, b * x.f1, x.f2)
        }
      })

      // sink
      {
        val outPath = new fcore.fs.Path(tempOutputPath("/graphs/duplicate_edges.csv"))
        val outputFormat = new fjava.io.CsvOutputFormat[fjava.tuple.Tuple3[Int, Int, String]](outPath)
        result.write(outputFormat, tempOutputPath("/graphs/duplicate_edges.csv"), fcore.fs.FileSystem.WriteMode.OVERWRITE)
      }

      env.execute()
    }

    //val x = algorithm.run(rt)
  }

  private def edgesMapTest() = {

    val a = 10
    val b = 5

    val test = true

    val algorithm = emma.parallelize {
      for (i <- Range(0, 1)) {
        val E = read(materializeResource("/graphs/dwg.csv"), new CSVInputFormat[EdgeWithLabel[String]])
        val A = for (edge <- E) yield EdgeWithLabel(a * edge.src, edge.dst, edge.label)

        val B = if (test) {
          for (edge <- A) yield EdgeWithLabel(edge.src, 4 * b * edge.dst, edge.label)
        } else {
          for (edge <- A) yield EdgeWithLabel(edge.src, 2 * b * edge.dst, edge.label)
        }

        write(tempOutputPath("/graphs/duplicate_edges.csv"), new CSVOutputFormat[EdgeWithLabel[String]]())(B)
        println(i)
      }
    }

    val x = algorithm.run(rt)
  }

  private def runGlobalAggregates() = {
    val algorithm = /*emma.parallelize*/ {
      val N = 10000
      val A = DataBag(1 to N)

      val B = A.minBy(_ < _)
      val C = A.maxBy(_ < _)
      val D = A.min()
      val E = A.max()
      val F = A.sum()
      val G = A.product()
      val H = A.count()
      val I = A.empty()
      val J = A.exists(_ % 79 == 0)
      val K = A.forall(_ % 79 == 0)
    }

    //algorithm.run(rt)
  }

  private def runTest() = {
    val algorithm = /*emma.parallelize*/ {
      val N = 10000

      val A = for (a <- DataBag(1 to N)) yield (a, 2 * a, 3 * a)

      A.fetch()
    }

    //algorithm.run(rt)
  }

  private def testSerialization() = {
    val z = 7
    val algorithm = /*emma.parallelize*/ {
      val N = 10000
      val M = Math.sqrt(N).ceil.toInt

      val X = DataBag(1 to M)
      val Y = DataBag(1 to M)
      val Z = DataBag(1 to N)
      val R = for (x <- X; y <- Y; z <- Z; if x * x + y * y == z) yield (x, y, z)

      R
      //val A = (for (x <- X; y <- Y; if x == z) yield (x, y)).groupBy(_._1)

      //      val K = read[Int]("file://hello.txt", new CSVInputFormat[Int])
      //      val A = (for (k <- K; l <- K; if l == k) yield (k, l)).groupBy(_._1 % 10) // DataBag(1 to N).groupBy(_ % 10) FIXME
      //      val B = for (a <- A; if a.key % 7 != 0) yield (a.key, a.values.map(_._2).sum())
      //      val C = for (x <- read[Int]("file://hello.txt", new CSVInputFormat[Int]) if x % z != 0) yield 5*x
      //      val D = write("file://hellotimesthree.txt", new OutputFormat[Int])(C)
      //      val E = for (a <- read[Int]("file://hello.txt", new CSVInputFormat[Int])) yield (a, 3*a) //.minBy(_._1 < _._1)
    }

    //val x = algorithm.run(rt)
  }

  private def runMinimal() = {
    val algorithm = /*emma.parallelize*/ {
      val N = 10000
      val M = Math.sqrt(N).ceil.toInt

      val X = DataBag(1 to M)
      val Y = DataBag(1 to M)
      val Z = DataBag(1 to N)

      val A = for (x <- X; y <- Y; z <- Z; if x * x + y * y == z) yield (x, y, z)
    }

    //algorithm.run(rt)
  }

  private def runCompareStoreSales() = {

    val salesLUrl = "file://tmp/cmp-sales-input-l.txt"
    val salesRUrl = "file://tmp/cmp-sales-input-r.txt"
    val outputUrl = "file://tmp/cmp-sales-output.txt"

    import eu.stratosphere.emma.examples.exploration.unnesting.CompareStoreSales.Schema._

    val algorithm = /* parallelize */ {

      val salesL = read(salesLUrl, new CSVInputFormat[SalesHistory])
      val salesR = read(salesRUrl, new CSVInputFormat[SalesHistory])

      val comparison = for (l <- salesL.groupBy(x => GroupKey(x.store, x.date));
                            r <- salesR.groupBy(x => GroupKey(x.store, x.date));
                            if l.key.store.area == r.key.store.area && l.key.date == r.key.date) yield {

        val balance = for (entryL <- l.values;
                           entryR <- r.values;
                           if entryL.product == entryR.product) yield entryL.count * entryL.product.price - entryR.count * entryR.product.price

        SalesBalance(l.key.store, r.key.store, l.key.date, balance.sum())
      }

      write(outputUrl, new CSVOutputFormat[SalesBalance])(comparison)
    }

    // algorithm.run(rt)
  }

  private def runKMeans() = {

    val epsilon = 0.5
    val k = 3
    val inputUrl = "file://tmp/kmeans-input.txt"
    val outputUrl = "file://tmp/kmeans-output.txt"

    import eu.stratosphere.emma.examples.datamining.clustering.KMeans
    import eu.stratosphere.emma.examples.datamining.clustering.KMeans.Schema._

    val algorithm = /*emma.parallelize*/ {
      // read input
      val points = read(inputUrl, new CSVInputFormat[Point])

      // initialize random cluster means
      val random = new Random(KMeans.SEED)
      var means = DataBag(for (i <- 1 to k) yield Point(i, Vector(random.nextDouble(), random.nextDouble(), random.nextDouble())))
      var change = 0.0

      // initialize solution
      var solution = for (p <- points) yield {
        val closestMean = means.minBy((m1, m2) => (p.pos squaredDist m1.pos) < (p.pos squaredDist m2.pos)).get
        Solution(p, closestMean.id)
      }

      do {
        // update solution: re-assign clusters
        solution = for (s <- solution) yield {
          val closestMean = means.minBy((m1, m2) => (s.point.pos squaredDist m1.pos) < (s.point.pos squaredDist m2.pos)).get
          s.copy(clusterID = closestMean.id)
        }

        // update means
        val newMeans = for (cluster <- solution.groupBy(_.clusterID)) yield {
          val sum = (for (p <- cluster.values) yield p.point.pos).fold[Vector](Vector.zeros(3), identity, (x, y) => x + y)
          val cnt = (for (p <- cluster.values) yield p.point.pos).fold[Int](0, _ => 1, (x, y) => x + y)
          Point(cluster.key, sum / cnt)
        }

        // compute change between the old and the new means
        change = {
          val distances = for (mean <- means; newMean <- newMeans; if mean.id == newMean.id) yield mean.pos squaredDist newMean.pos
          distances.sum()
        }

        // use new means for the next iteration
        means = newMeans
      } while (change < epsilon)

      // write result
      write(outputUrl, new CSVOutputFormat[(PID, PID)])(for (s <- solution) yield (s.point.id, s.clusterID))
    }

    //algorithm.run(rt)
  }


  class DataflowWrapper extends scala.AnyRef {

    import org.apache.flink.api.{java => fjava};
    import org.apache.flink.api.{common => fcommon};
    import org.apache.flink.api.common.{functions => ffns};
    import org.apache.flink.{core => fcore};

    def run(env: fjava.ExecutionEnvironment) = {
      val __input = {
        val __input = {
          val inPath = new fcore.fs.Path("/tmp/emma/graphs/dwg.csv");
          val inputFormat = new fjava.io.CsvInputFormat[fjava.tuple.Tuple3[Int, Int, String]](inPath, "\n", '\t', classOf[Int], classOf[Int], classOf[String]);
          val typeInformation = new fjava.typeutils.TupleTypeInfo[fjava.tuple.Tuple3[Int, Int, String]](fcommon.typeinfo.BasicTypeInfo.INT_TYPE_INFO, fcommon.typeinfo.BasicTypeInfo.INT_TYPE_INFO, fcommon.typeinfo.BasicTypeInfo.STRING_TYPE_INFO);
          env.createInput(inputFormat, typeInformation)
        }
        __input.map(new ffns.MapFunction[fjava.tuple.Tuple3[Int, Int, String], fjava.tuple.Tuple3[Int, Int, String]] {
          override def map(x: fjava.tuple.Tuple3[Int, Int, String]): fjava.tuple.Tuple3[Int, Int, String] = {
            new fjava.tuple.Tuple3[Int, Int, String](x.f0, x.f1, x.f2)
          }
        })
      }

      val outPath = new fcore.fs.Path("/tmp/output.txt");
      val outputFormat = new fjava.io.CsvOutputFormat[fjava.tuple.Tuple3[Int, Int, String]](outPath);
      __input.write(outputFormat, "/tmp/output.txt", fcore.fs.FileSystem.WriteMode.OVERWRITE)
    }
  }

}