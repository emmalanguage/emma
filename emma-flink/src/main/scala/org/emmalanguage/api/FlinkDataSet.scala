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
package org.emmalanguage
package api

import alg.Alg

import org.apache.flink.api.common.functions.RichMapPartitionFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala.utils.DataSetUtils
import org.apache.flink.api.scala.{ExecutionEnvironment => FlinkEnv}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.util.Collector

import scala.util.hashing.MurmurHash3

import java.lang

/** A `DataBag` implementation backed by a Flink `DataSet`. */
class FlinkDataSet[A: Meta] private[api]
(
  @transient private[emmalanguage] val rep: DataSet[A]
) extends DataBag[A] {

  import FlinkDataSet.typeInfoForType
  import Meta.Projections._

  @transient override val m = implicitly[Meta[A]]

  private[emmalanguage] implicit def env = this.rep.getExecutionEnvironment

  // -----------------------------------------------------
  // Structural recursion
  // -----------------------------------------------------

  override def fold[B: Meta](alg: Alg[A, B]): B = {
    val collected = rep.map(x => alg.init(x)).reduce(alg.plus).collect()
    assert(collected.size <= 1)
    if (collected.isEmpty) alg.zero
    else collected.head
  }

  // -----------------------------------------------------
  // Monad Ops
  // -----------------------------------------------------

  override def map[B: Meta](f: (A) => B): DataBag[B] =
    FlinkDataSet(rep.map(f))

  override def flatMap[B: Meta](f: (A) => DataBag[B]): DataBag[B] =
    FlinkDataSet(rep.flatMap((x: A) => f(x).collect()))

  def withFilter(p: (A) => Boolean): DataBag[A] =
    FlinkDataSet(rep.filter(p))

  // -----------------------------------------------------
  // Grouping
  // -----------------------------------------------------

  override def groupBy[K: Meta](k: (A) => K): DataBag[Group[K, DataBag[A]]] =
    FlinkDataSet(rep.groupBy(k).reduceGroup((it: Iterator[A]) => {
      val buffer = it.toBuffer // This is because the iterator might not be serializable
      Group(k(buffer.head), DataBag(buffer))
    }))

  // -----------------------------------------------------
  // Set operations
  // -----------------------------------------------------

  override def union(that: DataBag[A]): DataBag[A] = that match {
    case dbag: ScalaSeq[A] => this union FlinkDataSet(dbag.rep)
    case dbag: FlinkDataSet[A] => FlinkDataSet(this.rep union dbag.rep)
    case _ => throw new IllegalArgumentException(s"Unsupported rhs for `union` of type: ${that.getClass}")
  }

  override def distinct: DataBag[A] =
    FlinkDataSet(rep.distinct)

  // -----------------------------------------------------
  // Partition-based Ops
  // -----------------------------------------------------

  def sample(k: Int, seed: Long = 5394826801L): Vector[A] = {
    // sample per partition and sorted by partition ID
    val Seq(hd, tl@_*) = new DataSetUtils(rep).zipWithIndex
      .mapPartition(new RichMapPartitionFunction[(Long, A), (Int, Array[Option[A]])] {
        @transient private var pid = 0

        override def open(parameters: Configuration): Unit = {
          super.open(parameters)
          pid = getRuntimeContext.getIndexOfThisSubtask
        }

        import scala.collection.JavaConversions._

        def mapPartition(it: lang.Iterable[(Long, A)], out: Collector[(Int, Array[Option[A]])]): Unit = {
          val sample = Array.fill(k)(Option.empty[A])
          for ((i, e) <- it) {
            if (i >= k) {
              val j = util.RanHash(seed).at(i).nextLong(i + 1)
              if (j < k) sample(j.toInt) = Some(e)
            } else sample(i.toInt) = Some(e)
          }
          out.collect(pid -> sample)
        }
      }).collect().sortBy(_._1).map(_._2)

    // merge the sequence of samples and filter None values
    val rs = for {
      Some(v) <- tl.foldLeft(hd)((xs, ys) => for ((x, y) <- xs zip ys) yield y orElse x)
    } yield v

    // convert result to vector
    rs.toVector
  }

  def zipWithIndex(): DataBag[(A, Long)] =
    FlinkDataSet(new DataSetUtils(rep).zipWithIndex).map(_.swap)

  // -----------------------------------------------------
  // Sinks
  // -----------------------------------------------------

  override def writeCSV(path: String, format: CSV)(implicit converter: CSVConverter[A]): Unit = {
    rep.mapPartition((it: Iterator[A]) => new Traversable[String] {
      def foreach[U](f: (String) => U): Unit = {
        val csv = CSVScalaSupport[A](format).writer()
        val con = CSVConverter[A]
        val rec = Array.ofDim[String](con.size)
        for (x <- it) {
          con.write(x, rec, 0)(format)
          f(csv.writeRowToString(rec))
        }
      }
    }).writeAsText(path, FileSystem.WriteMode.OVERWRITE)

    rep.getExecutionEnvironment.execute()
  }

  override def writeText(path: String): Unit =
    rep.writeAsText(
      filePath = path,
      writeMode = FileSystem.WriteMode.OVERWRITE
    )

  def writeParquet(path: String, format: Parquet)(implicit converter: ParquetConverter[A]): Unit =
    ???

  override def collect(): Seq[A] = collected

  private lazy val collected: Seq[A] =
    rep.collect()

  // -----------------------------------------------------
  // equals, hashCode and toString
  // -----------------------------------------------------

  override def equals(o: Any) =
    super.equals(o)

  override def hashCode(): Int = {
    val (a, b, c, n) = rep
      .mapPartition(it => {
        var a, b, n = 0
        var c = 1
        it foreach { x =>
          val h = x.##
          a += h
          b ^= h
          if (h != 0) c *= h
          n += 1
        }
        Some((a, b, c, n)).iterator
      })
      .collect()
      .fold((0, 0, 1, 0))((x, r) => (x, r) match {
        case ((a1, b1, c1, n1), (a2, b2, c2, n2)) => (a1 + a2, b1 ^ b2, c1 * c2, n1 + n2)
      })

    var h = MurmurHash3.traversableSeed
    h = MurmurHash3.mix(h, a)
    h = MurmurHash3.mix(h, b)
    h = MurmurHash3.mixLast(h, c)
    MurmurHash3.finalizeHash(h, n)
  }
}

object FlinkDataSet extends DataBagCompanion[FlinkEnv] {

  import scala.reflect.runtime.{universe => u}

  private val memo = collection.mutable.Map.empty[Any, Any]

  { // initialize memo table with standard types
    import org.apache.flink.api.scala._
    // standard Scala types
    memoizeTypeInfo(implicitly[Meta[Unit]], createTypeInformation)
    memoizeTypeInfo(implicitly[Meta[Boolean]], createTypeInformation)
    memoizeTypeInfo(implicitly[Meta[Char]], createTypeInformation)
    memoizeTypeInfo(implicitly[Meta[Byte]], createTypeInformation)
    memoizeTypeInfo(implicitly[Meta[Short]], createTypeInformation)
    memoizeTypeInfo(implicitly[Meta[Int]], createTypeInformation)
    memoizeTypeInfo(implicitly[Meta[Long]], createTypeInformation)
    memoizeTypeInfo(implicitly[Meta[Float]], createTypeInformation)
    memoizeTypeInfo(implicitly[Meta[Double]], createTypeInformation)
    memoizeTypeInfo(implicitly[Meta[String]], createTypeInformation)
    memoizeTypeInfo(implicitly[Meta[BigInt]], createTypeInformation)
    memoizeTypeInfo(implicitly[Meta[BigDecimal]], createTypeInformation)
    // standard Java types
    memoizeTypeInfo(implicitly[Meta[java.lang.Void]], createTypeInformation)
    memoizeTypeInfo(implicitly[Meta[java.lang.Boolean]], createTypeInformation)
    memoizeTypeInfo(implicitly[Meta[java.lang.Character]], createTypeInformation)
    memoizeTypeInfo(implicitly[Meta[java.lang.Byte]], createTypeInformation)
    memoizeTypeInfo(implicitly[Meta[java.lang.Short]], createTypeInformation)
    memoizeTypeInfo(implicitly[Meta[java.lang.Integer]], createTypeInformation)
    memoizeTypeInfo(implicitly[Meta[java.lang.Long]], createTypeInformation)
    memoizeTypeInfo(implicitly[Meta[java.lang.Float]], createTypeInformation)
    memoizeTypeInfo(implicitly[Meta[java.lang.Double]], createTypeInformation)
    memoizeTypeInfo(implicitly[Meta[java.math.BigInteger]], createTypeInformation)
    memoizeTypeInfo(implicitly[Meta[java.math.BigDecimal]], createTypeInformation)
  }

  def memoizeTypeInfo[T](implicit meta: Meta[T], info: TypeInformation[T]): TypeInformation[T] = {
    val tpe = fix(meta.tpe).toString
    val res = memo.getOrElseUpdate(tpe, info)
    res.asInstanceOf[TypeInformation[T]]
  }

  implicit def typeInfoForType[T](implicit meta: Meta[T]): TypeInformation[T] = {
    val tpe = fix(meta.tpe).toString
    if (memo.contains(tpe)) memo(tpe).asInstanceOf[TypeInformation[T]]
    else throw new RuntimeException(
      s"""
        |Cannot find TypeInformation for type $tpe.
        |Try calling `FlinkDataSet.memoizeTypeInfo[$tpe]` explicitly before the `emma.onFlink` quote.
      """.stripMargin.trim
    )
  }

  private def fix(tpe: u.Type): u.Type = tpe.dealias.map(t => {
    if (t =:= u.typeOf[java.lang.String]) u.typeOf[String]
    else t
  })

  import Meta.Projections._

  // ---------------------------------------------------------------------------
  // Constructors
  // ---------------------------------------------------------------------------

  def empty[A: Meta](
    implicit flink: FlinkEnv
  ): DataBag[A] = FlinkDataSet(flink
    .fromElements[A]())

  def apply[A: Meta](values: Seq[A])(
    implicit flink: FlinkEnv
  ): DataBag[A] = FlinkDataSet(flink
    .fromCollection(values)
    .setParallelism(flink.getParallelism))

  def readText(path: String)(
    implicit flink: FlinkEnv
  ): DataBag[String] = FlinkDataSet(flink
    .readTextFile(path))

  def readCSV[A: Meta : CSVConverter](path: String, format: CSV)(
    implicit flink: FlinkEnv
  ): DataBag[A] = FlinkDataSet(flink
    .readTextFile(path, format.charset)
    .mapPartition((it: Iterator[String]) => new Traversable[A] {
      def foreach[U](f: (A) => U): Unit = {
        val csv = CSVScalaSupport[A](format).parser()
        val con = CSVConverter[A]
        for (line <- it) f(con.read(csv.parseLine(line), 0)(format))
      }
    }))

  def readParquet[A: Meta : ParquetConverter](path: String, format: Parquet)(
    implicit flink: FlinkEnv
  ): DataBag[A] = ???

  // ---------------------------------------------------------------------------
  // Implicit Rep -> DataBag conversion
  // ---------------------------------------------------------------------------

  private[api] def apply[A: Meta](
    rep: DataSet[A]
  )(implicit flink: FlinkEnv): DataBag[A] = new FlinkDataSet(rep)

  private[api] def unapply[A: Meta](
    bag: DataBag[A]
  )(implicit flink: FlinkEnv): Option[DataSet[A]] = bag match {
    case bag: FlinkDataSet[A] => Some(bag.rep)
    case _ => Some(flink.fromCollection(bag.collect()))
  }
}
