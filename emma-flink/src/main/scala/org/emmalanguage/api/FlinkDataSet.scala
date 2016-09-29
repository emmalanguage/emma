package org.emmalanguage
package api

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

import scala.language.{higherKinds, implicitConversions}

/** A `DataBag` implementation backed by a Flink `DataSet`. */
class FlinkDataSet[A: Meta] private[api](@transient private val rep: DataSet[A]) extends DataBag[A] {

  import FlinkDataSet.{typeInfoForType, wrap}

  // -----------------------------------------------------
  // Structural recursion
  // -----------------------------------------------------

  override def fold[B: Meta](z: B)(s: A => B, u: (B, B) => B): B = {
    val collected = rep.map(x => s(x)).reduce(u).collect()
    if (collected.isEmpty) {
      z
    } else {
      assert(collected.size == 1)
      collected.head
    }
  }

  // -----------------------------------------------------
  // Monad Ops
  // -----------------------------------------------------

  override def map[B: Meta](f: (A) => B): FlinkDataSet[B] =
    rep.map(f)

  override def flatMap[B: Meta](f: (A) => DataBag[B]): FlinkDataSet[B] =
    rep.flatMap((x: A) => f(x).fetch())

  def withFilter(p: (A) => Boolean): FlinkDataSet[A] =
    rep.filter(p)

  // -----------------------------------------------------
  // Grouping
  // -----------------------------------------------------

  override def groupBy[K: Meta](k: (A) => K): FlinkDataSet[Group[K, DataBag[A]]] = {
    rep.groupBy(k).reduceGroup((it: Iterator[A]) => {
      val buffer = it.toBuffer // This is because the iterator might not be serializable
      Group(k(buffer.head), DataBag(buffer))
    })
  }

  // -----------------------------------------------------
  // Set operations
  // -----------------------------------------------------

  override def union(that: DataBag[A]): FlinkDataSet[A] = that match {
    case dataset: FlinkDataSet[A] => this.rep union dataset.rep
  }

  override def distinct: FlinkDataSet[A] =
    rep.distinct

  // -----------------------------------------------------
  // Sinks
  // -----------------------------------------------------

  def fetch(): Seq[A] =
    rep.collect()
}

object FlinkDataSet {

  import org.apache.flink.api.common.typeinfo.TypeInformation

  import scala.reflect.runtime.currentMirror
  import scala.reflect.runtime.universe._
  import scala.tools.reflect.ToolBox

  import java.nio.file.{Files, Paths}

  private lazy val codeGenDirDefault = Paths
    .get(sys.props("java.io.tmpdir"), "emma", "codegen")
    .toAbsolutePath.toString

  /** The directory where the toolbox will store runtime-generated code. */
  private lazy val codeGenDir = {
    val path = Paths.get(sys.props.getOrElse("emma.codegen.dir", codeGenDirDefault))
    // Make sure that generated class directory exists
    Files.createDirectories(path)
    path.toAbsolutePath.toString
  }

  private lazy val flinkApi = currentMirror.staticModule("org.apache.flink.api.scala.package")
  private lazy val typeInfo = flinkApi.info.decl(TermName("createTypeInformation"))
  private lazy val toolbox = currentMirror.mkToolBox(options = s"-d $codeGenDir")

  private lazy val memo = collection.mutable.Map.empty[Any, Any]

  implicit def typeInfoForType[T: Meta]: TypeInformation[T] = {
    val ttag = implicitly[TypeTag[T]]
    val info = memo.getOrElseUpdate(ttag, toolbox.eval(q"$typeInfo[${ttag.tpe}]")).asInstanceOf[TypeInformation[T]]
    info
  }

  implicit def wrap[A: Meta](rep: DataSet[A]): FlinkDataSet[A] =
    new FlinkDataSet(rep)

  def apply[A: Meta](implicit flink: ExecutionEnvironment): FlinkDataSet[A] =
    flink.fromElements[A]()

  def apply[A: Meta](seq: Seq[A])(implicit flink: ExecutionEnvironment): FlinkDataSet[A] =
    flink.fromCollection(seq)
}