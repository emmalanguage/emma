package eu.stratosphere.emma.runtime

import java.util.UUID

import eu.stratosphere.emma.codegen.utils
import eu.stratosphere.emma.ir._

package object operators {

  object BasicProperties extends Enumeration {
    type Prop = Value
    val
    ID,
    LOCAL_DOP,
    GLOBAL_DOP,
    OPERATOR_TYPE
    = Value
  }

  // ---------------------------------------------------
  // Traits
  // ---------------------------------------------------

  import scala.reflect.runtime.universe._

  trait Op {
    val operatorName = utils.newOperatorName("TEST")

    val properties = scala.collection.mutable.Map[BasicProperties.Prop, Any]().empty
  }

  trait UnaryOp extends Op {
    val in: Type
    val out: Type
  }

  trait BinaryOp extends Op {
    val in1: Type
    val in2: Type
    val out: Type
  }

  trait UDF {
    this: Op =>
    val udf: Expr[Any]
  }

  // ---------------------------------------------------
  // Operators
  // ---------------------------------------------------

  case class SourceOp[A](src: Read[A]) extends UnaryOp {
    override val in = src.tag.tpe
    // TODO: introduce Unit type
    override val out = src.tag.tpe
  }

  case class TempSourceOp[A](src: TempSource[A]) extends UnaryOp {
    override val in = src.tag.tpe
    override val out = src.tag.tpe

    val ref = src.ref
  }

  case class SinkOp[A](snk: Write[A]) extends UnaryOp {
    override val in = snk.xs.tag.tpe
    override val out = snk.tag.tpe // Unit
  }

  case class TempSinkOp[A](snk: TempSink[A]) extends UnaryOp {
    override val in = snk.xs.tag.tpe
    override val out = snk.tag.tpe // Unit

    val name = snk.name
    val id = UUID.nameUUIDFromBytes(name.getBytes)
  }

  case class MapOp[A, B](map: Map[A, B]) extends UnaryOp with UDF {
    override val in = map.xs.tag.tpe
    override val out = map.tag.tpe

    override val udf = map.f
    val q"(..$args) => $body" = udf.tree
  }

}
