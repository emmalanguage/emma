package eu.stratosphere.emma.api

import spire.math._

import scala.reflect.ClassTag

package object lara {

  implicit class VectorOps[A: Numeric : ClassTag](private val n: A) {
    def +(v: Vector[A]): Vector[A] = v + n

    def -(v: Vector[A]): Vector[A] = v - n

    def *(v: Vector[A]): Vector[A] = v * n

    def /(v: Vector[A]): Vector[A] = v / n
  }

  implicit class MatrixOps[A: Numeric : ClassTag](private val n: A) {
    def +(v: Matrix[A]): Matrix[A] = v + n

    def -(v: Matrix[A]): Matrix[A] = v - n

    def *(v: Matrix[A]): Matrix[A] = v * n

    def /(v: Matrix[A]): Matrix[A] = v / n
  }

}
