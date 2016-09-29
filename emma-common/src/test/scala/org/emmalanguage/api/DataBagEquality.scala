package org.emmalanguage
package api

import scala.language.higherKinds
import scala.reflect.ClassTag

trait DataBagEquality {

  implicit def equality[M[_] <: DataBag[_], A] = new org.scalactic.Equality[M[A]] {

    val bagTag: ClassTag[M[_]] = implicitly[ClassTag[M[_]]]

    override def areEqual(lhs: M[A], any: Any): Boolean = any match {
      case rhs: DataBag[_] =>
        val lhsVals = lhs.fetch().toSeq
        val rhsVals = rhs.fetch().toSeq

        val lhsXtra = lhsVals diff rhsVals
        val rhsXtra = rhsVals diff lhsVals

        lhsXtra.isEmpty && rhsXtra.isEmpty
      case _ =>
        false
    }
  }
}

