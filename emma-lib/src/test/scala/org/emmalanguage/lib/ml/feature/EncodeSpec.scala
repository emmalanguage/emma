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
package lib.ml.feature

import api._
import lib.linalg._
import lib.ml._

import org.scalactic._

import collection.Map

class EncodeSpec extends FeatureSpec {

  implicit object SeqSPointIntEquality extends Equality[Seq[SPoint[Int]]] {

    def areEqual(lhs: Seq[SPoint[Int]], rhs: Any) = rhs match {
      case rhs: Seq[SPoint[Int]@unchecked] => {
        lhs.map(_.id) === rhs.map(_.id)
      } && (lhs.map(_.pos) zip rhs.map(_.pos)).forall({
        case (v, w) => v.size === w.size &&
          v.values.toList === w.values.toList &&
          v.indices.toList === w.indices.toList
      })
      case _ => false
    }
  }

  "encode.freq" should "compute corred results with a hash function" in {
    val exp = expHash(false)
    val act = freq(tokenss.zipWithIndex)
    act.shouldEqual(exp)(SeqSPointIntEquality)
  }

  it should "compute corred results with a dictionary" in {
    val dic = dict(tokenss)
    val exp = expDict(dic, false)
    val act = freq(dic, tokenss.zipWithIndex)
    act shouldEqual exp
  }

  "encode.bin" should "compute corred results with a hash function" in {
    val exp = expHash(true)
    val act = bin(tokenss.zipWithIndex)
    act shouldEqual exp
  }

  it should "compute corred results with a dictionary" in {
    val dic = dict(tokenss)
    val exp = expDict(dic, true)
    val act = bin(dic, tokenss.zipWithIndex)
    act shouldEqual exp
  }

  protected final def expHash(bin: Boolean) = for {
    (tokens, id) <- tokenss.zipWithIndex.toSeq
  } yield {
    val kx = (x: String) => nonNegativeMod(encode.native(x), encode.card)
    val rs = tokens.groupBy(kx).mapValues(vs => if (bin) 1.0 else vs.length)
    SPoint(id, sparse(encode.card, rs.toSeq))
  }

  protected final def expDict(dict: Map[String, Int], bin: Boolean) = for {
    (tokens, id) <- tokenss.zipWithIndex.toSeq
  } yield {
    val kx = (x: String) => nonNegativeMod(dict(x), dict.size)
    val rs = tokens.groupBy(kx).mapValues(vs => if (bin) 1.0 else vs.length)
    SPoint(id, sparse(dict.size, rs.toSeq))
  }

  protected def dict(xs: Seq[Array[String]]): Map[String, Int] = {
    val fs = for {
      x <- DataBag(xs)
      f <- DataBag(x)
    } yield f
    encode.dict(fs)
  }

  protected def freq(xs: Seq[(Array[String], Int)]) = {
    val rs = for {
      (tokens, id) <- DataBag(xs)
    } yield SPoint(id, encode.freq[String]()(tokens))
    rs.collect()
  }

  protected def freq(dict: Map[String, Int], xs: Seq[(Array[String], Int)]) = {
    val rs = for {
      (tokens, id) <- DataBag(xs)
    } yield SPoint(id, encode.freq[String](dict)(tokens))
    rs.collect()
  }

  protected def bin(xs: Seq[(Array[String], Int)]) = {
    val rs = for {
      (tokens, id) <- DataBag(xs)
    } yield SPoint(id, encode.bin[String]()(tokens))
    rs.collect()
  }

  protected def bin(dict: Map[String, Int], xs: Seq[(Array[String], Int)]) = {
    val rs = for {
      (tokens, id) <- DataBag(xs)
    } yield SPoint(id, encode.bin[String](dict)(tokens))
    rs.collect()
  }
}
