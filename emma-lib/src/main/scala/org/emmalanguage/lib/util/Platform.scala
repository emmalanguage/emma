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
package lib.util

/** Ported from the spark-ml library. */
object Platform {
  import sun.misc.Unsafe

  lazy val unsafe = try {
    val f = classOf[Unsafe].getDeclaredField("theUnsafe")
    f.setAccessible(true)
    f.get(null).asInstanceOf[Unsafe]
  } catch {
    case thr: Throwable =>
      throw new RuntimeException(s"Unable to create `unsafe`.", thr)
  }

  def offsetOrZero[A](cls: Class[Array[A]]) =
    if (unsafe != null) unsafe.arrayBaseOffset(cls)
    else 0

  //@formatter:off
  lazy val BooleanArrayOffset = offsetOrZero(classOf[Array[Boolean]])
  lazy val ByteArrayOffset    = offsetOrZero(classOf[Array[Byte]])
  lazy val ShortArrayOffset   = offsetOrZero(classOf[Array[Short]])
  lazy val IntArrayOffset     = offsetOrZero(classOf[Array[Int]])
  lazy val LongArrayOffset    = offsetOrZero(classOf[Array[Long]])
  lazy val FloatArrayOffset   = offsetOrZero(classOf[Array[Float]])
  lazy val DoubleArrayOffset  = offsetOrZero(classOf[Array[Double]])
  //@formatter:on

  def getInt(obj: AnyRef, offset: Long) =
    unsafe.getInt(obj, offset)

  def getByte(obj: AnyRef, offset: Long) =
    unsafe.getByte(obj, offset)
}
