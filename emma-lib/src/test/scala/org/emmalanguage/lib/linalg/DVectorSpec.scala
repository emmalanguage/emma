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
package lib.linalg

import lib.BaseLibSpec

import scala.util.Random

class DVectorSpec extends BaseLibSpec {
  val d = 10 // dimensionality
  val N = 50 // number of instances
  val prng = new Random()

  // Constructors

  "A DVector" should "be constructed from an Array" in {
    val exp = Array.fill(d)(prng.nextDouble())
    val vec = dense(exp)

    assert(vec.size == exp.length, "Vectors are not the same length")
    assert(vec.values.sameElements(exp), "Vectors don't have the same elements")
  }

  it should "be constructed with all elements zero" in {
    val exp = Array.fill(d)(0.0)
    val vec = zeros(d)

    assert(vec.size == exp.length, "Vectors are not the same length")
    assert(vec.values.sameElements(exp), "Vectors don't have the same elements")
  }

  // in-place operations

  it should "be added to a DVector in-place" in {
    val a1 = Array.fill(d)(prng.nextDouble())
    val a2 = Array.fill(d)(prng.nextDouble())

    val vec1 = dense(a1)
    val vec2 = dense(a2)

    val exp = a1.zip(a2).map(x => x._1 + x._2)
    vec1 += vec2

    vec1.values should contain theSameElementsAs exp
  }

  it should "be subtracted from a DVector in-place" in {
    val a1 = Array.fill(d)(prng.nextDouble())
    val a2 = Array.fill(d)(prng.nextDouble())

    val vec1 = dense(a1)
    val vec2 = dense(a2)

    val exp = a1.zip(a2).map(x => x._1 - x._2)
    vec1 -= vec2

    vec1.values should contain theSameElementsAs exp
  }

  it should "be multiplied element-wise with a DVector in-place" in {
    val a1 = Array.fill(d)(prng.nextDouble())
    val a2 = Array.fill(d)(prng.nextDouble())

    val vec1 = dense(a1)
    val vec2 = dense(a2)

    val exp = a1.zip(a2).map(x => x._1 * x._2)
    vec1 *= vec2

    vec1.values should contain theSameElementsAs exp
  }

  it should "be divided element-wise by a DVector in-place" in {
    val a1 = Array.fill(d)(prng.nextDouble())
    val a2 = Array.fill(d)(prng.nextDouble())

    val vec1 = dense(a1)
    val vec2 = dense(a2)

    val exp = a1.zip(a2).map(x => x._1 / x._2)
    vec1 /= vec2

    vec1.values should contain theSameElementsAs exp
  }

  it should "be added with a Double in-place" in {
    val a1 = Array.fill(d)(prng.nextDouble())
    val b  = prng.nextDouble()

    val vec1 = dense(a1)

    val exp = a1.map(x => x + b)
    vec1 += b

    vec1.values should contain theSameElementsAs exp
  }

  it should "be subtracted with a Double in-place" in {
    val a1 = Array.fill(d)(prng.nextDouble())
    val b  = prng.nextDouble()

    val vec1 = dense(a1)

    val exp = a1.map(x => x - b)
    vec1 -= b

    vec1.values should contain theSameElementsAs exp
  }

  it should "be multiplied with a Double in-place" in {
    val a1 = Array.fill(d)(prng.nextDouble())
    val b  = prng.nextDouble()

    val vec1 = dense(a1)

    val exp = a1.map(x => x * b)
    vec1 *= b

    vec1.values should contain theSameElementsAs exp
  }

  it should "be divided with a Double in-place" in {
    val a1 = Array.fill(d)(prng.nextDouble())
    val b  = prng.nextDouble()

    val vec1 = dense(a1)

    val exp = a1.map(x => x / b)
    vec1 /= b

    vec1.values should contain theSameElementsAs exp
  }

  // Non-in-place operations

  it should "be added to a DVector" in {
    val a1 = Array.fill(d)(prng.nextDouble())
    val a2 = Array.fill(d)(prng.nextDouble())

    val vec1 = dense(a1)
    val vec2 = dense(a2)

    val exp = a1.zip(a2).map(x => x._1 + x._2)
    val act = vec1 + vec2

    act.values should contain theSameElementsAs exp
  }

  it should "be subtracted from a DVector" in {
    val a1 = Array.fill(d)(prng.nextDouble())
    val a2 = Array.fill(d)(prng.nextDouble())

    val vec1 = dense(a1)
    val vec2 = dense(a2)

    val exp = a1.zip(a2).map(x => x._1 - x._2)
    val act = vec1 - vec2

    act.values should contain theSameElementsAs exp
  }

  it should "be multiplied element-wise with a DVector" in {
    val a1 = Array.fill(d)(prng.nextDouble())
    val a2 = Array.fill(d)(prng.nextDouble())

    val vec1 = dense(a1)
    val vec2 = dense(a2)

    val exp = a1.zip(a2).map(x => x._1 * x._2)
    val act = vec1 * vec2

    act.values should contain theSameElementsAs exp
  }

  it should "be divided element-wise by a DVector" in {
    val a1 = Array.fill(d)(prng.nextDouble())
    val a2 = Array.fill(d)(prng.nextDouble())

    val vec1 = dense(a1)
    val vec2 = dense(a2)

    val exp = a1.zip(a2).map(x => x._1 / x._2)
    val act = vec1 / vec2

    act.values should contain theSameElementsAs exp
  }

  it should "be added with a Double" in {
    val a1 = Array.fill(d)(prng.nextDouble())
    val b  = prng.nextDouble()

    val vec1 = dense(a1)

    val exp = a1.map(x => x + b)
    val act = vec1 + b

    act.values should contain theSameElementsAs exp
  }

  it should "be subtracted with a Double" in {
    val a1 = Array.fill(d)(prng.nextDouble())
    val b  = prng.nextDouble()

    val vec1 = dense(a1)

    val exp = a1.map(x => x - b)
    val act = vec1 - b

    act.values should contain theSameElementsAs exp
  }

  it should "be multiplied with a Double" in {
    val a1 = Array.fill(d)(prng.nextDouble())
    val b  = prng.nextDouble()

    val vec1 = dense(a1)

    val exp = a1.map(x => x * b)
    val act = vec1 * b

    act.values should contain theSameElementsAs exp
  }

  it should "be divided with a Double" in {
    val a1 = Array.fill(d)(prng.nextDouble())
    val b  = prng.nextDouble()

    val vec1 = dense(a1)

    val exp = a1.map(x => x / b)
    val act = vec1 / b

    act.values should contain theSameElementsAs exp
  }

  // utility functions

  it should "compute the l2 norm" in {
    val a = dense(Array.fill(d)(prng.nextDouble()))

    val act = norm(a, 2)
    val exp = Math.sqrt(a.values.map(x => x * x).sum)

    act shouldBe exp +- 0.001
  }

  it should "compute the squared distance" in {
    val a = dense(Array.fill(d)(prng.nextDouble()))

    val act = a dot a
    val exp = a.values.map(x => x * x).sum

    act shouldBe exp +- 0.001
  }

  it should "compute pairwise min between two DVectors" in {
    val a = dense(Array.fill(d)(prng.nextDouble()))
    val b = dense(Array.fill(d)(prng.nextDouble()))

    val act = a min b
    val exp = (a.values zip b.values).map { case (x, y) => Math.min(x, y) }

    act.values should contain theSameElementsAs exp
  }

  it should "compute pairwise max between two DVectors" in {
    val a = dense(Array.fill(d)(prng.nextDouble()))
    val b = dense(Array.fill(d)(prng.nextDouble()))

    val act = a max b
    val exp = (a.values zip b.values).map { case (x, y) => Math.max(x, y) }

    act.values should contain theSameElementsAs exp
  }

  // TODO add tests
}
