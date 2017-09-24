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

import lib.BaseLibSpec

class MurmurHashSpec extends BaseLibSpec {
  val hasher = new MurmurHash(0)

  "MurmurHash" should "hash integer values" in {
    hasher.hashInt(0) shouldBe 593689054
    hasher.hashInt(-42) shouldBe -189366624
    hasher.hashInt(42) shouldBe -1134849565
    hasher.hashInt(Int.MinValue) shouldBe -1718298732
    hasher.hashInt(Int.MaxValue) shouldBe -1653689534
  }

  it should "hash long values" in {
    hasher.hashLong(0) shouldBe 1669671676
    hasher.hashLong(-42) shouldBe -846261623
    hasher.hashLong(42) shouldBe 1871679806
    hasher.hashLong(Long.MinValue) shouldBe 1366273829
    hasher.hashLong(Long.MaxValue) shouldBe -2106506049
  }

  it should "hash a byte array" in {
    val bytes = (0 to 15).map(_.toByte).toArray
    hasher.hashUnsafeWords(bytes, Platform.ByteArrayOffset, bytes.length) shouldBe 420836317
  }
}
