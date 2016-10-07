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
package test.schema

import api._

import java.time.Instant

/** A simple domain for marketing, consisting of ads and ad views. */
object Marketing {

  val DEFAULT_CLASS = AdClass.FASHION

  case class Name(first: String, last: String)

  case class User(id: Long, name: Name, password: String)

  case class Ad(id: Long, name: String, `class`: AdClass.Value)

  case class Click(adID: Long, userID: Long, time: Instant)

  object AdClass extends Enumeration {
    type AdClass = Value
    val SERVICES, FASHION, GAMES, TECH = Value
  }

  // common value definitions used below
  val users = DataBag(Seq(
    User(1, Name("John", "Doe"), "GOD"),
    User(2, Name("Jane", "Doe"), "GODESS")
  ))

  val ads = DataBag(Seq(
    Ad(1, "Uber AD", AdClass.SERVICES),
    Ad(2, "iPhone 6 ad", AdClass.TECH),
    Ad(3, "Galaxy 7 ad", AdClass.TECH)
  ))

  val clicks = DataBag(Seq(
    Click(1, 1, Instant.parse("2015-04-04T14:50:05.00Z")),
    Click(1, 2, Instant.parse("2015-04-04T14:51:12.00Z")),
    Click(2, 3, Instant.parse("2015-04-04T14:55:06.00Z"))
  ))
}
