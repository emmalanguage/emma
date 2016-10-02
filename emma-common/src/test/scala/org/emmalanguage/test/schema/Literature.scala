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

object Literature {

  case class Book(title: String, author: String)

  case class Character(name: String, book: Book)

  // ---------------------------------------------------------------------------
  // The Hitchhiker's Guide to the Galaxy
  // ---------------------------------------------------------------------------

  val hhBook = Book("The Hitchhiker's Guide to the Galaxy", "Douglas Adams")

  val hhCrts = Seq(
    Character("Arthur Dent", hhBook),
    Character("Zaphod Beeblebrox", hhBook),
    Character("Prostetnic Vogon Jeltz", hhBook))
}
