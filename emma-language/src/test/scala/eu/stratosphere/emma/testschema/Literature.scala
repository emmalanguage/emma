package eu.stratosphere.emma
package testschema

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
