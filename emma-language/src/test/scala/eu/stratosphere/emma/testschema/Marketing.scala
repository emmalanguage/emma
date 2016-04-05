package eu.stratosphere.emma.testschema

import java.time.Instant

import eu.stratosphere.emma.api.DataBag

/**
 * A simple domain for marketing, consisting of ads and ad views.
 */
object Marketing {

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
