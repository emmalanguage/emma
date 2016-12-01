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
package examples.imdb

object model {

  //@formatter:off
  case class Movie
  (
    id       : Long,
    title    : Title,
    year     : Option[Short]  = None,
    yearFrom : Option[Short]  = None,
    yearTo   : Option[Short]  = None
  )

  case class Title
  (
    title         : String,
    version       : String,
    titleType     : String,
    episodeName   : Option[String] = None,
    episodeNumber : Option[String] = None,
    suspended     : Boolean        = false
  ) {
    private val parenTitleTypes = Set[String]("Series", "Episode")

    override def toString: String = {
      val modifiers = mkModifiers
      val episodeNm = mkEpisodeName
      val susString = if(suspended) Some("{{SUSPENDED}}") else None
      Seq[Option[String]](Some(mkTitle), Some(s"($version)"), modifiers, episodeNm, susString)
        .flatten
        .mkString(" ")
    }

    private def mkTitle: String =
      if(parenTitleTypes.contains(titleType)) s""""$title""""
      else title

    private def mkModifiers: Option[String] = titleType match {
      case "TV"         => Some("(TV)")
      case "Video"      => Some("(V)")
      case "VideoGame"  => Some("(VG)")
      case _            => None
    }

    private def mkEpisodeName: Option[String] =
      List(episodeName, episodeNumber.map(n => s"($n)")).flatten match {
        case Nil => None
        case x => Some(s"""{${x.mkString(" ")}}""")
      }
  }

  case class Person
  (
    id      : Long,
    gender  : Option[String],
    name    : String
  )

  case class Credit
  (
    personID        : Long,
    movieID         : Long,
    creditType      : String, // actor, director, cinematographer, editor, ...
    creditedAs      : Option[String] = None,
    characterName   : Option[String] = None,
    billingPosition : Option[Int] = None
  )

  case class Rating
  (
    movieID: Long,
    votesDistribution : String, // len 10, consisting of [.*1-9]
    totalVotes        : Int,
    rating            : String,
    isNew             : Boolean = false
  )

  case class Technical
  (
    movieID: Long,
    key     : String,
    value   : String,
    comment : Option[String]
  )

  case class Genre(movieID: Long, genre: String)

  case class Country(movieID: Long, country: String)

  case class Keyword(movieID: Long, keyword: String)

  case class Location(movieID: Long, location: String, comment: Option[String])

  case object TitleType extends Enumeration {
    val MotionPicture = Value("MotionPicture")
    val TV = Value("TV")
    val Video = Value("Video")
    val Series = Value("Series")
    val Episode = Value("Episode")
    val VideoGame = Value("VideoGame")
  }

  case object CreditType extends Enumeration {
    val Actor = Value("actor")
    val Composer = Value("composer")
    val Cinematographer = Value("cinematographer")
    val Director = Value("director")
    val Editor = Value("editor")
    val Producer = Value("producer")
    val Writer = Value("writer")
  }

  case object TechnicalKey extends Enumeration {
    val Cam = Value("Cam")
    val Met = Value("Met")
    val Ofm = Value("Ofm")
    val Pfm = Value("Pfm")
    val Rat = Value("Rat")
    val Pcs = Value("Pcs")
    val Lab = Value("Lab")
  }

  case object Gender extends Enumeration {
    val Male = Value("m")
    val Female = Value("f")
  }
  //@formatter:on
}
