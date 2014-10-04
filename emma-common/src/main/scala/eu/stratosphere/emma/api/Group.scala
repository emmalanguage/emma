package eu.stratosphere.emma.api

import eu.stratosphere.emma.api.model.Identity

case class Group[K, +V](key: K, values: V) extends Identity[K] {
  def identity = key
}
