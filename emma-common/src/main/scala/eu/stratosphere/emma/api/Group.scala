package eu.stratosphere.emma.api

import eu.stratosphere.emma.api.model.Identity

/**
 * Created by alexander on 10/2/14.
 */
case class Group[K, +V](key: K, values: V) extends Identity[K] {
  def identity = key
}
