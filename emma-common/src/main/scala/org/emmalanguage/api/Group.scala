package org.emmalanguage
package api

case class Group[K, +V](key: K, values: V)
