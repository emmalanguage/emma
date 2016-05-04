package eu.stratosphere.emma.streaming.backend.api

/**
 * Simple trait that defines serialization and deserialization for a type.
 *
 * @tparam T
 * Type to serialize.
 */
trait Serializer[T] {

  def serialize(value: T): Array[Byte]

  def deserialize(bytes: Array[Byte]): T

}
