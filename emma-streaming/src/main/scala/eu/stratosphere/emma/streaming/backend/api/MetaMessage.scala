package eu.stratosphere.emma.streaming.backend.api

/**
  * Type of message that contains no data.
  */
trait MetaMessage

/**
  * Timestamp to trigger order.
  * @param value
  *              Timestamp value.
  */
case class Timestamp(value: Int) extends MetaMessage

/**
  * Special message to mark end of the stream.
  * Useful for static inputs.
  */
case class EndOfStream() extends MetaMessage

