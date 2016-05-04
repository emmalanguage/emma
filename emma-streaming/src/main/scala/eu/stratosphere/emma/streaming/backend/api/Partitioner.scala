package eu.stratosphere.emma.streaming.backend.api

/**
 * Partitioner to decide for an output value which parallel
 * instances of the next operator should receive it.
 *
 * @tparam OUT
 * Type of output.
 */
trait Partitioner[OUT] {

  def partition(value: OUT, numOfPartitions: Int): List[Int]

}
