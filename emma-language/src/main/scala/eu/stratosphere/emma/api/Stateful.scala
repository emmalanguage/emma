package eu.stratosphere.emma.api

import eu.stratosphere.emma.api.model._
import scala.collection.mutable

/**
 * Abstractions for stateful collections.
 */
object Stateful {

  /**
   * An abstraction for statful collections.
   *
   * Allows pointwise updates with zero or one additional arguments.
   *
   * @param state A mutable Map used for indexed state maintenance in local execution.
   * @tparam A The element of the stored state.
   * @tparam K The key type.
   */
  sealed class Bag[A <: Identity[K], K] private(val state: mutable.Map[K, A]) {

    /**
     * Private constructor that transforms a DataBag into a stateful Bag.
     *
     * @param bag A DataBag containing elements with identity to be indexed and managed by this stateful Bag.
     */
    private[api] def this(bag: DataBag[A]) = this(bag.fold[mutable.Map[K, A]](mutable.Map.empty[K, A], s => mutable.Map((s.identity, s)), (x, y) => x ++ y))

    /**
     * Computes and applies a delta without additional inputs.
     *
     * @param f the update function to be applied for each point in the distributed state.
     */
    def update(f: A => Option[A]): DataBag[A] = DataBag(for (
      s <- state.values.toList;
      d <- f(s)) yield {
      state(d.identity) = d; d
    })

    /**
     * Computes and applies a delta with one additional input.
     *
     * @param f the update function to be applied for each point with matching update in the distributed state.
     * @tparam B A set of update messages to be passed as first extra agument to the update function.
     */
    def update[B <: Identity[K]](updates: DataBag[B])(f: (A, B) => Option[A]): DataBag[A] = DataBag(for (
      u <- updates.impl;
      s <- state.get(u.identity);
      d <- f(s, u)) yield {
      state(d.identity) = d; d
    })

    /**
     * Converts the stateful bag into an immutable Bag.
     *
     * @return A Databag containing the set of elements contained in this stateful Bag.
     */
    def bag(): DataBag[A] = DataBag((for (x <- state) yield x._2).toSeq)
  }

}
