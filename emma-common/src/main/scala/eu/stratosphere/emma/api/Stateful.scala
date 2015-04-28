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
    @deprecated("Use updateWith instead.", "28.04.2015")
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
    @deprecated("Use updateWith instead.", "28.04.2015")
    def update[B <: Identity[K]](updates: DataBag[B])(f: (A, B) => Option[A]): DataBag[A] = DataBag(for (
      u <- updates.impl;
      s <- state.get(u.identity);
      d <- f(s, u)) yield {
      state(d.identity) = d; d
    })

    /**
     * Computes and flattens a delta without additional inputs. The UDF `f` is allowed to modify the state element `s`,
     * however with the restriction that the modification should not affect it's identity.
     *
     * @param f the update function to be applied for each point in the distributed state.
     * @tparam B The type of the output elements.
     * @return The flattened result of all update invocations.
     */
    def updateWith[B](f: A => DataBag[B]): DataBag[B] =
      for (s <- bag(); d <- f(s)) yield d

    /**
     * Computes and flattens the `leftJoin` with `updates` which passes for each update element `u` it's
     * corresponding state element `s` to the update function `f`. The UDF `f` is allowed to modify the current
     * state element `s`, however with the restriction that the modification should not affect it's identity.
     *
     * @param updates A collection of inputs to be joined with.
     * @param f A UDF to be applied per pair `f(s, u)` as described above.
     * @tparam B The type of the `updates` elements.
     * @tparam C The type of the output elements.
     * @return The flattened result of all update invocations.
     */
    def updateWith[B <: Identity[K], C](updates: DataBag[B])(f: (A, B) => DataBag[C]): DataBag[C] =
      for {
        u <- updates
        s <- DataBag(state.get(u.identity).toSeq)
        d <- f(s, u)
      } yield d

    /**
     * Computes and flattens the `nestJoin(p, f)` which nests the current state elements `s` against their
     * associated bag of update elements. The UDF `f` is allowed to modify the state element `s`, however with the
     * restriction that the modification should not affect it's identity.
     *
     * == Nest Join
     *
     * A `nestJoin` is an algebraic operator which applies a function `f` to each pair `x`, `G p_x ys`.
     *
     * {{{
     * nestJoin(p, f)(xs, ys) = for (x <- xs) f(x, (for (y <- ys; if p(x, y)) yield y))
     * }}}
     *
     * The left argument `x` is taken from the left input `xs`.
     * The right argument denotes the group formed by applying a 'per x' filter `p(x, y)` to each element `y` from the
     * right input `ys`.
     *
     * One can think of a `nestJoin` as a half-`join`, half-`cogroup` operator.
     *
     * @param updates A collection of inputs to be matched per `x`.
     * @param k A key extractor function for matching `k(u) == s.identity`.
     * @param f A UDF to be applied per pair `f(x, G p_x ys)` as described above.
     * @tparam B The type of the `ys` elements.
     * @tparam C The type of the output elements.
     * @return The flattened collection
     */
    def updateWith[B, C](updates: DataBag[B])(k: B => K, f: (A, DataBag[B]) => DataBag[C]): DataBag[C] =
      for {
        s <- bag()
        d <- f(s, for (u <- updates; if k(u) == s.identity) yield u)
      } yield d

    /**
     * Converts the stateful bag into an immutable Bag.
     *
     * @return A Databag containing the set of elements contained in this stateful Bag.
     */
    def bag(): DataBag[A] = DataBag((for (x <- state) yield x._2).toSeq)
  }

}
