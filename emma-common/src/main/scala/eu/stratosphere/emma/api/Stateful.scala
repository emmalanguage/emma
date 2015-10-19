package eu.stratosphere.emma.api

import com.esotericsoftware.kryo.KryoException
import com.twitter.chill.ScalaKryoInstantiator
import eu.stratosphere.emma.api.model._

/**
 * Abstractions for stateful collections.
 */
object Stateful {

  val kryo = (new ScalaKryoInstantiator).newKryo()

  /**
   * An abstraction for stateful collections.
   *
   * Allows pointwise updates with zero or one additional arguments.
   *
   * @param state A mutable Map used for indexed state maintenance in local execution.
   * @tparam S The element of the stored state.
   * @tparam K The key type.
   */
  sealed class Bag[S <: Identity[K], K] private(val state: Map[K, S]) {

    var updateInProgress = false // This is for detecting when the user calls .bag() from the UDF of updateWith*

    /**
     * Private constructor that transforms a DataBag into a stateful Bag.
     *
     * @param bag A DataBag containing elements with identity to be indexed and managed by this stateful Bag.
     */
    private[api] def this(bag: DataBag[S]) = {
      this ({
        try bag.vals.map(s => {
          s.identity -> kryo.copy(s)
        }).toMap catch {
          case e: KryoException =>
            throw new Exception("""Kryo serializer could not copy state object. A likely cause is that the class of the
                                |state object is an inner class. Try moving it to a companion object.
                              """.stripMargin, e)
        }})
    }


    /**
     * Computes and flattens a delta without additional inputs. The UDF `f` is allowed to modify the state element `s`,
     * however with the restriction that the modification should not affect it's identity.
     *
     * @param f the update function to be applied for each point in the distributed state.
     * @tparam B The type of the output elements.
     * @return The flattened result of all update invocations.
     */
    def updateWithZero[B](f: S => DataBag[B]): DataBag[B] = checkNoBagAccess {
      for {
        s <- DataBag(state.values.toList)
        b <- f(s)
      } yield b
    }

    /**
     * Computes and flattens the `leftJoin` with `updates` which passes for each update element `u` it's
     * corresponding state element `s` to the update function `f`. The UDF `f` is allowed to modify the current
     * state element `s`, however with the restriction that the modification should not affect it's identity.
     *
     * @param updates A collection of inputs to be joined with.
     * @param k A key extractor function for matching `k(u) == s.identity`.
     * @param f A UDF to be applied per pair `f(s, u)` as described above.
     * @tparam A The type of the `updates` elements.
     * @tparam B The type of the output elements.
     * @return The flattened result of all update invocations.
     */
    def updateWithOne[A, B](updates: DataBag[A])
      (k: A => K, f: (S, A) => DataBag[B]): DataBag[B] = checkNoBagAccess {
      for {
        a <- updates
        s <- DataBag(state.get(k(a)).toList)
        b <- f(s, a)
      } yield b
    }

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
     * @tparam A The type of the update elements in `ys`.
     * @tparam B The type of the output elements.
     * @return The flattened collection
     */
    def updateWithMany[A, B](updates: DataBag[A])
      (k: A => K, f: (S, DataBag[A]) => DataBag[B]): DataBag[B] = checkNoBagAccess {
      for {
        s <- DataBag(state.values.toList)
        b <- f(s, updates withFilter { k(_) == s.identity })
      } yield b
    }


    /**
     * Converts the stateful bag into an immutable Bag.
     *
     * @return A Databag containing the set of elements contained in this stateful Bag.
     */
    def bag(): DataBag[S] = {
      if (updateInProgress)
        throw new StatefulAccessedFromUdfException()

      DataBag((for (x <- state) yield kryo.copy(x._2)).toSeq)
    }



    private def checkNoBagAccess[T](body: => T): T = {
      updateInProgress = true
      val result = body
      updateInProgress = false
      result
    }
  }
}
