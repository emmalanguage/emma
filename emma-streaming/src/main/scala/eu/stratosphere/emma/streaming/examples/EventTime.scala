package eu.stratosphere.emma.streaming.examples

import eu.stratosphere.emma.api.DataBag
import eu.stratosphere.emma.streaming.api._

import eu.stratosphere.emma.streaming.extended.ExtendedStreamBag._

/**
  * Sketch of expressing event time computation with StreamBag.
  * The basic idea is to have a nested StreamBag[StreamBag[A]] where the outer
  * StreamBag represents processing time, the inner event time.
  */
object EventTime {

  /**
    * Assigns a new StreamBag[A] to every event that contains only that single event
    * placed in the Stream at event time.
    *
    * @param ts
    * Timestamp assigning function.
    * @param xs
    * Original stream with out-of-order data.
    * @tparam A
    * Type of data-points in the stream.
    * @return
    * Nested StreamBag where the outer StreamBag denotes processing time, the inner denotes
    * event time.
    */
  def toEventTimeStream[A](ts: A => Int)(xs: StreamBag[A]): StreamBag[StreamBag[A]] =
    xs.map(x => StreamBag.unit[A](x).postpone(ts(x)))

  /**
    * Applies a transformation to a stream based on event time.
    *
    * @param ts
    *           Timestamp function that assign event timestamp to values.
    * @param xs
    *           Stream of values in processing time.
    * @param fs
    *           Transformation wished to apply on event time order, based on timestamp function.
    * @tparam A
    *           Type of input data.
    * @tparam B
    *           Type of output data.
    * @return
    *        Transformed nested stream. Outer StreamBag corresponds to processing time, inner StreamBag
    *        corresponds to event time.
    */
  def eventTimeStreamProcessing[A, B](ts: A => Int)(xs: StreamBag[A], fs: StreamBag[A] => StreamBag[B])
  : StreamBag[StreamBag[B]] = {
    // Accumulate all the event time StreamBags that contain one element.
    // Note that this yields Stream of StreamBags, using another primitive than StreamBag.
    // Also the scan here creates "collection" type (StreamBag). It might not be straightforward to
    // optimize this.
    val accEventTimeSB: Stream[StreamBag[A]] = toEventTimeStream(ts)(xs)
      .scan(StreamBag.empty[A])(identity, { case (sb1, sb2) => sb1.plus(sb2) })

    // Simply mapping the user defined transformation to the StreamBags,
    // then filtering the events of the inner event time bags, so that
    // only those events are shown at time T that happened before time T.
    // (This is needed e.g. for not to have duplicate windows when we
    // flatten the stream.)
    for {
      Timed(procTime, processedSB) <- StreamBag.fromStream(accEventTimeSB.map[StreamBag[B]](fs)).withTimestamp
    } yield {
      for {
        Timed(eventTime, x) <- processedSB.withTimestamp
        if eventTime <= procTime
      } yield {
        x
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val xs: StreamBag[(String, Int)] = Seq(
      DataBag(),
      DataBag(),
      DataBag(),
      DataBag(Seq(("anna", 2))),
      DataBag(Seq(("emma", 1), ("hops", 4), ("ok", 3))))

    // Creates a sliding window of size 2, slide-size 1, and concatenates the elements
    // in the window. For clarity getAtTime(t) is used to have Bags as a window rather than
    // StreamBags.
//    def eventTimeWindowSpec(xs: StreamBag[(String, Int)]): StreamBag[(Window, String)] = {
//      val windowedStream: StreamBag[(Window, StreamBag[(String, Int)])] =
//        Windows.createWindows(xs, Windows.slidingWindow(2, 1))
//      val aggregatedWindows: StreamBag[(Window, String)] = for {
//        Timed(t, (w, sb)) <- windowedStream.withTimestamp
//      } yield {
//        (w, sb.scan("")(_._1, (x, y) => x ++ y).getAtTime(t))
//      }
//
//      aggregatedWindows
//    }
//
//    val eventTimeWindows = eventTimeStreamProcessing[(String, Int), (Window, String)](_._2)(xs, eventTimeWindowSpec)
//
//    println(StreamBag.flatten(eventTimeWindows).showFirstN(5))
  }
}
