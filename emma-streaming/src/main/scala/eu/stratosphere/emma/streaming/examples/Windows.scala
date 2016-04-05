package eu.stratosphere.emma.streaming.examples

import eu.stratosphere.emma.api.DataBag

import eu.stratosphere.emma.streaming.api._

import eu.stratosphere.emma.streaming.extended.ExtendedStreamBag._

/*
  It might be advisable to define window streams with a separate end and start.
  The WindowStart would be received at the start of the window, while WindowEnd at
  the end. Other options and reasons of sticking to this representation:
    1. Window = W(start, end)
      have to place somewhere in the stream so that it is consistent with the stream time
      a) If placed at the start, we omit data-driven windows as sometimes the end cannot be
          determined at starting the window.
      b) If placed only at the end, we might not be able to express, that a computation shall be done
          in the future. Thus we cannot do (or it is much harder to do) optimizations in the future
          with preaggregates.
          (This is the current implementation.)
      c) If placed everywhere between start and end. This yields the same problem as in a)
    2. Window = WStart(start) U WEnd(start, end)
      WStart is placed at start and WEnd is placed at end. This seems reasonable as it addresses the previous issues.
 */

/**
  * Time interval (left-closed, right-open). Start time data points
  * will be included, end time data points will be excluded.
  *
  * @param start
  * Start time of the window (included).
  * @param end
  * End time of the window (excluded).
  */
case class Window(start: Int, end: Int)

// todo tests
object Windows {

  def slidingWindow(windowSize: Int, slideSize: Int): StreamBag[Window] =
    (for {
      n <- StreamBag.fromStream[Int](Stream.naturals)
      if n % slideSize == 0
    } yield {
      Window(n, n + windowSize)
    })
      /*
        The window stream is postponed because we are only able to compute
        the window when all the time elapsed for that window.
       */
      .postpone(windowSize - 1)

  def tumblingWindow(windowSize: Int): StreamBag[Window] =
    slidingWindow(windowSize, windowSize)

  def sessionWindowBasedOnStream[A](xs: StreamBag[A], sessionTimeout: Int) = {
    case class SessWinState(lastSeen: Int, start: Int, emit: Boolean)

    val ts: Stream[Option[Int]] = xs.timesNotEmpty()
    ts.scan[SessWinState](SessWinState(0, 0, false)) {
      case (SessWinState(lastSeen, start, lastEmitted), None) =>
        SessWinState(lastSeen, lastSeen, false)
      case (SessWinState(lastSeen, start, lastEmitted), Some(current)) =>
        val startOfWindow = if (lastEmitted) {
          lastSeen
        } else {
          start
        }
        val emitNow = current - lastSeen > sessionTimeout

        SessWinState(current, startOfWindow, emitNow)
    }
      .map { case SessWinState(end, start, emit) =>
        if (emit) {
          DataBag(Seq(Window(start, end)))
        } else {
          DataBag()
        }
      }
  }

  /*
    Implementation with stateful computation.
    todo Avoid creating empty session at the beginning of the stream.
   */
  def sessionWindowStateful[A](xs: StreamBag[A], sessionTimeout: Int) = {
    case class SessWinState(lastSeen: Int, start: Int)

    val ts = xs.withTimestamp.map(_.t)
    ts.stateful[SessWinState, Option[Window]](SessWinState(0, 0)) { case (SessWinState(lastSeen, start), t) =>
      if (t - lastSeen <= sessionTimeout) {
        (SessWinState(t, start), Option.empty[Window])
      } else {
        (SessWinState(t, t), Some(Window(start, t)))
      }
    }.withFilter(_.isDefined).map(_.get)
  }

  def createWindows[A](xs: StreamBag[A], ws: StreamBag[Window]): StreamBag[(Window, StreamBag[A])] =
    for {
      w <- ws
    } yield {
      (w,
        for {
          x <- xs.withTimestamp
          if w.start <= x.t && x.t < w.end
        } yield {
          x.v
        })
    }

  def main(args: Array[String]) {
    val xs: StreamBag[Int] = Stream.naturals
    val ys: StreamBag[Char] = Seq(DataBag(Seq('a', 'b', 'b')), DataBag(Seq('c', 'd')), DataBag(), DataBag(), DataBag(Seq('d', 'a', 'e')))
    val ys2: StreamBag[Char] = Seq(DataBag(), DataBag(), DataBag(Seq('a', 'b', 'b')), DataBag(Seq('c', 'd')), DataBag(), DataBag(), DataBag(Seq('d', 'a', 'e')))
    val zs: StreamBag[Int] = Seq(DataBag(Seq(2)), DataBag(Seq(3, 1)))
    val b: DataBag[String] = DataBag(Seq("anna", "emma"))

    //    println(createWindows(xs, tumblingWindow(3)))
    //    println(tumblingWindow(3))
    //    println(slidingWindow(3, 2))
    //    println(xs.mapPostpone(_ => 3))
    //    println(ys.accumulate())
    //
    //    println(xs.postpone(3))
    //    println(sessionWindow(ys, 2))
    //    println(sessionWindow(ys2, 1))
    println(ys.timesNotEmpty())
    println(sessionWindowBasedOnStream(ys, 0))
  }
}
