package eu.stratosphere.emma.streaming.examples.webshop

import scala.collection.immutable.TreeMap

trait TimedBuffer[T, A] {
  def getRange(from: T, to: T): Iterable[A]
  def append(x: A): TimedBuffer[T, A]
  def removeOlderThan(t: T): TimedBuffer[T, A]
}

case class TreeTimedBuffer[T, A](tree: TreeMap[T, Seq[A]], time: A => T)(implicit ordering: Ordering[T])
  extends TimedBuffer[T, A] {

  def this(time: A => T)(implicit ord: Ordering[T]) = this(new TreeMap[T, Seq[A]]()(ord), time)(ord)

  override def getRange(from: T, to: T): Iterable[A] = tree
    .to(to)
    .iteratorFrom(from)
    .flatMap(_._2)
    .toIterable

  override def removeOlderThan(t: T): TreeTimedBuffer[T, A] = TreeTimedBuffer(tree.from(t), time)

  override def append(x: A): TreeTimedBuffer[T, A] = {
    val t = time(x)
    val tSq = tree.getOrElse(t, Seq()) :+ x
    TreeTimedBuffer(tree.insert(t, tSq), time)
  }
}
