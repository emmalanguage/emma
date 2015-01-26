package eu.stratosphere.emma.util

class Counter {

  private var value = 0

  def advance = this.synchronized {
    value += 1
    this
  }

  def get = this.synchronized {
    value
  }
}
