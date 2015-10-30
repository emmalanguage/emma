package eu.stratosphere.emma.runtime.spark

import eu.stratosphere.emma.api.DataBag
import eu.stratosphere.emma.api.model.Identity
import eu.stratosphere.emma.runtime.AbstractStatefulBackend
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class StatefulBackend[S <: Identity[K] : ClassTag, K : ClassTag]
(sc: SparkContext, xs: RDD[S]) extends AbstractStatefulBackend[S, K] {

  // Create initial file
  writeStateToFile(xs)

  // todo: figure out how to delete the files (we can't delete it here in the update functions, because with haven't executed the dataflow yet)
  // We also can't just overwrite the old file, because the write might be occuring in parallel with the read
  // (at least as far as the Flink runtime is concerned, I mean the read probably keeps the file open while the topology is up)

  def fetchToStateLess() : RDD[S] = {
    readStateFromFile()
  }

  def updateWithZero[O: ClassTag](udf: S => DataBag[O]): RDD[O] = {

    val javaUdfResults : RDD[Either[S, O]] =
      readStateFromFile().flatMap(state =>
        udf(state).fetch().map(Right(_)) :+ Left(state)
      )

    writeNewState(javaUdfResults)
    filterRight(javaUdfResults)
  }

  def updateWithOne[U: ClassTag, O: ClassTag]
  (updates: RDD[U], updateKeySelector: U => K, udf: (S, U) => DataBag[O]): RDD[O] = {

    val javaUdfResults: RDD[Either[S, O]] =
      readStateFromFile().map(s => (s.identity, s))
        .cogroup(
          updates.map(x => (updateKeySelector(x), x))
        ).flatMap { case (_, (stateIt, updIt)) =>
        stateIt.headOption.map(state => {
          val updated =
            for (u <- updIt;
                 o <- udf(state, u).fetch())
              yield Right(o)
          updated ++ Iterable(Left(state))
        }).getOrElse(Iterable())
      }

    writeNewState(javaUdfResults)
    filterRight(javaUdfResults)
  }

  def updateWithMany[U: ClassTag, O: ClassTag]
  (updates: RDD[U], updateKeySelector: U => K, udf: (S, DataBag[U]) => DataBag[O]): RDD[O] = {

    val javaUdfResults : RDD[Either[S, O]] = readStateFromFile()
      .map(s => (s.identity, s))
      .cogroup(
        updates.map(x => (updateKeySelector(x), x))
      )
      .flatMap { case (_, (stateIt, updIt)) =>
      stateIt.headOption.map(state => {
        val upds = DataBag(updIt.toSeq)
        val updated =
          for (o <- udf(state, upds).fetch())
            yield Right(o)
        updated ++ Iterable(Left(state))
      }).getOrElse(Iterable())
    }

    writeNewState(javaUdfResults)
    filterRight(javaUdfResults)
  }

  private def readStateFromFile() : RDD[S] = {
    sc.objectFile[S](currentFileName())
  }

  private def writeNewState[O: ClassTag](javaUdfResults: RDD[Either[S, O]]) = {
    val newState = javaUdfResults.flatMap {
      case Left(l) => Iterable(l)
      case Right(_) => Iterable()
    }

    seqNumber += 1
    writeStateToFile(newState)
  }

  // todo: use more optimal serialization as saveAsObjectFile uses Java default
  private def writeStateToFile(state : RDD[S]) = {
    state.saveAsObjectFile(currentFileName())
  }

  private def filterRight[O: ClassTag](s: RDD[Either[S, O]]): RDD[O] = {
    s.flatMap {
      case Left(_) => Iterable()
      case Right(r) => Iterable(r)
    }
  }
}
