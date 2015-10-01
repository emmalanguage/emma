package eu.stratosphere.emma.runtime

import java.util.UUID

import eu.stratosphere.emma.api.DataBag
import eu.stratosphere.emma.api.model.Identity
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.{TypeSerializerInputFormat, TypeSerializerOutputFormat}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.util.Collector

import scala.reflect.ClassTag


class StatefulBackend[S <: Identity[K] : ClassTag : TypeInformation, K: ClassTag : TypeInformation]
        (env: ExecutionEnvironment, xs: DataSet[S]) extends AbstractStatefulBackend[S, K] {

  private val STATEFUL_BASE_PATH = String.format("%s/emma/stateful", System.getProperty("java.io.tmpdir"))

  private val uuid = UUID.randomUUID()
  private val fileNameBase = s"$STATEFUL_BASE_PATH/$uuid"
  private var seqNumber = 0

  private def currentFileName() = s"${fileNameBase}_$seqNumber"
  private def oldFileName() = s"${fileNameBase}_${seqNumber - 1}"

  private val typeInformation = xs.getType()


  // Create initial file
  xs.write(new TypeSerializerOutputFormat[S](), currentFileName(), FileSystem.WriteMode.NO_OVERWRITE)


  def updateWithMany[U: ClassTag : TypeInformation, O: ClassTag : TypeInformation]
        (updates: DataSet[U], updateKeySelector: U => K, udf: (S, DataBag[U]) => DataBag[O]): DataSet[O] = {

    val javaUdfResults = readStateFromFile().coGroup(updates).where(s => s.identity).equalTo(updateKeySelector).apply(
      (stateIt: Iterator[S], updIt: Iterator[U], out: Collector[Either[S, O]]) => {
        if (stateIt.hasNext) {
          val state = stateIt.next()
          val upds = DataBag(updIt.toSeq)
          for (o <- udf(state, upds).fetch())
            out.collect(Right(o))
          out.collect(Left(state))
        }
    })

    val newState = javaUdfResults.flatMap ((x, out: Collector[S]) => x match {
      case Left(l) => out.collect(l)
      case Right(_) =>
    })

    seqNumber += 1
    newState.write(new TypeSerializerOutputFormat[S](), currentFileName(), FileSystem.WriteMode.NO_OVERWRITE)
    // new File(oldFileName()).delete()
    // todo: figure out how to delete the file (we can't delete it here, because with haven't executed the dataflow yet)
    // We also can't just overwrite the old file here, because the write might be occuring in parallel with the read
    // (at least as far as the Flink runtime is concerned, I mean the read probably keeps the file open while the topology is up)

    javaUdfResults.flatMap ((x, out: Collector[O]) => x match {
      case Left(_) =>
      case Right(r) => out.collect(r)
    })
  }

  def fetchToStateLess() : DataSet[S] = {
    readStateFromFile()
  }


  private def readStateFromFile() : DataSet[S] = {
    env.readFile(new TypeSerializerInputFormat[S](typeInformation), currentFileName())
  }
}
