/*
 * Copyright © 2014 TU Berlin (emma@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.emmalanguage
package labyrinth.operators

import api.DataBag
import api.alg.Alg
import api.Group
import api.Meta
import compiler.Memo
import io.csv.CSV
import io.csv.CSVScalaSupport
import labyrinth.util.SerializedBuffer
import labyrinth.BagOperatorOutputCollector
import labyrinth.CFLConfig
import labyrinth.ElementOrEvent
import labyrinth.KickoffSource
import labyrinth.LabyNode
import labyrinth.partitioners.Partitioner
import labyrinth.util.SocketCollector

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.typeutils.PojoTypeInfo
import org.apache.flink.core.fs.FileInputSplit
import org.apache.flink.streaming.api.functions.sink.DiscardingSink
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.collection.JavaConverters
import scala.util.Either

import java.util

object ScalaOps {

  def map[IN, OUT](f: IN => OUT): FlatMap[IN, OUT] = {
    new FlatMap[IN, OUT]() {
      override def pushInElement(e: IN, logicalInputId: Int): Unit = {
        super.pushInElement(e, logicalInputId)
        out.collectElement(f(e))
      }
    }
  }

  def flatMap[IN, OUT](f: (IN, BagOperatorOutputCollector[OUT]) => Unit): FlatMap[IN, OUT] = {
    new FlatMap[IN, OUT]() {
      override def pushInElement(e: IN, logicalInputId: Int): Unit = {
        super.pushInElement(e, logicalInputId)
        f(e, out)
      }
    }
  }

  def flatMapDataBagHelper[IN, OUT](f: IN => DataBag[OUT]): FlatMap[IN, OUT] = {

    val lbda = (e: IN, coll: BagOperatorOutputCollector[OUT]) =>
      for(elem <- f(e)) yield { coll.collectElement(elem) }

    new FlatMap[IN, OUT]() {
      override def pushInElement(e: IN, logicalInputId: Int): Unit = {
        super.pushInElement(e, logicalInputId)
        lbda(e, out)
      }
    }

  }

  def withFilter[T](f: T => Boolean): FlatMap[T, T] = {
    new FlatMap[T, T]() {
      override def pushInElement(e: T, logicalInputId: Int): Unit = {
        super.pushInElement(e, logicalInputId)
        if (f(e)) out.collectElement(e)
      }
    }
  }

  def fromNothing[OUT](f: () => OUT ): BagOperator[org.emmalanguage.labyrinth.util.Nothing,OUT] = {
    new BagOperator[org.emmalanguage.labyrinth.util.Nothing,OUT]() {
      override def openOutBag() : Unit = {
        super.openOutBag()
        out.collectElement(f())
        out.closeBag()
      }
    }
  }

  def empty[OUT]: BagOperator[org.emmalanguage.labyrinth.util.Nothing, OUT] = {
    new BagOperator[org.emmalanguage.labyrinth.util.Nothing,OUT]() {
      override def openOutBag() : Unit = {
        super.openOutBag()
        out.closeBag()
      }
    }
  }

  // This should be variant in the input type, to allow for more specific types than Seq[IN], such as List[IN].
  def fromSingSrcApply[IN, T <: Seq[IN]](): SingletonBagOperator[T, IN] = {
    new SingletonBagOperator[T, IN] {
      override def pushInElement(e: T, logicalInputId: Int): Unit = {
        super.pushInElement(e, logicalInputId)
        e.foreach(x => out.collectElement(x))
      }
    }
  }

  def foldGroupValues[K,IN,OUT](keyExtractor: IN => K, i: IN => OUT, f: (OUT, OUT) => OUT): BagOperator[IN, OUT] = {

    new FoldGroupValues[K, IN, OUT]() {

      override protected def keyExtr(e: IN): K = keyExtractor(e)

      override def openOutBag(): Unit = {
        super.openOutBag()
        hm = new util.HashMap[K, OUT]
      }

      override def pushInElement(e: IN, logicalInputId: Int): Unit = {
        super.pushInElement(e, logicalInputId)
        val key = keyExtr(e)
        val g = hm.get(key)
        if (g == null) {
          hm.put(key, i(e))
        } else {
          hm.replace(key, f(g, i(e)))
        }
      }

      override def closeInBag(inputId: Int): Unit = {
        super.closeInBag(inputId)

        import scala.collection.JavaConversions._

        for (e <- hm.entrySet) {
          out.collectElement(e.getValue)
        }
        hm = null
        out.closeBag()
      }
    }
  }

  def foldGroup[K,IN,OUT](keyExtractor: IN => K, i: IN => OUT, f: (OUT, OUT) => OUT):
  BagOperator[IN, Group[K, OUT]] = {

    new FoldGroup[K, IN, OUT]() {

      override protected def keyExtr(e: IN): K = keyExtractor(e)

      override def openOutBag(): Unit = {
        super.openOutBag()
        hm = new util.HashMap[K, OUT]
      }

      override def pushInElement(e: IN, logicalInputId: Int): Unit = {
        super.pushInElement(e, logicalInputId)
        val key = keyExtr(e)
        val g = hm.get(key)
        if (g == null) {
          hm.put(key, i(e))
        } else {
          hm.replace(key, f(g, i(e)))
        }
      }

      override def closeInBag(inputId: Int): Unit = {
        super.closeInBag(inputId)

        import scala.collection.JavaConversions._

        for (e <- hm.entrySet) {
          out.collectElement(Group(e.getKey, e.getValue))
        }
        hm = null
        out.closeBag()
      }
    }
  }

  def foldGroupAlgHelper[K,IN,OUT](extr: IN => K, alg: Alg[IN,OUT]): BagOperator[IN, Group[K,OUT]] = {
    foldGroup[K,IN,OUT](extr, alg.init, alg.plus)
  }

  def reduceGroup[K,A](keyExtractor: A => K, f: (A, A) => A): BagOperator[A, A] = {
    foldGroupValues(keyExtractor, (x:A) => x, f)
  }

  def fold[A,B](zero: B, init: A => B, plus: (B, B) => B): BagOperator[A,B] = {
    new Fold[A,B] {
      override def openInBag(logicalInputId: Int): Unit = {
        super.openInBag(logicalInputId)
        assert (host.subpartitionId == 0) // Parallelism should be 1.
        result = zero
      }

      override def pushInElement(e: A, logicalInputId: Int): Unit = {
        super.pushInElement(e, logicalInputId)
        result = plus(result, init(e))
      }

      override def closeInBag(inputId: Int): Unit = {
        super.closeInBag(inputId)
        out.collectElement(result)
        out.closeBag()
      }
    }
  }

  def foldAlgHelper[A,B](alg: Alg[A,B]): BagOperator[A,B] = {
    fold(alg.zero, alg.init, alg.plus)
  }

  def reduce[A](zero:A, plus: (A,A) => A): BagOperator[A,A] = {
    fold(zero, e => e, plus)
  }

  def joinGeneric[IN, K](keyExtractor: IN => K): JoinGeneric[IN, K] = {
    new JoinGeneric[IN, K] {
      override protected def keyExtr(e: IN): K = keyExtractor(e)
    }
  }

  def cross[A,B]: BagOperator[Either[A, B], (A, B)] =
    new Cross[A,B]

  def joinScala[A,B,K](extrA: A => K, extrB: B => K): JoinScala[A,B,K] =
    new JoinScala[A,B,K] {
      override def keyExtr1(e: A) = extrA(e)
      override def keyExtr2(e: B) = extrB(e)
    }

  def singletonBagOperator[IN, OUT](f: IN => OUT): SingletonBagOperator[IN, OUT] = {
    new SingletonBagOperator[IN, OUT] {
      override def pushInElement(e: IN, logicalInputId: Int): Unit = {
        super.pushInElement(e, logicalInputId)
        out.collectElement(f(e))
      }
    }
  }

  def union[T](): Union[T] = {
    new Union[T]
  }

  def textSource: BagOperator[String, InputFormatWithInputSplit[String, FileInputSplit]] = {
    new CFAwareTextSource
  }

  def textReader: BagOperator[InputFormatWithInputSplit[String, FileInputSplit], String] = {
    new CFAwareFileSourceParaReader[String, FileInputSplit](Memo.typeInfoForType[String])
  }

  def toCsvString[T](implicit converter: org.emmalanguage.io.csv.CSVConverter[T]): BagOperator[Either[T, CSV], String] =
  {
    new ToCsvString[T] {

      override def openOutBag(): Unit = {
        super.openOutBag()
        buff = new SerializedBuffer[Either[T, CSV]](inSer)
        csvConv = converter
        closed(0) = false
        closed(1) = false
      }

      override def pushInElement(e: Either[T, CSV], logicalInputId: Int): Unit = {
        super.pushInElement(e, logicalInputId)
        if (logicalInputId == 0) { // input data
          if (!csvWriterInitiated) {
            buff.add(e)
          }
          else { // generate string from input
            out.collectElement(generateString(e))
          }
        }
        else { // CSV info
          csvInfo = e.right.get
          csvInfoInitiated = true
          csvWriter = CSVScalaSupport[T](csvInfo).writer()
          csvWriterInitiated = true
          // generate strings from buffered input
          val it = buff.iterator()
          while (it.hasNext) { out.collectElement(generateString(it.next())) }
        }
      }

      override def closeInBag(inputId: Int): Unit = {
        super.closeInBag(inputId)

        closed(inputId) = true

        if (closed(0) && closed(1) ) {
          csvWriter.close()
          out.closeBag()
        }
      }

      def generateString(e: Either[T, CSV]): String = {
        val rec = Array.ofDim[String](csvConv.size)
        csvConv.write(e.left.get, rec, 0)(csvInfo)
        csvWriter.writeRowToString(rec)
      }
    }
  }

  def writeString: BagOperator[String, scala.Unit] = {
    new FileSinkString
  }

  def condNode(trueBbIds: Seq[Int], falseBbIds: Seq[Int])
  : BagOperator[Boolean, org.emmalanguage.labyrinth.util.Unit] = {
    new ConditionNodeScalaReally(trueBbIds.toArray, falseBbIds.toArray)
  }

  def collectToClient[T](env: StreamExecutionEnvironment, in: LabyNode[_, T], bbId: Int) = {
    labyrinth.util.Util.collect(env.getJavaEnv, in, bbId)
  }
}



class ConditionNodeScalaReally(val trueBranchBbIds: Array[Int], val falseBranchBbIds: Array[Int])
  extends SingletonBagOperator[Boolean, labyrinth.util.Unit] {

  def this(trueBranchBbId: Int, falseBranchBbId: Int) {
    this(Array[Int](trueBranchBbId), Array[Int](falseBranchBbId))
  }

  override def pushInElement(e: Boolean, logicalInputId: Int): Unit = {
    super.pushInElement(e, logicalInputId)
    for (b <- if (e) trueBranchBbIds else falseBranchBbIds) {
      out.appendToCfl(b)
    }
  }
}


object LabyStatics {
  def translateAll(implicit env: StreamExecutionEnvironment): Unit = LabyNode.translateAll(env.getJavaEnv)
  def setTerminalBbid(id: Int): Unit = CFLConfig.getInstance().terminalBBId = id
  def setKickoffSource(ints: Int*)(implicit env: StreamExecutionEnvironment): Unit = {
    val kickoffSrc = new KickoffSource(ints:_*)
    env.addSource(kickoffSrc)(org.apache.flink.api.scala.createTypeInformation[org.emmalanguage.labyrinth.util.Unit])
      .addSink(new DiscardingSink[org.emmalanguage.labyrinth.util.Unit])
  }
  def registerCustomSerializer(): Unit = PojoTypeInfo.registerCustomSerializer(classOf[ElementOrEvent[_]],
    new ElementOrEvent.ElementOrEventSerializerFactory)

  def phi[T](name: String, bbId: Int, inputPartitioner: Partitioner[T],
    inSer: TypeSerializer[T], typeInfo: TypeInformation[ElementOrEvent[T]]) =
    LabyNode.phi(name, bbId, inputPartitioner, inSer, typeInfo)

  def executeAndGetCollectedNonBag[T: Meta](env: StreamExecutionEnvironment, socColl: SocketCollector[T]): T = {
    val arrayList: util.ArrayList[T] = labyrinth.util.Util.executeAndGetCollected(env.getJavaEnv, socColl)
    assert(arrayList.size() == 1)
    arrayList.get(0)
  }

  def executeAndGetCollectedBag[T: Meta](env: StreamExecutionEnvironment, socColl: SocketCollector[T]): DataBag[T] = {
    val arrayList: util.ArrayList[T] = labyrinth.util.Util.executeAndGetCollected(env.getJavaEnv, socColl)
    DataBag(JavaConverters.asScalaIteratorConverter(arrayList.iterator).asScala.toSeq)
  }

  def executeWithCatch(env: StreamExecutionEnvironment): Unit = {
    labyrinth.util.Util.executeWithCatch(env.getJavaEnv)
  }
}