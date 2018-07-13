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
package labyrinth.jobs

import labyrinth.BagOperatorOutputCollector
import labyrinth.CFLConfig
import labyrinth.ElementOrEvent
import labyrinth.KickoffSource
import labyrinth.LabyNode
import labyrinth.LabySource
import labyrinth.operators.Print
import labyrinth.partitioners.Always0
import labyrinth.partitioners.RoundRobin
import labyrinth.util.Unit

import labyrinth.operators._
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeHint
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.functions.sink.DiscardingSink
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object WordCountScala {

  private val stringSerializer = TypeInformation.of(classOf[String]).createSerializer(new ExecutionConfig)

  private val tuple2StringIntegerSerializer =
		TypeInformation.of(new TypeHint[Tuple2[String, Integer]]() {}).createSerializer(new ExecutionConfig)

  @throws[Exception]
  def main(args: Array[String]): scala.Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    CFLConfig.getInstance.terminalBBId = 0 // this will have to be found automatically

    val kickoffSource = new KickoffSource(0) // this as well
    env.addSource(kickoffSource).addSink(new DiscardingSink[Unit]) // kickoff source has no output

    val para = env.getParallelism
    val lines = List("foo bar foo foo bar lol lol lol foo rofl",
      " lol foo lol bar lol bar bar foo foo rofl foo",
      "foo bar foo foo bar lol lol lol foo rofl lasagna")

    // source to read line by line
    val input = new LabySource[String](env.fromCollection(lines).javaStream, 0,
			TypeInformation.of(new TypeHint[ElementOrEvent[String]]() {}))

    val split = new LabyNode[String, String](
      "split2",
      ScalaOps.flatMap((s: String, coll: BagOperatorOutputCollector[String]) =>
				for(elem <- s.split(" ")) { coll.collectElement(elem) }),
      0,
      new RoundRobin[String](para),
      stringSerializer,
      TypeInformation.of(new TypeHint[ElementOrEvent[String]]() {})
    )
      .addInput(input, true)
      .setParallelism(para)

    val mapnode = new LabyNode[String, Tuple2[String, Integer]](
      "map-phase",
      ScalaOps.map((s: String) => new Tuple2[String, Integer](s, 1)),
      0,
      new RoundRobin[String](para),
      stringSerializer,
      TypeInformation.of(new TypeHint[ElementOrEvent[Tuple2[String, Integer]]]() {})
    )
      .addInput(split, true, false)
      .setParallelism(para)

    // count phase
    val reduceNode = new LabyNode[Tuple2[String, Integer], Tuple2[String, Integer]](
      "reduce-phase",
      ScalaOps.reduceGroup(
        t => t.f0,
        (old, current) => new Tuple2[String, Integer](old.f0, old.f1 + current.f1)
      ),
      0,
      new Always0[Tuple2[String, Integer]](para),
      tuple2StringIntegerSerializer,
      TypeInformation.of(new TypeHint[ElementOrEvent[Tuple2[String, Integer]]]() {})
    )
      .addInput(mapnode, true, false)
      .setParallelism(para)

    val printNode = new LabyNode[Tuple2[String, Integer], Unit](
      "print-phase",
      new Print[Tuple2[String, Integer]]("printcount"),
      0,
      new Always0[Tuple2[String, Integer]](1),
      tuple2StringIntegerSerializer,
      TypeInformation.of(new TypeHint[ElementOrEvent[Unit]]() {})
    )
      .addInput(reduceNode, true, false)
      .setParallelism(1)

    LabyNode.translateAll(env.getJavaEnv)
    System.out.println(env.getExecutionPlan)
    env.execute
  }
}
