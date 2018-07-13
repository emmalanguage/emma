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

import compiler.Memo
import org.emmalanguage.labyrinth.CFLConfig
import org.emmalanguage.labyrinth.ElementOrEvent
import org.emmalanguage.labyrinth.ElementOrEventTypeInfo
import org.emmalanguage.labyrinth.KickoffSource
import org.emmalanguage.labyrinth.LabyNode
import org.emmalanguage.labyrinth.operators.ConditionNode
import org.emmalanguage.labyrinth.operators.ScalaOps
import org.emmalanguage.labyrinth.partitioners.Always0

import org.apache.flink.api.java.typeutils.PojoTypeInfo
import org.apache.flink.streaming.api.functions.sink.DiscardingSink
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

object SimpleCFWhileLoop {

//  //        var i = 0
//  //        while (i < 100) i += 1
//  //        println(i)
//
//  @whileLoop def while$1(i: Int): Unit = {
//    val x$1 = i < 100
//    @loopBody def body$1(): Unit = {
//      val i$3 = i + 1
//      while$1(i$3)
//    }
//    @suffix def suffix$1(): Unit = {
//      println(i)
//    }
//    if (x$1) body$1()
//    else suffix$1()
//  }
//  while$1(0)

  @throws[Exception]
  def main(args: Array[String]): scala.Unit = {


    // outer    -> 0
    // while$1  -> 1
    // body$1   -> 2
    // suffix$1 -> 3


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    PojoTypeInfo.
      registerCustomSerializer(classOf[ElementOrEvent[_]], new ElementOrEvent.ElementOrEventSerializerFactory)

    CFLConfig.getInstance.terminalBBId = 3

    val kickoffSrc = new KickoffSource(0, 1)
    env.addSource(kickoffSrc).addSink(new DiscardingSink[labyrinth.util.Unit])



    val n1 = new LabyNode[labyrinth.util.Nothing, Int](
      "fromNothing",
      ScalaOps.fromNothing(() => {val tmp = 0; tmp }),
      0,
      new Always0[labyrinth.util.Nothing](1),
      null,
      new ElementOrEventTypeInfo[Int](Memo.typeInfoForType[Int])
    )

    val i = LabyNode.phi[Int]("i", 1, new Always0[Int](1), null,
      new ElementOrEventTypeInfo[Int](Memo.typeInfoForType[Int]))

    val x$1 = new LabyNode[Int, java.lang.Boolean]("x$1", ScalaOps.singletonBagOperator(_ < 100), 1,
      new Always0[Int](1), null, new ElementOrEventTypeInfo[java.lang.Boolean](Memo.typeInfoForType[java.lang.Boolean]))
      .addInput(i, true, false)

    val i$3 = new LabyNode[Int, Int]("i$3", ScalaOps.singletonBagOperator(_ + 1), 2,
      new Always0[Int](1), null, new ElementOrEventTypeInfo[Int](Memo.typeInfoForType[Int]))
      .addInput(i, false, true)

    i.addInput(i$3, false, true)

    val printlnNode = new LabyNode[Int, Unit](
      "map",
      ScalaOps.map( (i:Int) => println(i)),
      3,
      new Always0[Int](1),
      null,
      new ElementOrEventTypeInfo[Unit](Memo.typeInfoForType[Unit])
    )
      .addInput(i, false, true)

    val ifCondNode = new LabyNode(
      "ifCondNode",
      new ConditionNode(  //[java.lang.Boolean, util.Unit]
        Array(2, 1), //vigyazat!
        Array(3)
      ),
      1,
      new Always0[java.lang.Boolean](1), null,
      new ElementOrEventTypeInfo[labyrinth.util.Unit](Memo.typeInfoForType[labyrinth.util.Unit]))
      .addInput(x$1, true, false)

    i.addInput(n1, false, true)



    LabyNode.translateAll(env.getJavaEnv)

    env.execute
  }


}
