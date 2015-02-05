package eu.stratosphere.emma.codegen.flink

import eu.stratosphere.emma.codegen.utils.DataflowCompiler
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.io.RemoteCollectorImpl
import org.apache.flink.api.java.operators.JoinOperator
import org.apache.flink.api.java.operators.JoinOperator.EquiJoin
import org.apache.flink.api.java.typeutils.{ResultTypeQueryable, TupleTypeInfo}
import org.apache.flink.api.java.{ClosureCleaner, ExecutionEnvironment}

import scala.reflect.runtime.universe._


object CodegenProofOfConcept {

  /**
   * To test this, do the following steps:
   *
   * 1) Tweak the Flink startup scripts:
   *
   * # in taskmanager.sh
   * $JAVA_RUN [...] -classpath "/tmp/emma/codegen:$FLINK_TM_CLASSPATH"
   * # in jobmanager.sh
   * $JAVA_RUN [...] -classpath "/tmp/emma/codegen:$FLINK_JM_CLASSPATH"
   *
   * 2) Start flink
   *
   * bin/start-cluster.sh
   *
   * 3) Run the jar with the following command
   *
   * java -Demma.classgen.dir=/tmp/emma/codegen -cp "/tmp/emma:/path/to/this.jar" eu.stratosphere.emma.codegen.CodegenProofOfConcept remote
   */
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: <jar> [remote|local]")
      System.exit(-1)
    }

    val env = if (args(0) == "remote")
      ExecutionEnvironment.createRemoteEnvironment("localhost", 6123)
    else
      ExecutionEnvironment.createLocalEnvironment()

    testCollect(env)
  }

  def testCodegen(env: ExecutionEnvironment) = {

    val quotedPlan = q"""

      val text = env.fromElements(
        "Who's there?",
        "I think I hear them. Stand, ho! Who's there?");

      val wordCounts = text
        .flatMap(new Tokenizer(separator))
        .groupBy(0)
        .sum(1);

      wordCounts.writeAsCsv("file:///tmp/codegen-proof-of-concept/output", WriteMode.OVERWRITE)

      env.execute("Word Count Example")
    """

    val dfTree =
      q"""
        object DataflowWrapper {
          import org.apache.flink.api.common.functions.FlatMapFunction
          import org.apache.flink.api.java.ExecutionEnvironment
          import org.apache.flink.api.java.DataSet
          import org.apache.flink.util.Collector
          import org.apache.flink.core.fs.FileSystem.WriteMode

          class Tokenizer(val separator: String) extends RichFlatMapFunction[String, org.apache.flink.api.java.tuple.Tuple2[String, Int]] {
            @Override
            def flatMap(line: String, out: Collector[org.apache.flink.api.java.tuple.Tuple2[String, Int]]): Unit = {
              for (word: String <- line.split(separator)) {
                out.collect(new org.apache.flink.api.java.tuple.Tuple2[String, Int](word, 1));
              }
            }
          }

          def run(env: ExecutionEnvironment, separator: String) = { $quotedPlan }
       }""".asInstanceOf[ImplDef]

    val dfCompiler = new DataflowCompiler()
    val dfSymbol = dfCompiler.compile(dfTree).asModule

    val separator = "'"

    dfCompiler.execute[Unit](dfSymbol, Array[Any](env, separator))
  }

  def testCollect(env: ExecutionEnvironment) = {
    val in1 = 1 to 10
    val in2 = 1 to 20
    DataflowWrapper2.run(env, in1, in2)
  }

  object DataflowWrapper {

    import org.apache.flink.api.java.ExecutionEnvironment
    import org.apache.flink.util.Collector

    class Multiplier(val separator: String) extends org.apache.flink.api.common.functions.RichFlatMapFunction[Int, org.apache.flink.api.java.tuple.Tuple2[Int, Int]] with ResultTypeQueryable[org.apache.flink.api.java.tuple.Tuple2[Int, Int]] {
      @Override
      def flatMap(x: Int, out: Collector[org.apache.flink.api.java.tuple.Tuple2[Int, Int]]): Unit = {
        for (a <- 1 to 10) out.collect(new org.apache.flink.api.java.tuple.Tuple2[Int, Int](x, a * x))
      }

      @Override
      def getProducedType = new TupleTypeInfo[org.apache.flink.api.java.tuple.Tuple2[Int, Int]](BasicTypeInfo.getInfoFor(classOf[Int]), BasicTypeInfo.getInfoFor(classOf[Int]))
    }

    def run(env: ExecutionEnvironment, separator: String) = {

      val text = env.fromElements(1, 2, 3, 4);

      val wordCounts = text
        .flatMap(new Multiplier(separator))
        .groupBy(0)
        .sum(1)

      wordCounts.cross(wordCounts)

      // local collection to store results in
      val collection = java.util.Collections.synchronizedCollection(new java.util.ArrayList[org.apache.flink.api.java.tuple.Tuple2[Int, Int]]())
      // collect results from remote in local collection
      RemoteCollectorImpl.collectLocal(wordCounts, collection)

      env.execute("Multiplier Example")
      System.out.println(collection);

      RemoteCollectorImpl.shutdownAll()
    }
  }

  object DataflowWrapper2 {

    import org.apache.flink.api.common.functions._
    import org.apache.flink.api.common.typeinfo.BasicTypeInfo
    import org.apache.flink.api.java.functions.KeySelector
    import org.apache.flink.api.java.{ExecutionEnvironment, tuple}
    import org.apache.flink.api.java.typeutils.{ResultTypeQueryable, TupleTypeInfo};

    class EmmaMapper00001 extends RichMapFunction[tuple.Tuple2[Int, Int], tuple.Tuple2[Int, Int]] with ResultTypeQueryable[tuple.Tuple2[Int, Int]] {

      override def map(x: tuple.Tuple2[Int, Int]): tuple.Tuple2[Int, Int] = new tuple.Tuple2[Int, Int](x.f0, x.f1);

      override def getProducedType = new TupleTypeInfo[tuple.Tuple2[Int, Int]](BasicTypeInfo.getInfoFor(classOf[Int]), BasicTypeInfo.getInfoFor(classOf[Int]))
    };

    class EmmaKeyX00002 extends KeySelector[Int, Int] with ResultTypeQueryable[Int] {
      override def getKey(x: Int): Int = x

      override def getProducedType = BasicTypeInfo.getInfoFor(classOf[Int])
    };

    class EmmaKeyY00003 extends KeySelector[Int, Int] with ResultTypeQueryable[Int] {
      override def getKey(y: Int): Int = y

      override def getProducedType = BasicTypeInfo.getInfoFor(classOf[Int])
    };

    class EmmaJoin00004 extends RichJoinFunction[Int, Int, tuple.Tuple2[Int, Int]] with ResultTypeQueryable[tuple.Tuple2[Int, Int]] {
      override def join(x: Int, y: Int): tuple.Tuple2[Int, Int] = new tuple.Tuple2[Int, Int](x, y);

      override def getProducedType = new TupleTypeInfo[tuple.Tuple2[Int, Int]](BasicTypeInfo.getInfoFor(classOf[Int]), BasicTypeInfo.getInfoFor(classOf[Int]))
    };

    def run(env: ExecutionEnvironment, scatter00001: Seq[Int], scatter00002: Seq[Int]) = {
      {
        val __input = {
          val __inx = env.fromCollection(scala.collection.JavaConversions.asJavaCollection(scatter00001.map((v) => v)));
          val __iny = env.fromCollection(scala.collection.JavaConversions.asJavaCollection(scatter00002.map((v) => v)));

          // direct variant
          val keyxSelector = new EmmaKeyX00002()
          val keyxType = org.apache.flink.api.java.typeutils.TypeExtractor.getKeySelectorTypes(keyxSelector, __inx.getType)
          val keyx = new org.apache.flink.api.java.operators.Keys.SelectorFunctionKeys[Int, Int](keyxSelector, __inx.getType, keyxType)

          val keyySelector = new EmmaKeyY00003()
          val keyyType = org.apache.flink.api.java.typeutils.TypeExtractor.getKeySelectorTypes(keyySelector, __iny.getType)
          val keyy = new org.apache.flink.api.java.operators.Keys.SelectorFunctionKeys[Int, Int](keyySelector, __inx.getType, keyxType)

          val generatedFunction = new JoinOperator.DefaultJoin.WrappingFlatJoinFunction(clean(env, new EmmaJoin00004()))

          // direct variant
          val y = new EquiJoin(
            __inx,
            __iny,
            keyx,
            keyy,
            generatedFunction,
            new TupleTypeInfo[tuple.Tuple2[Int, Int]](BasicTypeInfo.getInfoFor(classOf[Int]), BasicTypeInfo.getInfoFor(classOf[Int])),
            JoinHint.OPTIMIZER_CHOOSES,
            "unknown"
          )

          // variant 1
          // val x = __inx.join(__iny).where(new EmmaKeyX00002()).equalTo(new EmmaKeyY00003()).`with`(new EmmaJoin00004())

          y
        }.map(new EmmaMapper00001());

        val typeInformation = new TupleTypeInfo[tuple.Tuple2[Int, Int]](BasicTypeInfo.getInfoFor(classOf[Int]), BasicTypeInfo.getInfoFor(classOf[Int]));
        val outFormat = new org.apache.flink.api.java.io.TypeSerializerOutputFormat[tuple.Tuple2[Int, Int]]();

        outFormat.setInputType(typeInformation);
        outFormat.setSerializer(typeInformation.createSerializer());

        __input.write(outFormat, "file:///tmp/emma/temp/comprehension$macro$20", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)
      };

      env.execute("Emma[".$plus("comprehension$macro$20").$plus("]"))
    }
  }

  def clean[F](env: ExecutionEnvironment, f: F): F = {
    if (env.getConfig.isClosureCleanerEnabled) {
      ClosureCleaner.clean(f, true)
    }
    ClosureCleaner.ensureSerializable(f)
    f
  }

}
