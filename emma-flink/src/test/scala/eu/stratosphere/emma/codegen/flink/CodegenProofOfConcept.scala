package eu.stratosphere.emma.codegen.flink

import eu.stratosphere.emma.codegen.utils.DataflowCompiler
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.java.io.RemoteCollectorImpl
import org.apache.flink.api.java.typeutils.{ResultTypeQueryable, TupleTypeInfo}

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

          class Tokenizer(val separator: String) extends org.apache.flink.api.common.functions.RichFlatMapFunction[String, org.apache.flink.api.java.tuple.Tuple2[String, Int]] {
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
    val dfSymbol = dfCompiler.compile(dfTree)

    val separator = "'"

    dfCompiler.execute[Unit](dfSymbol, "foobar", Array[Any](env, separator))
  }

  def testCollect(env: ExecutionEnvironment) = {
    DataflowWrapper.run(env, " ")
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

      // local collection to store results in
      val collection = java.util.Collections.synchronizedCollection(new java.util.ArrayList[org.apache.flink.api.java.tuple.Tuple2[Int, Int]]())
      // collect results from remote in local collection
      RemoteCollectorImpl.collectLocal(wordCounts, collection)

      env.execute("Multiplier Example")
      System.out.println(collection);

      RemoteCollectorImpl.shutdownAll()
    }
  }

}
