package eu.stratosphere.emma.codegen

import __wrapper$1$5f16f3172f874f8a89d70d880eadd371.SynthesizedTypeInformation001
import __wrapper$3$0aaea7ad48d547c7983ccdb91fedd4ef.SynthesizedTypeInformation002
import __wrapper$4$c15a3690fcd84699bb17d7bf5f583c45.SynthesizedTypeInformation003
import __wrapper$5$1ac622deb6fd4b829d52508317819c41.SynthesizedTypeInformation004
import __wrapper$6$c6b3fbe375f04867bcd03050f82ae457.SynthesizedTypeInformation005

object comprehension$macro$43 extends scala.AnyRef {
  import org.apache.flink.api.java.ExecutionEnvironment;
  import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
  import eu.stratosphere.emma.api.DataBag;
  class EmmaMapper00001 extends org.apache.flink.api.common.functions.RichMapFunction[(eu.stratosphere.emma.codegen.flink.TestSchema.FilmFestivalWinner, eu.stratosphere.emma.codegen.flink.TestSchema.IMDBEntry), (Int, String)] with ResultTypeQueryable[(Int, String)] {
    override def map(x: (eu.stratosphere.emma.codegen.flink.TestSchema.FilmFestivalWinner, eu.stratosphere.emma.codegen.flink.TestSchema.IMDBEntry)): (Int, String) = Tuple2.apply[Int, String](x._2.year, x._1.title);
    override def getProducedType = new SynthesizedTypeInformation002()
  };
  class EmmaKeyX00002 extends org.apache.flink.api.java.functions.KeySelector[eu.stratosphere.emma.codegen.flink.TestSchema.FilmFestivalWinner, (String, Int)] with ResultTypeQueryable[(String, Int)] {
    override def getKey(x: eu.stratosphere.emma.codegen.flink.TestSchema.FilmFestivalWinner): (String, Int) = Tuple2.apply[String, Int](x.title, x.year);
    override def getProducedType = new SynthesizedTypeInformation003()
  };
  class EmmaKeyY00003 extends org.apache.flink.api.java.functions.KeySelector[eu.stratosphere.emma.codegen.flink.TestSchema.IMDBEntry, (String, Int)] with ResultTypeQueryable[(String, Int)] {
    override def getKey(y: eu.stratosphere.emma.codegen.flink.TestSchema.IMDBEntry): (String, Int) = Tuple2.apply[String, Int](y.title, y.year);
    override def getProducedType = new SynthesizedTypeInformation003()
  };
  class EmmaJoin00004 extends org.apache.flink.api.common.functions.RichJoinFunction[eu.stratosphere.emma.codegen.flink.TestSchema.FilmFestivalWinner, eu.stratosphere.emma.codegen.flink.TestSchema.IMDBEntry, (eu.stratosphere.emma.codegen.flink.TestSchema.FilmFestivalWinner, eu.stratosphere.emma.codegen.flink.TestSchema.IMDBEntry)] with ResultTypeQueryable[(eu.stratosphere.emma.codegen.flink.TestSchema.FilmFestivalWinner, eu.stratosphere.emma.codegen.flink.TestSchema.IMDBEntry)] {
    override def join(x: eu.stratosphere.emma.codegen.flink.TestSchema.FilmFestivalWinner, y: eu.stratosphere.emma.codegen.flink.TestSchema.IMDBEntry): (eu.stratosphere.emma.codegen.flink.TestSchema.FilmFestivalWinner, eu.stratosphere.emma.codegen.flink.TestSchema.IMDBEntry) = Tuple2.apply[eu.stratosphere.emma.codegen.flink.TestSchema.FilmFestivalWinner, eu.stratosphere.emma.codegen.flink.TestSchema.IMDBEntry](x, y);
    override def getProducedType = new SynthesizedTypeInformation005()
  };
  class EmmaMapper00005 extends org.apache.flink.api.common.functions.RichMapFunction[(eu.stratosphere.emma.codegen.flink.TestSchema.FilmFestivalWinner, eu.stratosphere.emma.codegen.flink.TestSchema.IMDBEntry), (Int, String)] with ResultTypeQueryable[(Int, String)] {
    override def map(x: (eu.stratosphere.emma.codegen.flink.TestSchema.FilmFestivalWinner, eu.stratosphere.emma.codegen.flink.TestSchema.IMDBEntry)): (Int, String) = Tuple2.apply[Int, String](x._2.year, x._1.title);
    override def getProducedType = new SynthesizedTypeInformation002()
  };
  class EmmaKeyX00006 extends org.apache.flink.api.java.functions.KeySelector[eu.stratosphere.emma.codegen.flink.TestSchema.FilmFestivalWinner, (String, Int)] with ResultTypeQueryable[(String, Int)] {
    override def getKey(x: eu.stratosphere.emma.codegen.flink.TestSchema.FilmFestivalWinner): (String, Int) = Tuple2.apply[String, Int](x.title, x.year);
    override def getProducedType = new SynthesizedTypeInformation003()
  };
  class EmmaKeyY00007 extends org.apache.flink.api.java.functions.KeySelector[eu.stratosphere.emma.codegen.flink.TestSchema.IMDBEntry, (String, Int)] with ResultTypeQueryable[(String, Int)] {
    override def getKey(y: eu.stratosphere.emma.codegen.flink.TestSchema.IMDBEntry): (String, Int) = Tuple2.apply[String, Int](y.title, y.year);
    override def getProducedType = new SynthesizedTypeInformation003()
  };
  class EmmaJoin00008 extends org.apache.flink.api.common.functions.RichJoinFunction[eu.stratosphere.emma.codegen.flink.TestSchema.FilmFestivalWinner, eu.stratosphere.emma.codegen.flink.TestSchema.IMDBEntry, (eu.stratosphere.emma.codegen.flink.TestSchema.FilmFestivalWinner, eu.stratosphere.emma.codegen.flink.TestSchema.IMDBEntry)] with ResultTypeQueryable[(eu.stratosphere.emma.codegen.flink.TestSchema.FilmFestivalWinner, eu.stratosphere.emma.codegen.flink.TestSchema.IMDBEntry)] {
    override def join(x: eu.stratosphere.emma.codegen.flink.TestSchema.FilmFestivalWinner, y: eu.stratosphere.emma.codegen.flink.TestSchema.IMDBEntry): (eu.stratosphere.emma.codegen.flink.TestSchema.FilmFestivalWinner, eu.stratosphere.emma.codegen.flink.TestSchema.IMDBEntry) = Tuple2.apply[eu.stratosphere.emma.codegen.flink.TestSchema.FilmFestivalWinner, eu.stratosphere.emma.codegen.flink.TestSchema.IMDBEntry](x, y);
    override def getProducedType = new SynthesizedTypeInformation005()
  };
  def run(env: ExecutionEnvironment) = {
    {
      val __input = {
        val __xs = {
          val typeInformation = new SynthesizedTypeInformation004();
          val inFormat = {
            val inFormat = new org.apache.flink.api.scala.operators.ScalaCsvInputFormat[eu.stratosphere.emma.codegen.flink.TestSchema.FilmFestivalWinner](new org.apache.flink.core.fs.Path("/tmp/emma/cinema/canneswinners.csv"), typeInformation);
            inFormat.setFieldDelimiter('\t');
            inFormat
          };
          env.createInput(inFormat, typeInformation)
        };
        val __ys = {
          val typeInformation = new SynthesizedTypeInformation001();
          val inFormat = new org.apache.flink.api.java.io.TypeSerializerInputFormat[eu.stratosphere.emma.codegen.flink.TestSchema.IMDBEntry](typeInformation.createSerializer());
          inFormat.setFilePath("file:///tmp/emma/temp/imdbTop100");
          env.createInput(inFormat, typeInformation)
        };
        val keyxSelector = new EmmaKeyX00002();
        val keyxType = org.apache.flink.api.java.typeutils.TypeExtractor.getKeySelectorTypes(keyxSelector, __xs.getType);
        val keyx = new org.apache.flink.api.java.operators.Keys.SelectorFunctionKeys[eu.stratosphere.emma.codegen.flink.TestSchema.FilmFestivalWinner, (String, Int)](keyxSelector, __xs.getType, keyxType);
        val keyySelector = new EmmaKeyY00003();
        val keyyType = org.apache.flink.api.java.typeutils.TypeExtractor.getKeySelectorTypes(keyySelector, __ys.getType);
        val keyy = new org.apache.flink.api.java.operators.Keys.SelectorFunctionKeys[eu.stratosphere.emma.codegen.flink.TestSchema.IMDBEntry, (String, Int)](keyySelector, __ys.getType, keyxType);
        val generatedFunction = new org.apache.flink.api.java.operators.JoinOperator.DefaultJoin.WrappingFlatJoinFunction(clean(env, new EmmaJoin00004()));
        new org.apache.flink.api.java.operators.JoinOperator.EquiJoin(__xs, __ys, keyx, keyy, generatedFunction, new SynthesizedTypeInformation005(), org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES, "unknown")
      }.map(new EmmaMapper00001()).union({
        val __xs = {
          val typeInformation = new SynthesizedTypeInformation004();
          val inFormat = {
            val inFormat = new org.apache.flink.api.scala.operators.ScalaCsvInputFormat[eu.stratosphere.emma.codegen.flink.TestSchema.FilmFestivalWinner](new org.apache.flink.core.fs.Path("/tmp/emma/cinema/berlinalewinners.csv"), typeInformation);
            inFormat.setFieldDelimiter('\t');
            inFormat
          };
          env.createInput(inFormat, typeInformation)
        };
        val __ys = {
          val typeInformation = new SynthesizedTypeInformation001();
          val inFormat = new org.apache.flink.api.java.io.TypeSerializerInputFormat[eu.stratosphere.emma.codegen.flink.TestSchema.IMDBEntry](typeInformation.createSerializer());
          inFormat.setFilePath("file:///tmp/emma/temp/imdbTop100");
          env.createInput(inFormat, typeInformation)
        };
        val keyxSelector = new EmmaKeyX00006();
        val keyxType = org.apache.flink.api.java.typeutils.TypeExtractor.getKeySelectorTypes(keyxSelector, __xs.getType);
        val keyx = new org.apache.flink.api.java.operators.Keys.SelectorFunctionKeys[eu.stratosphere.emma.codegen.flink.TestSchema.FilmFestivalWinner, (String, Int)](keyxSelector, __xs.getType, keyxType);
        val keyySelector = new EmmaKeyY00007();
        val keyyType = org.apache.flink.api.java.typeutils.TypeExtractor.getKeySelectorTypes(keyySelector, __ys.getType);
        val keyy = new org.apache.flink.api.java.operators.Keys.SelectorFunctionKeys[eu.stratosphere.emma.codegen.flink.TestSchema.IMDBEntry, (String, Int)](keyySelector, __ys.getType, keyxType);
        val generatedFunction = new org.apache.flink.api.java.operators.JoinOperator.DefaultJoin.WrappingFlatJoinFunction(clean(env, new EmmaJoin00008()));
        new org.apache.flink.api.java.operators.JoinOperator.EquiJoin(__xs, __ys, keyx, keyy, generatedFunction, new SynthesizedTypeInformation005(), org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES, "unknown")
      }.map(new EmmaMapper00005()));
      val typeInformation = new SynthesizedTypeInformation002();
      val outFormat = new org.apache.flink.api.java.io.TypeSerializerOutputFormat[(Int, String)]();
      outFormat.setInputType(typeInformation);
      outFormat.setSerializer(typeInformation.createSerializer());
      __input.write(outFormat, "file:///tmp/emma/temp/comprehension$macro$43", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)
    };
    env.execute("Emma[".$plus("comprehension$macro$43").$plus("]"))
  };
  def clean[F](env: ExecutionEnvironment, f: F): F = {
    if (env.getConfig.isClosureCleanerEnabled)
      org.apache.flink.api.java.ClosureCleaner.clean(f, true)
    else
      ();
    org.apache.flink.api.java.ClosureCleaner.ensureSerializable(f);
    f
  }
}