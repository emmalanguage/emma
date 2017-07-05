---
layout: simple
title: Project Setup
---

# {{ page.title }}

## Configure an Existing Project

To add Emma to an existing project, add the `emma-language` dependency

```xml
<!-- Core Emma API and compiler infrastructure -->
<dependency>
    <groupId>org.emmalanguage</groupId>
    <artifactId>emma-language</artifactId>
    <version>{{ site.current_version }}</version>
</dependency>
```

and either `emma-flink` or `emma-spark` depending on the desired execution backend.

```xml
<!-- Emma backend for Flink -->
<dependency>
    <groupId>org.emmalanguage</groupId>
    <artifactId>emma-flink</artifactId>
    <version>{{ site.current_version }}</version>
</dependency>
```

```xml
<!-- Emma backend for Spark -->
<dependency>
    <groupId>org.emmalanguage</groupId>
    <artifactId>emma-spark</artifactId>
    <version>{{ site.current_version }}</version>
</dependency>
```

## Setup a New Project

To bootstrap a new project `org.acme:emma-quickstart` from a Maven archetype, use the following command.

```bash
mvn archetype:generate -B                  \
    -DartifactId=emma-quickstart           \
    -DgroupId=org.acme                     \
    -Dversion=0.1-SNAPSHOT                 \
    -Dpackage=org.acme.emma                \
    -DarchetypeArtifactId=emma-quickstart  \
    -DarchetypeGroupId=org.emmalanguage    \
    -DarchetypeVersion={{ site.current_version }}
```

Build the project with one of the following commands.

```bash
mvn package # without tests
mvn verify  # with tests
```

### HDFS Setup

If you are not familiar with Hadoop, check the ["Getting started with Hadoop"](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html) guide.

To run the algorithms on a Flink or Spark cluster, copy the input files to HDFS.

Assuming a variable to `bin/hdfs`

```bash
export HDFS=/path/to/hadoop-2.x/bin/hdfs
export HDFS_ADDR="$HOSTNAME:9000"
```

you can run the following commands.

```bash
$HDFS dfs -mkdir -p /tmp/output
$HDFS dfs -mkdir -p /tmp/input
$HDFS dfs -copyFromLocal emma-quickstart-library/src/test/resources/* /tmp/input/.
```

### Running the Examples on Flink

If you are not familiar with Flink, check the ["Getting started with Flink"](https://ci.apache.org/projects/flink/flink-docs-release-1.2/quickstart/setup_quickstart.html) guide.

Assuming a variable to `bin/flink`

```bash
export FLINK=/path/to/flink-1.2.x/bin/flink
```

and a local filesystem path shared between all nodes in your Flink cluster

```bash
export CODEGEN=/tmp/emma/codegen
```

you can run the algorithms in your quickstart project with one of the following commands.

<ul class="tabs" data-tabs id="example-tabs">
  <li class="tabs-title is-active"><a href="#flink-wordcount">WordCount</a></li>
  <li class="tabs-title"><a href="#flink-transitive-closure" aria-selected="true">Transitive Closure</a></li>
  <li class="tabs-title"><a href="#flink-k-means">K-Means</a></li>
</ul>

<div class="tabs-content snippets-content" data-tabs-content="example-tabs">
<div class="tabs-panel is-active" id="flink-wordcount" style="padding: 0">
{% highlight bash%}
$FLINK run -C "file://$CODEGEN/" \
  emma-quickstart-flink/target/emma-quickstart-flink-0.1-SNAPSHOT.jar \
  word-count \
  hdfs://$HDFS_ADDR/tmp/input/text/jabberwocky.txt \
  hdfs://$HDFS_ADDR/tmp/output/wordcount-output.tsv \
  --codegen "$CODEGEN"
{% endhighlight %}
</div>
<div class="tabs-panel" id="flink-transitive-closure">
{% highlight bash%}
$FLINK run -C "file://$CODEGEN/" \
  emma-quickstart-flink/target/emma-quickstart-flink-0.1-SNAPSHOT.jar \
  transitive-closure \
  hdfs://$HDFS_ADDR/tmp/input/graphs/trans-closure/edges.tsv \
  hdfs://$HDFS_ADDR/tmp/output/trans-closure-output.tsv \
  --codegen "$CODEGEN"
{% endhighlight %}
</div>
<div class="tabs-panel" id="flink-k-means">
{% highlight bash%}
$FLINK run -C "file://$CODEGEN/" \
  emma-quickstart-flink/target/emma-quickstart-flink-0.1-SNAPSHOT.jar \
  k-means 2 4 0.001 10 \
  hdfs://$HDFS_ADDR/tmp/input/ml/clustering/kmeans/points.tsv \
  hdfs://$HDFS_ADDR/tmp/output/kmeans-output.tsv \
  --codegen "$CODEGEN"
{% endhighlight %}
</div>
</div>

### Running the Examples on Spark

If you are not familiar with Spark, check the ["Getting started with Spark"](http://spark.apache.org/docs/latest/quick-start.html) guide.

Assuming a variable to `bin/spark-submit`

```bash
export SPARK=/path/to/spark-2.1.x/bin/spark-submit
```

and a Spark master URL

```bash
export SPARK_ADDR="$HOSTNAME:7077"
```

you can run the algorithms in your quickstart project with one of the following commands.

<ul class="tabs" data-tabs id="example-tabs">
  <li class="tabs-title is-active"><a href="#spark-wordcount">WordCount</a></li>
  <li class="tabs-title"><a href="#spark-transitive-closure" aria-selected="true">Transitive Closure</a></li>
  <li class="tabs-title"><a href="#spark-k-means">K-Means</a></li>
</ul>

<div class="tabs-content snippets-content" data-tabs-content="example-tabs">
<div class="tabs-panel is-active" id="spark-wordcount" style="padding: 0">
{% highlight bash%}
$SPARK --master "spark://$SPARK_ADDR" \
  emma-quickstart-spark/target/emma-quickstart-spark-0.1-SNAPSHOT.jar \
  word-count \
  hdfs://$HDFS_ADDR/tmp/input/text/jabberwocky.txt \
  hdfs://$HDFS_ADDR/tmp/output/wordcount-output.tsv \
  --master "spark://$SPARK_ADDR"
{% endhighlight %}
</div>
<div class="tabs-panel" id="spark-transitive-closure">
{% highlight bash%}
$SPARK --master "spark://$SPARK_ADDR" \
  emma-quickstart-spark/target/emma-quickstart-spark-0.1-SNAPSHOT.jar \
  transitive-closure \
  hdfs://$HDFS_ADDR/tmp/input/graphs/trans-closure/edges.tsv \
  hdfs://$HDFS_ADDR/tmp/output/trans-closure-output.tsv \
  --master "spark://$SPARK_ADDR"
{% endhighlight %}
</div>
<div class="tabs-panel" id="spark-k-means">
{% highlight bash%}
$SPARK --master "spark://$SPARK_ADDR" \
  emma-quickstart-spark/target/emma-quickstart-spark-0.1-SNAPSHOT.jar \
  k-means 2 4 0.001 10 \
  hdfs://$HDFS_ADDR/tmp/input/ml/clustering/kmeans/points.tsv \
  hdfs://$HDFS_ADDR/tmp/output/kmeans-output.tsv \
  --master "spark://$SPARK_ADDR"
{% endhighlight %}
</div>
</div>
