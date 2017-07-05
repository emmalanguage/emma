#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}
package algorithms.text

import org.emmalanguage.FlinkAware
import org.emmalanguage.api._

class FlinkWordCountIntegrationSpec extends BaseWordCountIntegrationSpec with FlinkAware {

  override def wordCount(input: String, output: String, csv: CSV): Unit =
    withDefaultFlinkEnv(implicit flink => emma.onFlink {
      // read the input
      val docs = DataBag.readText(input)
      // parse and count the words
      val counts = WordCount(docs)
      // write the results into a file
      counts.writeCSV(output, csv)
    })
}
