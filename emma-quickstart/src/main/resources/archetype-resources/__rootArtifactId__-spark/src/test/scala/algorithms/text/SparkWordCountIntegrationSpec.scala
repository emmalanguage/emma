#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}
package algorithms.text

import org.emmalanguage.SparkAware
import org.emmalanguage.api._


class SparkWordCountIntegrationSpec extends BaseWordCountIntegrationSpec with SparkAware {

  override def wordCount(input: String, output: String, csv: CSV): Unit =
    withDefaultSparkSession(implicit spark => emma.onSpark {
      // read the input
      val docs = DataBag.readCSV[String](input, csv)
      // parse and count the words
      val counts = WordCount(docs)
      // write the results into a file
      counts.writeCSV(output, csv)
    })
}
