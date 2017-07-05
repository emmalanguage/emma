#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}
package algorithms.text

import org.emmalanguage.api._

@emma.lib
object WordCount {

  def apply(docs: DataBag[String]): DataBag[(String, Long)] = {
    val words = for {
      line <- docs
      word <- DataBag[String](line.toLowerCase.split("${symbol_escape}${symbol_escape}W+"))
      if word != ""
    } yield word

    // group the words by their identity and count the occurrence of each word
    val counts = for {
      group <- words.groupBy(identity)
    } yield (group.key, group.values.size)

    counts
  }
}
