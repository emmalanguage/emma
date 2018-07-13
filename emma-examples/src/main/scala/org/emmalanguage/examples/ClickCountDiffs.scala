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
package examples

import api._

@emma.lib
object ClickCountDiffs {

  def apply(baseName: String, numDays: Int): Unit = {
    val baseInName = baseName

    // (no join with pageAttributes yet)
    var yesterdayCounts: DataBag[(Int, Int)] = DataBag.empty // should be null, but the compilation doesn't handle it
    var day = 1
    while (day <= numDays) {
      // Read all page-visits for this day
      val visits: DataBag[Int] = DataBag.readText(baseInName + day).map(Integer.parseInt) // integer pageIDs
      // Count how many times each page was visited:
      val counts: DataBag[(Int, Int)] = visits
        .groupBy(identity)
        .map(group => (group.key, group.values.size.toInt))
      // Compare to previous day (but skip the first day)
      if (day != 1) {
        // In the paper, this is actually an outer join
        val diffs: DataBag[Int] =
          for {
            c <- counts
            y <- yesterdayCounts
            if c._1 == y._1
          } yield Math.abs(c._2 - y._2)
        val sum = diffs.reduce(0)((x: Int, y: Int) => x + y)
        DataBag(Seq(sum)).writeCSV(baseName + day + ".out", csvConfig)
      }
      yesterdayCounts = counts
      day += 1
    }

  }

  val csvConfig = CSV()

}
