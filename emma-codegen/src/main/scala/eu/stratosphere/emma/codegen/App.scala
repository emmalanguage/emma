//package eu.stratosphere.emma.codegen
//
//import eu.stratosphere.emma.api.DataBag
//
///**
// *
// */
//object App {
//
//  private def runTest() = {
//    val algorithm = /*emma.parallelize*/ {
//      val N = 10000
//
//      val A = for (a <- DataBag(1 to N)) yield (a, 2 * a, 3 * a)
//
//      A.fetch()
//    }
//
//    //algorithm.run(rt)
//  }
//
//  def main(args: Array[String]) {
//
//  }
//}
