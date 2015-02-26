package eu.stratosphere.emma.examples.tpch.query01

case class Result(
                   returnFlag: String,
                   lineStatus: String,
                   sumQty: Int,
                   sumBasePrice: Double,
                   sumDiscPrice: Double,
                   sumCharge: Double,
                   avgQty: Double,
                   avgPrice: Double,
                   avgDisc: Double,
                   countOrder: Long) {}
