package eu.stratosphere.emma.examples.tpch

object Schema {

  // --------------------------------------------------------------------------------------------
  // -------------------------------- TPC-H Schema ----------------------------------------------
  // --------------------------------------------------------------------------------------------

  case class Nation(
    nationKey: Int,
    name: String,
    regionKey: Int,
    comment: String)

  case class Region(
    regionKey: Int,
    name: String,
    comment: String)

  case class Part(
    partKey: Int,
    name: String,
    mfgr: String,
    brand: String,
    ptype: String,
    size: Int,
    container: String,
    retailPrice: Double,
    comment: String)

  case class Supplier(
    suppKey: Int,
    name: String,
    address: String,
    nationKey: Int,
    phone: String,
    accBal: Double,
    comment: String)

  case class PartSupp(
    partKey: Int,
    suppKey: Int,
    availQty: Int,
    supplyCost: Double,
    comment: String)

  case class Customer(
    custKey: Int,
    name: String,
    address: String,
    nationKey: Int,
    phone: String,
    accBal: Double,
    mktSegment: String,
    comment: String)

  case class Order(
    orderKey: Int,
    custKey: Int,
    orderStatus: String,
    totalPrice: Double,
    orderDate: String,
    orderPriority: String,
    clerk: String,
    shipPriority: Int,
    comment: String)

  case class Lineitem(
    orderKey: Int,
    partKey: Int,
    suppKey: Int,
    lineNumber: Int,
    quantity: Int,
    extendedPrice: Double,
    discount: Double,
    tax: Double,
    returnFlag: String,
    lineStatus: String,
    shipDate: String,
    commitDate: String,
    receiptDate: String,
    shipInstruct: String,
    shipMode: String,
    comment: String)
}
