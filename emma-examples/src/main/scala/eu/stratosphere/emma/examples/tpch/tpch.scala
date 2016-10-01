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
package eu.stratosphere.emma.examples

package object tpch {

  // --------------------------------------------------------------------------------------------
  // -------------------------------- TPC-H tpch ----------------------------------------------
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
