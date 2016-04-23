package eu.stratosphere.emma.api.lara

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ImplicitsTest extends BaseTest {

  val vDouble = Vector(Array(1.0, 2.0, 3.0, 4.0))
  val vInt = Vector(1 to 10)

  val mDouble = Matrix(2, 2, Array(1.0, 2.0, 3.0, 4.0))
  val mInt = Matrix(2, 2, Array(1, 2, 3, 4))

  "Scalar op Vector" - {
    "+" in {
      val res = 2.0 + vDouble
      res shouldBe vDouble.map(e => e + 2)

      val res2 = 2 + vInt
      res2 shouldBe vInt.map(e => e + 2)
    }
    "-" in {
      val res = 2.0 - vDouble
      res shouldBe vDouble.map(e => e - 2)

      val res2 = 2 - vInt
      res2 shouldBe vInt.map(e => e - 2)
    }
    "*" in {
      val res = 2.0 * vDouble
      res shouldBe vDouble.map(e => e * 2)

      val res2 = 2 * vInt
      res2 shouldBe vInt.map(e => e * 2)
    }
    "/" in {
      val res = 2.0 / vDouble
      res shouldBe vDouble.map(e => e / 2)

      val res2 = 2 / vInt
      res2 shouldBe vInt.map(e => e / 2)
    }
  }

  "Scalar op Matrix" - {
    "+" in {
      val res = 2.0 + mDouble
      res shouldBe mDouble.map(e => e + 2)

      val res2 = 2 + mInt
      res2 shouldBe mInt.map(e => e + 2)
    }
    "-" in {
      val res = 2.0 - mDouble
      res shouldBe mDouble.map(e => e - 2)

      val res2 = 2 - mInt
      res2 shouldBe mInt.map(e => e - 2)
    }
    "*" in {
      val res = 2.0 * mDouble
      res shouldBe mDouble.map(e => e * 2)

      val res2 = 2 * mInt
      res2 shouldBe mInt.map(e => e * 2)
    }
    "/" in {
      val res = 2.0 / mDouble
      res shouldBe mDouble.map(e => e / 2)

      val res2 = 2 / mInt
      res2 shouldBe mInt.map(e => e / 2)
    }
  }
}
