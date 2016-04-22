package eu.stratosphere.emma.api.lara

import eu.stratosphere.emma.api.DataBag
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TransformationsTest extends BaseTest {

  val elements = Seq(
    0,0,1,
    0,1,2,
    1,0,3,
    1,1,4
  )

  val tuples = Seq(
    (0,0,1),
    (0,1,2),
    (1,0,3),
    (1,1,4)
  )

  val vectors = Seq(
    Vector(Array(0,0,1), isRowVector = true),
    Vector(Array(0,1,2), isRowVector = true),
    Vector(Array(1,0,3), isRowVector = true),
    Vector(Array(1,1,4), isRowVector = true)
  )

  val tupleBag = DataBag(tuples)

  val vectorBag = DataBag(vectors)

  val matrix: Matrix[Int] = Matrix(4, 3, elements.toArray)

  "Bag to Matrix" - {
    "(i,j,v) tuples" in {
      val m = Transformations.indexedToMatrix(tupleBag)

      m.numRows shouldBe 2
      m.numCols shouldBe 2
      m.toArray shouldBe Array(1,2,3,4)
    }

    "(i,j,v) tuples, static size" in {
      val m = Transformations.indexedToMatrix(2, 2)(tupleBag)

      m.numRows shouldBe 2
      m.numCols shouldBe 2
      m.toArray shouldBe Array(1,2,3,4)
    }

    "Bag[Product]" in {
      val m = Transformations.toMatrix[Int](tupleBag)

      m.numRows shouldBe 4
      m.numCols shouldBe 3

      val vals = tupleBag.vals.flatten(a => Array(a._1, a._2, a._3))
      m.toArray shouldBe vals
    }

    "Bag[Vector]" in {
      val m = Transformations.vecToMatrix[Int](vectorBag)

      m.numRows shouldBe 4
      m.numCols shouldBe 3

      val vals = tupleBag.vals.flatten(a => Array(a._1, a._2, a._3))
      m.toArray shouldBe vals
    }
  }

  "Matrix to Bag" - {
    "Bag[Vector]" in {
      val bag = Transformations.toBag(matrix)
      val fetched = bag.fetch()
      fetched shouldBe vectors
    }

    "Bag[(Int, Vector)]" in {
      val bag = Transformations.toIndexedBag(matrix)
      bag.fetch() shouldBe vectors.zipWithIndex.map(_.swap)
    }
  }
}
