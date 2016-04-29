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

  val vectorArray = Seq(1, 2, 3, 4, 5)
  val vector = Vector(vectorArray.toArray)

  val vectorBag = DataBag(vectorArray)

  val tuplesBag = DataBag(tuples)

  val vectorsBag = DataBag(vectors)

  val matrix: Matrix[Int] = Matrix(4, 3, elements.toArray)

  "Bag to Matrix" - {
    "(i,j,v) tuples" in {
      val m = Transformations.indexedToMatrix(tuplesBag)

      m.numRows shouldBe 2
      m.numCols shouldBe 2
      m.toArray shouldBe Array(1,2,3,4)

      val sparse = Transformations.indexedToMatrix(tuplesBag plus DataBag(Seq((4, 4, 12))))

      sparse.numRows shouldBe 5
      sparse.numCols shouldBe 5
      sparse.toArray shouldBe Array(1, 2, 0, 0, 0,
                                    3, 4, 0, 0, 0,
                                    0, 0, 0, 0, 0,
                                    0, 0, 0, 0, 0,
                                    0, 0, 0, 0, 12)
    }

    "(i,j,v) tuples, static size" in {
      val m = Transformations.indexedToMatrix(2, 2)(tuplesBag)

      m.numRows shouldBe 2
      m.numCols shouldBe 2
      m.toArray shouldBe Array(1,2,3,4)

      val sparse = Transformations.indexedToMatrix(5, 5)(tuplesBag plus DataBag(Seq((4, 4, 12))))

      sparse.numRows shouldBe 5
      sparse.numCols shouldBe 5
      sparse.toArray shouldBe Array(1, 2, 0, 0, 0,
                                    3, 4, 0, 0, 0,
                                    0, 0, 0, 0, 0,
                                    0, 0, 0, 0, 0,
                                    0, 0, 0, 0, 12)
    }

    "Bag[Product]" in {
      val m = Transformations.toMatrix[Int](tuplesBag)

      m.numRows shouldBe 4
      m.numCols shouldBe 3

      val vals = tuplesBag.vals.flatten(a => Array(a._1, a._2, a._3))
      m.toArray shouldBe vals
    }

    "Bag[Vector]" in {
      val m = Transformations.vecToMatrix[Int](vectorsBag)

      m.numRows shouldBe 4
      m.numCols shouldBe 3

      val vals = tuplesBag.vals.flatten(a => Array(a._1, a._2, a._3))
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

  "Bag to Vector" - {
    "Bag[Numeric]" in {
      val vec = Transformations.toVector(vectorBag)

      vec.length shouldBe vectorArray.length
      vec.toArray shouldBe vectorArray
    }
    "Bag[(Int, Numeric)], static size" in {
      val vec = Transformations.indexedToVector(vectorArray.length)(DataBag(zipWithZero(vectorArray)))

      vec.length shouldBe vectorArray.length
      vec.toArray shouldBe vectorArray

      val sparse = Transformations.indexedToVector(15)(DataBag(Seq(
        (0,1),
        (3,2),
        (10,5)
      )))

      sparse.length shouldBe 15
      sparse.toArray shouldBe Array(1,0,0,2,0,0,0,0,0,0,5,0,0,0,0)
    }

    "Bag[(Int, Numeric)]" in {
      val vec = Transformations.indexedToVector(DataBag(zipWithZero(vectorArray)))

      vec.length shouldBe vectorArray.length
      vec.toArray shouldBe vectorArray

      val sparse = Transformations.indexedToVector(DataBag(Seq(
        (0,1),
        (3,2),
        (10,5)
      )))

      sparse.length shouldBe 11
      sparse.toArray shouldBe Array(1,0,0,2,0,0,0,0,0,0,5)
    }
  }

  "Vector to Bag" - {
    "Bag[Numeric]" in {
      val bag = Transformations.toBag(vector)
      val fetched = bag.fetch()

      fetched shouldBe vectorArray
    }

    "Bag[(Int, Numeric]" in {
      val bag = Transformations.toIndexedBag(vector)
      val fetched = bag.fetch()

      fetched shouldBe zipWithZero(vectorArray)
    }
  }

  def zipWithZero[A](seq: Seq[A]): Seq[(Int, A)] = {
    var i = -1
    seq.map( e => {
      i += 1
      (i, e)
    })
  }
}
