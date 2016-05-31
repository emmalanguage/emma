package eu.stratosphere.emma.api.lara

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MatrixFactoriesTest extends BaseTest {

  var vector: Vector[Int] = _
  var vectorFill: Vector[Int] = _
  var matrix: Matrix[Int] = _
  var matrixFill: Matrix[Int] = _
  var matrixFillRows: Matrix[Int] = _
  var matrixFillCols: Matrix[Int] = _

  val vectorValues = Array(1,2,3,4,5)

  val matrixValues = Array(
    1, 6, 11,
    2, 7, 12,
    3, 8, 13,
    4, 9, 14,
    5, 10, 15
  )

  before {
    vector = Vector(vectorValues)
    vectorFill = Vector.fill(5)( i => 1 + i)
    matrix = Matrix(5,3,matrixValues)
    matrixFill = Matrix.fill(5,3)((i,j) => 1 + (i + j*5))
    matrixFillRows = Matrix.fillRows(5,3)(i => Vector.fill(3)(j => 1 + (i + j*5) ))
    matrixFillCols = Matrix.fillColumns(5,3)( j => Vector.fill(5)(i => 1 + (i + j*5)))
  }

  "vector factories" - {
    "length" in {
      vector.length shouldBe 5
      vectorFill.length shouldBe 5
    }
    "fields" in {
      for (i <- 0 until 5) {
        vector.get(i) shouldBe vectorValues(i)
        vectorFill.get(i) shouldBe vectorValues(i)
      }
    }
  }

  "matrix factories" - {
    "# rows" in {
      matrix.numRows shouldBe 5
      matrixFill.numRows shouldBe 5
      matrixFillCols.numRows shouldBe 5
      matrixFillRows.numRows shouldBe 5
    }
    "# columns" in {
      matrix.numCols shouldBe 3
      matrixFill.numCols shouldBe 3
      matrixFillCols.numCols shouldBe 3
      matrixFillRows.numCols shouldBe 3
    }
    "fields" in {
      for (i <- 0 until 5; j <- 0 until 3) {
        matrix.get(i, j) shouldBe matrixValues(i * 3 + j)
        matrixFill.get(i, j) shouldBe matrixValues(i * 3 + j)
        matrixFillCols.get(i, j) shouldBe matrixValues(i * 3 + j)
        matrixFillRows.get(i, j) shouldBe matrixValues(i * 3 + j)
      }
    }
    "correct vector length in gen function" in {
      a[IllegalArgumentException] should be thrownBy {
        Matrix.fillColumns(5,3)(i => Vector.zeros[Int](6))
      }
      a[IllegalArgumentException] should be thrownBy {
        Matrix.fillRows(5,3)(i => Vector.zeros[Int](6))
      }
      a[IllegalArgumentException] should be thrownBy {
        Matrix.fillColumns(5,3)(i => Vector.zeros[Int](2))
      }
      a[IllegalArgumentException] should be thrownBy {
        Matrix.fillRows(5,3)(i => Vector.zeros[Int](2))
      }
    }
  }
}
