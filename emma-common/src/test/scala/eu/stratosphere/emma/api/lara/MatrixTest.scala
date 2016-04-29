package eu.stratosphere.emma.api.lara

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MatrixTest extends BaseTest {

  var matrix: Matrix[Int] = _
  val values: Array[Int] = Array(
    1, 2, 3, 4, 5,
    6, 7, 8, 9, 10,
    11, 12, 13, 14, 15
  )
  val valuesT = Array(
    1, 6, 11,
    2, 7, 12,
    3, 8, 13,
    4, 9, 14,
    5, 10, 15
  )

  val rowVecValues = Array(1,2,3,4,5)
  val rowM = rowVecValues ++ rowVecValues ++ rowVecValues

  val colVecValues = Array(1,2,3)
  val colM = Array(1,1,1,1,1,2,2,2,2,2,3,3,3,3,3)

  before {
    matrix = Matrix(3, 5, values)
  }

  "matrix properties" - {
    "transposed" in {
      matrix.transposed shouldBe false
    }
    "# rows" in {
      matrix.numRows shouldBe 3
    }
    "# columns" in {
      matrix.numCols shouldBe 5
    }
    "rRange" in {
      matrix.rRange shouldBe (0 until 3)
      matrix.transpose().rRange shouldBe (0 until 5)
    }
    "cRange" in {
      matrix.cRange shouldBe (0 until 5)
      matrix.transpose().cRange shouldBe (0 until 3)
    }
    "fields" in {
      matrix.get(2, 2) shouldBe 13
      matrix.transpose().get(3, 1) shouldBe 9
    }
    "invalid indexes" in {
      a[IllegalArgumentException] should be thrownBy {
        matrix.get(values.length + 3, values.length + 2)
      }
    }
  }

  "matrix x scalar" - {
    "+" in {
      val res = matrix + 3
      res.toArray shouldBe values.map(_ + 3)
    }
    "-" in {
      val res = matrix - 3
      res.toArray shouldBe values.map(_ - 3)
    }
    "*" in {
      val res = matrix * 3
      res.toArray shouldBe values.map(_ * 3)
    }
    "/" in {
      val res = matrix / 3
      res.toArray shouldBe values.map(_ / 3)
    }
  }

  "matrix x vector (row-wise)" - {
    "require correct boundaries" in {
      val vec = Vector(Array(1,2,3,4,5,6), isRowVector = true)
      a[IllegalArgumentException] should be thrownBy {
        matrix + vec
      }
    }
    "+" in {
      val vec = Vector(rowVecValues, isRowVector = true)
      val res = matrix + vec
      res.toArray shouldBe values.zip(rowM).map(e => e._1 + e._2)
    }
    "-" in {
      val vec = Vector(rowVecValues, isRowVector = true)
      val res = matrix - vec
      res.toArray shouldBe values.zip(rowM).map(e => e._1 - e._2)
    }
    "*" in {
      val vec = Vector(rowVecValues, isRowVector = true)
      val res = matrix * vec
      res.toArray shouldBe values.zip(rowM).map(e => e._1 * e._2)
    }
    "/" in {
      val vec = Vector(rowVecValues, isRowVector = true)
      val res = matrix / vec
      res.toArray shouldBe values.zip(rowM).map(e => e._1 / e._2)
    }
  }

  "matrix x vector (col-wise)" - {
    "require correct boundaries" in {
      val vec = Vector(Array(1,2,3,4,5,6), isRowVector = false)
      a[IllegalArgumentException] should be thrownBy {
        matrix + vec
      }
    }
    "+" in {
      val vec = Vector(colVecValues, isRowVector = false)
      val res = matrix + vec
      res.toArray shouldBe values.zip(colM).map(e => e._1 + e._2)
    }
    "-" in {
      val vec = Vector(colVecValues, isRowVector = false)
      val res = matrix - vec
      res.toArray shouldBe values.zip(colM).map(e => e._1 - e._2)
    }
    "*" in {
      val vec = Vector(colVecValues, isRowVector = false)
      val res = matrix * vec
      res.toArray shouldBe values.zip(colM).map(e => e._1 * e._2)
    }
    "/" in {
      val vec = Vector(colVecValues, isRowVector = false)
      val res = matrix / vec
      res.toArray shouldBe values.zip(colM).map(e => e._1 / e._2)
    }
  }

  "matrix x matrix (pointwise)" - {
    "+" in {
      val other = Matrix(3, 5, values)
      val res = matrix + other
      res.toArray shouldBe values.zip(values).map(e => e._1 + e._2)
    }
    "-" in {
      val other = Matrix(3, 5, values)
      val res = matrix - other
      res.toArray shouldBe values.zip(values).map(e => e._1 - e._2)
    }
    "*" in {
      val other = Matrix(3, 5, values)
      val res = matrix * other
      res.toArray shouldBe values.zip(values).map(e => e._1 * e._2)
    }
    "/" in {
      val other = Matrix(3, 5, values)
      val res = matrix / other
      res.toArray shouldBe values.zip(values).map(e => e._1 / e._2)
    }
  }

  "matrix multiplication" - {
    "m x m - boundaries" in {
      val other = Matrix(3, 5, values)
      a[IllegalArgumentException] should be thrownBy {
        matrix %*% other
      }
    }
    "m x m" in {
      val other = Matrix(5, 3, values)
      val res = matrix %*% other

      val expected = Array(
        135, 150, 165,
        310, 350, 390,
        485, 550, 615
      )

      res.numRows shouldBe 3
      res.numCols shouldBe 3
      res.toArray shouldBe expected
    }
    "m x v - boundaries" in {
      val other = Vector(Array(1,2,3))
      a[IllegalArgumentException] should be thrownBy {
        matrix %*% other
      }
    }
    "m x v" in {
      val other = Vector(rowVecValues)
      val res = matrix %*% other

      val expected = Array(55, 130, 205)

      res.length shouldBe 3
      res.toArray shouldBe expected
    }
  }

  "matrix operations" - {
    "diag - boundaries" in {
      a[IllegalArgumentException] should be thrownBy {
        matrix.diag()
      }
    }
    "diag" in {
      val m = Matrix.fill[Int](3,3)((i, j) => i + j)
      val res = m.diag()

      res.toArray shouldBe (0 until 3).map(i => i + i)
    }
    "transpose" in {
      val res = matrix.transpose()

      res.toArray shouldBe valuesT
    }
    "row(i) - boundaries" in {
      a[IllegalArgumentException] should be thrownBy {
        matrix.row(20)
      }
    }
    "row(i)" in {
      for (r <- matrix.rRange) {
        val rVec = matrix.row(r)
        rVec.toArray shouldBe values.slice(r * 5, r * 5 + 5)
      }
    }
    "column(i) - boundaries" in {
      a[IllegalArgumentException] should be thrownBy {
        matrix.column(20)
      }
    }
    "column(i)" in {
      for (c <- matrix.cRange) {
        val cVec = matrix.column(c)
        cVec.toArray shouldBe valuesT.slice(c * 3, c * 3 + 3)
      }
    }
    "rows to scalar" in {
      val res: Vector[Int] = matrix.rows(v => v.aggregate(_ + _))

      val expected = for (r <- 0 until 3) yield {
        values.slice(r * 5, r * 5 + 5).sum
      }

      res.rowVector shouldBe true
      res.toArray shouldBe expected
    }
    "rows to vector" in {
      val res: Matrix[Int] = matrix.rows(v => v * 3)

      res.numRows shouldBe 3
      res.numCols shouldBe 5
      res.toArray shouldBe values.map(_ * 3)
    }
    "cols to scalar" in {
      val res: Vector[Int] = matrix.cols(v => v.aggregate(_ + _))

      val expected = for (r <- 0 until 5) yield {
        valuesT.slice(r * 3, r * 3 + 3).sum
      }

      res.rowVector shouldBe false
      res.toArray shouldBe expected
    }
    "cols to vector" in {
      val res: Matrix[Int] = matrix.cols(v => v * 3)

      res.numRows shouldBe 3
      res.numCols shouldBe 5
      res.toArray shouldBe values.map(_ * 3)
    }
    "elements" in {
      val res = matrix.elements(0)(e => e, (a, b) => a + b)

      res shouldBe values.foldLeft(0)((s, i) => s + i)
    }
    "map" in {
      val res = matrix.map(e => e + 5)

      res.numRows shouldBe 3
      res.numCols shouldBe 5
      res.toArray shouldBe values.map(e => e + 5)
    }
    "plus" in {
      val other = Matrix(5, 3, valuesT)
      val res = matrix plus other

      val expected = Array(
        1, 6, 11, 4, 5,
        2, 7, 12, 9, 10,
        3, 8, 13, 14, 15,
        4, 9, 14, 0, 0,
        5, 10, 15, 0, 0
      )

      res.numRows shouldBe 5
      res.numCols shouldBe 5
      res.toArray shouldBe expected
    }
  }
}
