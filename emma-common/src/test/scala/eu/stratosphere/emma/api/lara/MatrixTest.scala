package eu.stratosphere.emma.api.lara

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MatrixTest extends BaseTest {

  var matrix: Matrix[Int] = _
  val values: Array[Int] = Array.apply(
    1,2,3,4,5,
    6,7,8,9,10,
    11,12,13,14,15
  )

  before {
    matrix = new DenseMatrix[Int](3, 5, values, false)
  }

  "matrix properties" - {
    "transposed" in {
      matrix.transposed should be (false)
    }
    "# rows" in {
      matrix.numRows should be (3)
    }
    "# columns" in {
      matrix.numCols should be (5)
    }
    "fields" in {
      matrix.get(2, 2) should be (13)
    }
    "invalid indexes" in {
      a [IllegalArgumentException] should be thrownBy {
        matrix.get(values.length + 3, values.length + 2)
      }
    }
  }

  "matrix x scalar" - {
    "+"  in {
      val res: DenseMatrix[Int] = (matrix + 3).asInstanceOf[DenseMatrix[Int]]
      res.values should be (values.map(_ + 3))
    }
    "-"  in {
      val res: DenseMatrix[Int] = (matrix - 3).asInstanceOf[DenseMatrix[Int]]
      res.values should be (values.map(_ - 3))
    }
    "*"  in {
      val res: DenseMatrix[Int] = (matrix * 3).asInstanceOf[DenseMatrix[Int]]
      res.values should be (values.map(_ * 3))
    }
    "/"  in {
      val res: DenseMatrix[Int] = (matrix / 3).asInstanceOf[DenseMatrix[Int]]
      res.values should be (values.map(_ / 3))
    }
  }

  "matrix x matrix (pointwise)" - {
    "+" in {
      val other = new DenseMatrix[Int](3, 5, values, false)
      val res: DenseMatrix[Int] = (matrix + other).asInstanceOf[DenseMatrix[Int]]
      res.values should be (values.zip(values).map(e => e._1 + e._2))
    }
    "-" in {
      val other = new DenseMatrix[Int](3, 5, values, false)
      val res: DenseMatrix[Int] = (matrix - other).asInstanceOf[DenseMatrix[Int]]
      res.values should be (values.zip(values).map(e => e._1 - e._2))
    }
    "*" in {
      val other = new DenseMatrix[Int](3, 5, values, false)
      val res: DenseMatrix[Int] = (matrix * other).asInstanceOf[DenseMatrix[Int]]
      res.values should be (values.zip(values).map(e => e._1 * e._2))
    }
    "/" in {
      val other = new DenseMatrix[Int](3, 5, values, false)
      val res: DenseMatrix[Int] = (matrix / other).asInstanceOf[DenseMatrix[Int]]
      res.values should be (values.zip(values).map(e => e._1 / e._2))
    }
  }

//  "matrix multiplication" in {
//    val other = new DenseMatrix[Int](3, 5, values, false)
//    val res: DenseMatrix[Int] = (matrix %*% other).asInstanceOf[DenseMatrix[Int]]
//
//    res.values should be (values.zip(values).map(e => e._1 / e._2))
//  }

  "matrix operations" - {
    // inv, diag, transpose, row(i), col(i), rows, cols, map
  }
}
