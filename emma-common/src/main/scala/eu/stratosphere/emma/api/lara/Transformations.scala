package eu.stratosphere.emma.api.lara

import eu.stratosphere.emma.api.DataBag
import spire.math._

import scala.reflect.ClassTag

object Transformations {

  //////////////////////////////////////////
  // Bag -> Matrix
  //////////////////////////////////////////

  /**
   * Transforms a [[DataBag]] `bag` of [[Product]]'s into a [[Matrix]] `M`.
   *
   * The number of rows is defined by the number of elements in the `bag`.
   * The number of columns is defined by the arity of the product. The elements in the product
   * must be of the same type (or one has to specify a suitable upper bound for A).
   *
   * @param bag the databag to be transformed
   * @tparam A element type of output matrix
   * @return A matrix with values from the supplied databag
   */
  def toMatrix[A: Numeric : ClassTag](bag: DataBag[Product]): Matrix[A] = {
    val (rows, cols) = bag.fold((0, 0))(e => (1, e.productArity), (a, b) => (a._1 + b._1, a._2))
    // maybe do that also with a fold
    val rowIndexed = bag.vals.zipWithIndex

    val array = Array.ofDim[A](rows * cols)
    val indexer = Utils.indexed(rows, cols)
    rowIndexed.foreach { e =>
      for (j <- 0 until e._1.productArity) {
        val i = e._2
        array(indexer(i, j)) = e._1.productElement(j).asInstanceOf[A]
      }
    }
    Matrix[A](rows, cols, array)
  }

  /**
   * Transforms a [[DataBag]] `bag` of [[Vector]]'s into a [[Matrix]] `M`.
   *
   * The number of rows is defined by the number of elements in the `bag`.
   * The number of columns is defined by the length of the Vector.
   *
   * @param bag the databag to be transformed
   * @tparam A element type of vector and output matrix
   * @return A matrix with values from the supplied databag
   */
  def vecToMatrix[A: Numeric : ClassTag](bag: DataBag[Vector[A]]): Matrix[A] = {
    val (rows, cols) = bag.fold((0, 0))(e => (1, e.length), (a, b) => (a._1 + b._1, a._2))
    val rowIndexed = bag.vals.zipWithIndex

    val array = Array.ofDim[A](rows * cols)
    val indexer = Utils.indexed(rows, cols)
    rowIndexed.foreach { e =>
      for (j <- 0 until e._1.length) {
        val i = e._2
        array(indexer(i, j)) = e._1.get(j)
      }
    }
    Matrix[A](rows, cols, array)
  }

  /**
   * Transforms a [[DataBag]] `bag` of (row, col, value) triplets into a [[Matrix]] `M`.
   *
   * The number of rows and columns of `M` is explicitly defined by the arguments. The elements in the product
   * must be of the same type (or one has to specify a suitable upper bound for A).
   *
   * @param rows the number of rows in the output matrix
   * @param cols the number of columns in the output matrix
   * @param bag  the databag to be transformed
   * @tparam A element type of output matrix
   * @return A matrix with values from the supplied databag
   */
  def indexedToMatrix[A: Numeric : ClassTag](rows: Int, cols: Int)(bag: DataBag[(Int, Int, A)]): Matrix[A] = {
    val indexer = Utils.indexed(rows, cols)
    val values = Array.ofDim[A](rows * cols)
    for (e <- bag.vals) {
      values(indexer(e._1, e._2)) = e._3
    }
    Matrix(rows, cols, values)
  }

  /**
   * Transforms a [[DataBag]] `bag` of (row, col, value) triplets into a [[Matrix]] `M`.
   *
   * The number of rows and columns of `M` is implicitly defined by the largest row and column indexes of the
   * values in the `bag`. The elements in the product must be of the same type (or one has to specify a suitable
   * upper bound for A).
   *
   * @param bag the databag to be transformed
   * @tparam A element type of output matrix
   * @return A matrix with values from the supplied databag
   */
  def indexedToMatrix[A: Numeric : ClassTag](bag: DataBag[(Int, Int, A)]): Matrix[A] = {
    val res = bag.fold((0, 0))(e => (e._1, e._2), (a, b) => (Math.max(a._1, b._1), Math.max(a._2, b._2)))
    val rows = res._1 + 1
    val cols = res._2 + 1
    val indexer = Utils.indexed(rows, cols)
    val values = Array.ofDim[A](rows * cols)
    for (e <- bag.vals) {
      values(indexer(e._1, e._2)) = e._3
    }
    Matrix(rows, cols, values)
  }

  //////////////////////////////////////////
  // Matrix -> Bag
  //////////////////////////////////////////

  /**
   * Transforms a [[Matrix]] `M` into a [[DataBag]] of [[Vector]]'s.
   *
   * @param matrix the matrix to be transformed
   * @tparam A element type of the matrix
   * @return A databag of vectors with type A
   */
  def toBag[A: Numeric : ClassTag](matrix: Matrix[A]): DataBag[Vector[A]] = {
    DataBag(for (i <- matrix.rRange) yield matrix.row(i))
  }

  /**
   * Transforms a [[Matrix]] `M` into a [[DataBag]] of (row index, [[Vector]]) tuple's.
   *
   * @param matrix the matrix to be transformed
   * @tparam A element type of the matrix
   * @return A databag of vectors with type A
   */
  def toIndexedBag[A: Numeric : ClassTag](matrix: Matrix[A]): DataBag[(Int, Vector[A])] = {
    DataBag(for (i <- matrix.rRange) yield (i, matrix.row(i)))
  }

  //////////////////////////////////////////
  // Bag -> Vector
  //////////////////////////////////////////

  /**
   * Transforms a [[DataBag]] `bag` of [[spire.math.Numeric]]'s into a [[Vector]] `V`.
   *
   * The length of the vector `V` is defined by the number of elements in the bag.
   *
   * @param bag the databag to be transformed
   * @tparam A element type of the output vector
   * @return vector with values from the supplied databag
   */
  def toVector[A: Numeric : ClassTag](bag: DataBag[A]): Vector[A] = {
    Vector(bag.vals.toArray[A])
  }

  /**
   * Transforms a [[DataBag]] `bag` of (index, value) tuple's into a [[Vector]] `V`.
   *
   * The length of the vector `V` is implicitly defined by the largest index of the
   * values in the `bag`.
   *
   * @param bag the databag to be transformed
   * @tparam A element type of the output vector
   * @return vector with values from the supplied databag
   */
  def indexedToVector[A: Numeric : ClassTag](bag: DataBag[(Int, A)]): Vector[A] = {
    val size = bag.fold(0)(e => e._1, (a, b) => Math.max(a, b)) + 1
    val values = Array.ofDim[A](size)
    for (e <- bag.vals) {
      values(e._1) = e._2
    }
    Vector(values)
  }

  /**
   * Transforms a [[DataBag]] `bag` of (index, value) tuple's into a [[Vector]] `V`.
   *
   * The length of the vector `V` is explicitly defined by the `size` argument
   *
   * @param size the size of the resulting vector
   * @param bag the databag to be transformed
   * @tparam A element type of the output vector
   * @return vector with values from the supplied databag
   */
  def indexedToVector[A: Numeric : ClassTag](size: Int)(bag: DataBag[(Int, A)]): Vector[A] = {
    val values = Array.ofDim[A](size)
    for (e <- bag.vals) {
      values(e._1) = e._2
    }
    Vector(values)
  }

  //////////////////////////////////////////
  // Vector -> Bag
  //////////////////////////////////////////

  /**
   * Transforms a [[Vector]] `V` into a [[DataBag]] of [[spire.math.Numeric]]'s.
   *
   * @param vector the vector to be transformed
   * @tparam A element type of the vector
   * @return A databag of values with type A
   */
  def toBag[A: Numeric : ClassTag](vector: Vector[A]): DataBag[A] = {
    DataBag(for (i <- vector.Range) yield vector.get(i))
  }

  /**
   * Transforms a [[Vector]] `V` into a [[DataBag]] of (index, value) tuple's.
   *
   * @param vector the vector to be transformed
   * @tparam A element type of the vector
   * @return A databag of values with type A
   */
  def toIndexedBag[A: Numeric : ClassTag](vector: Vector[A]): DataBag[(Int, A)] = {
    DataBag(for (i <- vector.Range) yield (i, vector.get(i)))
  }
}
