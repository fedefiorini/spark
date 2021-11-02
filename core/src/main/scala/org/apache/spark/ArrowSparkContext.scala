package org.apache.spark

import org.apache.arrow.vector.ValueVector
import org.apache.spark.rdd.ArrowRDD

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

class ArrowSparkContext(config: SparkConf) extends SparkContext(config) {
  /**
   * Method to create ArrowRDD[T] starting off from an array of Arrow-backed ValueVector
   *
   * (27.07) tried to include it in the actual SparkContext but somehow there were
   * some troubles with "cannot resolve symbol ..." so I left it here. It works
   */
  def makeArrowRDD[T: ClassTag : TypeTag](@transient vectors: Array[ValueVector],
                                numSlices: Int = defaultParallelism): ArrowRDD[T] = withScope {
    assertNotStopped()
    new ArrowRDD[T](this, vectors, numSlices, Map[Int, Seq[String]]())
  }
}
