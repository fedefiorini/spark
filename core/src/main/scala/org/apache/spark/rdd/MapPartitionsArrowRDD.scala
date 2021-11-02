package org.apache.spark.rdd

import org.apache.spark.internal.Logging
import org.apache.spark.{Partition, TaskContext}

import scala.reflect.{ClassTag, classTag}
import scala.reflect.runtime.universe._

/**
 *
 * This class contains the equivalent of MapPartitionsRDD[T] that can work with ArrowRDD's, in order
 * to preserve ValueVector as data type for the RDD's until the very last stage of transformations
 * (such as data retrieval, using the ArrowPartition iterator)
 */
private[spark] class MapPartitionsArrowRDD[U: ClassTag, T: ClassTag]
                        (var par : ArrowRDD[T], f : T => U)
                        (implicit tag : TypeTag[U], tag2 : TypeTag[T])
                        extends ArrowRDD[U](par.context, par.data, par.numSlices, par.locationPrefs) with Logging {

  /* Update 17.09: used for .filter() transformations */
  private var _preservePartitioning = false
  override val partitioner = if (_preservePartitioning) par.partitioner else None

  def setPreservePartitioning() : Unit = {
    _preservePartitioning = true
  }

  override def getPartitions : Array[Partition] = {
    if (tag == tag2) par.partitions.map(x => x.asInstanceOf[ArrowPartition].transformValues[U,T](f))
    else par.partitions.map(x => x.asInstanceOf[ArrowPartition].convert[U,T](f))
//    par.partitions.map(x => x.asInstanceOf[ArrowPartition].convert[U,T](f))
  }

  override def compute(split: Partition, context: TaskContext): Iterator[U] = {
    /* In case of Tuple2 as result, create new ArrowCompositePartition, otherwise keep an ArrowPartition */
    if (classTag[U].equals(classTag[(_,_)])){
      split.asInstanceOf[ArrowPartition].iterator2[T,U].asInstanceOf[Iterator[U]]
    }
    else {
      split.asInstanceOf[ArrowPartition].iterator[U]
    }
  }
}
