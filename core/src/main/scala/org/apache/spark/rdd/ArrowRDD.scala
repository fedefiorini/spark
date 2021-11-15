package org.apache.spark.rdd

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector._
import org.apache.spark._
import org.apache.spark.internal.Logging

import scala.reflect.{ClassTag, classTag}
import scala.reflect.runtime.universe._

/**
 * An Arrow-backed RDD using ValueVector as data input.
 * The logic used is similar to the one in ParallelCollectionRDD, with the only difference being that most of the
 * logic to extract the data from the vectors (and the resulting value's data type) is delegated to the partitions
 * themselves.
 *
 * Notice that it allows a vector of ValueVector(s) as input, only to determine in the compute(...) method if it's
 * a single vector or more.
 * Only the 1-D and 2-D array cases have been implemented for sake of clarity, in order to show the feasibility
 * of the overall solution rather than an omni-comprehensive work (which would've required too much time and resources).
 *
 * Plus, 3-D vectors or higher would require implementing the corresponding ArrowComposite_N_Partition which would
 * allow TupleN (N >= 3), thus increasing the effort and not providing any more meaningful result in terms of
 * overall feasibility
 *
 * @param sc the SparkContext associated with this RDD
 * @param data the Array of ValueVector(s) used to create this RDD
 * @param numSlices the level of parallelism / no. of partitions of this RDD (default = 1)
 * @param locationPrefs location preferences for the computation. Not defined
 * @tparam T the RDD primitive data type (as defined by the Scala Standard)
 */
private [spark] class ArrowRDD[T: ClassTag](@transient sc: SparkContext,
                                                    @transient val data: Array[ValueVector],
                                                    val numSlices: Int,
                                                    val locationPrefs: Map[Int, Seq[String]])
                                                    (implicit tag: TypeTag[T])
                                                    extends RDD[T](sc, Nil) with Logging {

  /* As said earlier, more vectors haven't been implemented as input for this RDD */
  private val _len = data.length
  require(_len <= 2, "Required maximum two ValueVector as parameter")


  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    _len match {
      case 1 => new InterruptibleIterator(context, split.asInstanceOf[ArrowPartition].iterator.asInstanceOf[Iterator[T]])
      case 2 => new InterruptibleIterator(context, split.asInstanceOf[ArrowPartition].iterator2.asInstanceOf[Iterator[T]])
      case _ => throw new SparkException("Required maximum two ValueVector as parameter")
    }
  }

  override protected def getPartitions: Array[Partition] = {
    _len match {
      case 1 =>
        val slices = ArrowRDD.slice[T](data.head, numSlices) //only one element is in there, so it's also the first
        slices.indices.map(i => new ArrowPartition(id, i, slices(i))).toArray
      case 2 =>
        val vectorizedSlices = ArrowRDD.sliceAndVectorize[T](data, numSlices)
        vectorizedSlices.indices.map(i => new ArrowPartition(id, i, vectorizedSlices(i))).toArray
      case _ => throw new SparkException("Required maximum two ValueVector as parameter")
    }
  }

  override protected def getPreferredLocations(s: Partition): Seq[String] = {
    locationPrefs.getOrElse(s.index, Nil)
  }

  /***
   * Returns a MapPartitionsArrowRDD[U,T] that is then responsible to eventually convert the underlying
   * ValueVector type (if the function f needs it) to hold values of type [U].
   * The conversion logic is delegated to the underlying ArrowPartition, each of which converts its own
   * ValueVector chunk independently (pretty much same logic as original MapPartitionsRDD).
   *
   * It doesn't actually override RDD.map(f: T => U) because of the extra complexity added by using TypeTags!
   */
  def map[U: ClassTag](f: T => U)(implicit tag: TypeTag[U], tag2 : TypeTag[T]) : ArrowRDD[U] = {
    val cleanF = sc.clean(f)
    new MapPartitionsArrowRDD[U, T](this, cleanF)
  }

  def vectorMin() : Int = {
    val parts = this.getPartitions.iterator.map(x => x.asInstanceOf[ArrowPartition].min())
    parts.min
  }
}

private object ArrowRDD {

   implicit def rddToPairRDDFunctions[K : ClassTag, V : ClassTag](rdd: ArrowRDD[(K, V)])
                                          (implicit kt: TypeTag[K], vt: TypeTag[V], ord: Ordering[K] = null): PairRDDFunctions[K, V] = {
    new PairRDDFunctions(rdd)
  }

  implicit def rddToOrderedRDDFunctions[K : Ordering : ClassTag, V: ClassTag](rdd: ArrowRDD[(K, V)])
                                                                             (implicit kt: TypeTag[K], vt: TypeTag[V]) : OrderedRDDFunctions[K, V, (K, V)] = {
    new OrderedRDDFunctions[K, V, (K, V)](rdd)
  }

  def slice[T: ClassTag](vec: ValueVector,
                         numSlices: Int): Seq[ValueVector] = {

    if (numSlices < 1) throw new IllegalArgumentException("Positive number of partitions required")

    def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
      (0 until numSlices).iterator.map { i =>
        val start = ((i * length) / numSlices).toInt
        val end = (((i + 1) * length) / numSlices).toInt

        (start, end)
      }
    }

    /**
     * Slice original ValueVector with zero-copy, using the Slicing operations described
     * in the Java Arrow API documentation page.
     * Then convert the resulting vectors in a sequence
     */
    positions(vec.getValueCount, numSlices).map {
      case (start, end) => {
        val allocator = vec.getAllocator
        val tp = vec.getTransferPair(allocator)

        tp.splitAndTransfer(start, end-start)
        tp.getTo
      }
    }.toSeq
  }

  /* Same as slice() above, but used with two vectors as input. This method uses zero-copy split of the vectors
  * and creates a Tuple2 with the slices, which will then be used to create an ArrowCompositePartition */
  def sliceAndVectorize[T: ClassTag](vectors: Array[ValueVector],
                                   numSlices: Int) : Seq[Array[ValueVector]] = {
    if (numSlices < 1) throw new IllegalArgumentException("Positive number of partitions required")

    require(vectors(0).getValueCount == vectors(1).getValueCount, "Vectors need to be the same size")

    def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
      (0 until numSlices).iterator.map { i =>
        val start = ((i * length) / numSlices).toInt
        val end = (((i + 1) * length) / numSlices).toInt

        (start, end)
      }
    }

    //Both value vectors have the same length, so only one is used
    positions(vectors.head.getValueCount, numSlices).map {
      case (start, end) => {
        // use of head and last to fetch the two different vectors
        val alloc1 = vectors.head.getAllocator
        val tp1 = vectors.head.getTransferPair(alloc1)
        val alloc2 = vectors.last.getAllocator
        val tp2 = vectors.last.getTransferPair(alloc2)

        tp1.splitAndTransfer(start, end-start)
        tp2.splitAndTransfer(start, end-start)

        Array[ValueVector](tp1.getTo, tp2.getTo)
      }
    }.toSeq
  }
}