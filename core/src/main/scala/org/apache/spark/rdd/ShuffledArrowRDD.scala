package org.apache.spark.rdd

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.Serializer

import scala.reflect.ClassTag

private [spark] class ShuffledArrowRDD[K: ClassTag, V: ClassTag, C: ClassTag](
    @transient var prev: ArrowRDD[_ <: Product2[K, V]],
    part: Partitioner) extends RDD[(K, C)](prev.context, Nil) with Logging {

  @transient private val _data = prev.data

  private var userSpecifiedSerializer: Option[Serializer] = None

  private var keyOrdering: Option[Ordering[K]] = None

  private var aggregator: Option[Aggregator[K, V, C]] = None

  private var mapSideCombine: Boolean = false

  /** Set a serializer for this RDD's shuffle, or null to use the default (spark.serializer) */
  def setSerializer(serializer: Serializer): ShuffledArrowRDD[K, V, C] = {
    this.userSpecifiedSerializer = Option(serializer)
    this
  }

  /** Set key ordering for RDD's shuffle. */
  def setKeyOrdering(keyOrdering: Ordering[K]): ShuffledArrowRDD[K, V, C] = {
    this.keyOrdering = Option(keyOrdering)
    this
  }

  /** Set aggregator for RDD's shuffle. */
  def setAggregator(aggregator: Aggregator[K, V, C]): ShuffledArrowRDD[K, V, C] = {
    this.aggregator = Option(aggregator)
    this
  }

  /** Set mapSideCombine flag for RDD's shuffle. */
  def setMapSideCombine(mapSideCombine: Boolean): ShuffledArrowRDD[K, V, C] = {
    this.mapSideCombine = mapSideCombine
    this
  }

  override def getDependencies: Seq[Dependency[_]] = {
    val serializer = userSpecifiedSerializer.getOrElse {
      val serializerManager = SparkEnv.get.serializerManager
      if (mapSideCombine) {
        serializerManager.getSerializer(implicitly[ClassTag[K]], implicitly[ClassTag[C]])
      } else {
        serializerManager.getSerializer(implicitly[ClassTag[K]], implicitly[ClassTag[V]])
      }
    }
    List(new ShuffleDependency(prev, part, serializer, keyOrdering, aggregator, mapSideCombine))
  }

  override val partitioner = Some(part)


  override def getPartitions: Array[Partition] = {
    prev.partitions
//    Array.tabulate[Partition](part.numPartitions)(i => new ArrowPartition(id, i, _data))
  }

  override protected def getPreferredLocations(partition: Partition): Seq[String] = {
    val tracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
    tracker.getPreferredLocationsForShuffle(dep, partition.index)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[(K, C)] = {
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
    val metrics = context.taskMetrics().createTempShuffleReadMetrics()
    SparkEnv.get.shuffleManager.getReader(
      dep.shuffleHandle, split.index, split.index + 1, context, metrics)
      .read()
      .asInstanceOf[Iterator[(K, C)]]
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    prev = null
  }

  private[spark] override def isBarrier(): Boolean = false
}
