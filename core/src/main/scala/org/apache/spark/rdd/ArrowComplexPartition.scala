package org.apache.spark.rdd

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.spark.{Partition, SparkException}
import org.apache.spark.internal.Logging

import java.io.{Externalizable, ObjectInput, ObjectOutput}
import scala.reflect.ClassTag

/**
 * ArrowPartition that works with StructVector vectors as input.
 * It works very similarly to the ArrowCompositePartition (used for Tuple2 of ValueVector(s)), but with a
 * fundamental difference.
 * In the case of ArrowCompositePartition, the input data consists of two separate vectors, treated independently
 * and with independent data types.
 * In this case, the input StructVector has two (todo: extend to more vectors?) child vectors,
 * so the reasoning to extract the data and all the checks is fundamentally different.
 *
 * Maybe, in a future iteration, the two could be merged (with an awful amount of checks to ensure proper
 * functioning at all times)
 */
class ArrowComplexPartition extends Partition with Externalizable with Logging {

  private var _rddId : Long = 0L
  private var _slice : Int = 0
  private var _data : StructVector = StructVector.empty("structVector", new RootAllocator(Long.MaxValue))

  def this(rddId: Long, slice: Int, data: StructVector) = {
    this
    require(data.isInstanceOf[StructVector], "This Arrow Partition is reserved to " +
      "org.apache.arrow.vector.complex.StructVector data type only")
    require(data.size() == 2, "This Arrow Partition works only for vectors composed of" +
      "two child/nested vectors as part of the struct")
    _rddId = rddId
    _slice = slice
    _data = data
  }

  def iterator[T: ClassTag, U: ClassTag] : Iterator[(T, U)] = {
    var idx = 0
    val children = _data.getChildrenFromFields

    /* Require the size of both vectors to be equal, to simplify returning a value */
    require(children.get(0).getValueCount == children.get(1).getValueCount,
      "This Arrow Partition works only for child vectors of the same size")

    new Iterator[(T, U)] {
      /* Imposing the two value counts to be equal, only checking the first is fine */
      override def hasNext: Boolean = idx > 0 && idx < children.get(0).getValueCount

      override def next(): (T,U) = {
        val value1 = children.get(0).getObject(idx).asInstanceOf[T]
        val value2 = children.get(1).getObject(idx).asInstanceOf[U]
        idx += 1

        (value1, value2)
      }
    }
  }

  override def hashCode(): Int = (51 * (51 + _rddId) + _slice).toInt

  override def equals(other: Any): Boolean = other match {
    case that: ArrowComplexPartition => this._rddId == that._rddId && this._slice == that._slice
    case _ => false
  }

  override def index: Int = _slice

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeLong(_rddId)
    out.writeInt(_slice)

    val children = _data.getChildrenFromFields
    val len = children.get(0).getValueCount   //Since the two vectors have the same size, only one is checked
    val minorType1 = children.get(0).getMinorType
    val minorType2 = children.get(1).getMinorType

    out.writeInt(len)
    out.writeObject((minorType1, minorType2)) //Pass it as tuple (like ArrowCompositePartition)

    for (i <- 0 until len) {
      out.writeObject(children.get(0).getObject(i))
      out.writeObject(children.get(1).getObject(i))
    }
  }

  override def readExternal(in: ObjectInput): Unit = {
    _rddId = in.readLong()
    _slice = in.readInt()

    val len = in.readInt()
    val minorTypes = in.readObject().asInstanceOf[(MinorType, MinorType)]

    minorTypes match {
        //TODO: IMPLEMENT LOGIC (follow ArrowCompositePartition)
      case _ => throw new SparkException("Unsupported Arrow Vector(s)")
    }
  }
}
