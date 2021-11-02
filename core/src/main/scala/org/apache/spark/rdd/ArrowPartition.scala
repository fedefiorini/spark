package org.apache.spark.rdd

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.{BigIntVector, IntVector, StringVector, ValueVector, VarBinaryVector, ZeroVector}
import org.apache.spark.{Partition, SparkException}
import org.apache.spark.internal.Logging

import java.io.{Externalizable, ObjectInput, ObjectOutput}
import scala.reflect.{ClassTag, classTag}
import scala.reflect.runtime.universe._

/**
 * ArrowPartition offers the main logic for treating the Arrow vectors in Spark.
 * It derives from the ParallelCollectionPartition in terms of usage, but instead of having
 * Seq[T] as data injected it has an Arrow-backed ValueVector of any type.
 *
 * The individual values in the vector are then retrieved when calling the iterator[T], where
 * T is inferred from the type of the vector used as input.
 *
 * See ArrowCompositePartition for multi-vector case
 *
 * Since the value vectors cannot be serialized, it requires using Externalizable to "trick" Spark
 * into thinking it's actually serializing the vectors, whereas it's actually writing them as
 * Array[Byte], therefore maintaining the correct native representation
 */
class ArrowPartition extends Partition with Externalizable with Logging {

  private var _rddId : Long = 0L
  private var _slice : Int = 0
  private var _data : Array[ValueVector] = Array[ValueVector](new ZeroVector, new ZeroVector)

  private val _len = _data.length

  def this(rddId : Long, slice : Int, data : Array[ValueVector]) = {
    this
    require(_len <= 2, "Required maximum 2 ValueVector as parameter")
    _rddId = rddId
    _slice = slice
    _data = data
  }

  def this(rddId: Long, slice : Int, data : ValueVector) = {
    this
    _rddId = rddId
    _slice = slice
    _data = Array[ValueVector](data, new ZeroVector)
  }

  /* Make vector accessible for sub-classes and debug, if required. If not, leave it for future */
  def getVectors: Array[ValueVector] = {
    _data
  }

  /**
   * This iterator actually iterates over the vector and retrieves each value
   * It's called by the compute(...) method in the ArrowRDD and it generalizes
   * the type based on the vector it iterates over to.
   *
   * @tparam T the actual data type for the elements of the given vector
   * @return the Iterator[T] over the vector values
   */
  def iterator[T: TypeTag] : Iterator[T] = {
    require(_data.last.isInstanceOf[ZeroVector],
    "This iterator can only be called for single-vector ArrowPartitions")

    var idx = 0
    new Iterator[T] {
      override def hasNext: Boolean = idx < _data.head.getValueCount

      override def next(): T = {
        val value = _data.head.getObject(idx).asInstanceOf[T]
        idx += 1
        value
      }
    }
  }

  def iterator2[T: TypeTag, U: TypeTag] : Iterator[(T,U)] = {
    require(_data.head.getValueCount == _data.last.getValueCount,
    "Both vectors have to be equal in size")
    require(!_data.last.isInstanceOf[ZeroVector],
    "This iterator can only be called with second vector initialized")

    var idx = 0
    new Iterator[(T, U)] {
      override def hasNext: Boolean = idx < _data.head.getValueCount

      override def next(): (T, U) = {
        val value1 = _data.head.getObject(idx).asInstanceOf[T]
        val value2 = _data.last.getObject(idx).asInstanceOf[U]
        idx += 1

        (value1, value2)
      }
    }
  }

  /* hashCode(), equals() and index are copied from ParallelCollectionPartition, since
  * the logic doesn't change for this example. Only difference, in hashCode(), "43"
  * is used instead of "41" */
  override def hashCode() : Int = (43 * (43 + _rddId) + _slice).toInt

  override def equals(other: Any) : Boolean = other match {
    case that: ArrowPartition => this._rddId == that._rddId && this._slice == that._slice
    case _ => false
  }

  override def index: Int = _slice

  /*METHODS INHERITED FROM EXTERNALIZABLE AND IMPLEMENTED
  *
  * When writing and reading the ArrowPartition it's required to use both the vector's length and
  * the vector's actual Type, in order to create the correct vector and populate it with the right
  * data upon reading from the byte stream used during the de-/serialization process */
  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeLong(_rddId)
    out.writeInt(_slice)

    var nVecs = 2
    if (_data.last.isInstanceOf[ZeroVector]) nVecs = 1
    out.writeInt(nVecs)

    /* Since it's required that both vectors have the same length */
    val len = _data.head.getValueCount
    out.writeInt(len)

    nVecs match {
      case 1 =>
        val vecType = _data.head.getMinorType
        out.writeObject(vecType)
        for (i <- 0 until len) out.writeObject(_data.head.getObject(i))
      case 2 =>
        val minorType1 = _data.head.getMinorType
        val minorType2 = _data.last.getMinorType

        out.writeObject((minorType1, minorType2)) //pass it as tuple (easier reading?)

        //write vector1 first, then vector2 (easier reading?)
        for (i <- 0 until len) {
          out.writeObject(_data.head.getObject(i))
          out.writeObject(_data.last.getObject(i))
        }
      case _ => throw new SparkException("writeExternal doesn't support more than two ValueVector(s)")
    }
  }

  override def readExternal(in: ObjectInput): Unit = {
    _rddId = in.readLong()
    _slice = in.readInt()

    val nVecs = in.readInt()
    val len = in.readInt()
    val allocator = new RootAllocator(Integer.MAX_VALUE)

    /* This double match-case construct allows all possible combinations between ValueVector types, as well as
    * including all possible combinations between one and two vectors being used (it basically merges the
    * previous ArrowPartition and ArrowCompositePartition 's readExternal methods together.
    *
    * In this example some types have been left out. INT and BIGINT (long) prove the feasibility for fixed-width data types,
    * whereas VARBINARY and STRING (newly added) prove it for variable-width vectors. 
    * 
    * Important: in case of two vectors (Tuple2 result type in transformations), the order of the vectors 
    * is really important. So the individual case are treated separately 
    * (i.e. (INT, STRING) != (STRING, INT) */
    nVecs match {
      case 1 =>
        val minorType = in.readObject().asInstanceOf[MinorType]
        minorType match {
          case MinorType.INT =>
            _data = Array[ValueVector](new IntVector("vector", allocator), new ZeroVector)
            _data.head.asInstanceOf[IntVector].setInitialCapacity(len)
            _data.head.asInstanceOf[IntVector].allocateNew()
            for (i <- 0 until len) _data.head.asInstanceOf[IntVector]
              .set(i, in.readObject().asInstanceOf[Int])
            _data.head.setValueCount(len)
          case MinorType.BIGINT =>
            _data = Array[ValueVector](new BigIntVector("vector", allocator), new ZeroVector)
            _data.head.asInstanceOf[BigIntVector].setInitialCapacity(len)
            _data.head.asInstanceOf[BigIntVector].allocateNew()
            for (i <- 0 until len) _data.head.asInstanceOf[BigIntVector]
              .set(i, in.readObject().asInstanceOf[Long])
            _data.head.setValueCount(len)
          case MinorType.VARBINARY =>
            _data = Array[ValueVector](new VarBinaryVector("vector", allocator), new ZeroVector)
            _data.head.asInstanceOf[VarBinaryVector].setInitialCapacity(len)
            _data.head.asInstanceOf[VarBinaryVector].allocateNew()
            for (i <- 0 until len) {
              _data.head.asInstanceOf[VarBinaryVector]
                .setSafe(i, in.readObject().asInstanceOf[Array[Byte]])
            }
            _data.head.setValueCount(len)
          case MinorType.STRING =>
            _data = Array[ValueVector](new StringVector("vector", allocator), new ZeroVector)
            _data.head.asInstanceOf[StringVector].setInitialCapacity(len)
            _data.head.asInstanceOf[StringVector].allocateNew()
            for (i <- 0 until len) _data.head.asInstanceOf[StringVector]
              .setSafe(i, in.readObject().asInstanceOf[String])
            _data.head.setValueCount(len)
          case _ => throw new SparkException("Unsupported Arrow Vector (Single Vector)")
        }
      case 2 =>
        val minorTypes = in.readObject().asInstanceOf[(MinorType, MinorType)]
        minorTypes match {
          case (MinorType.INT, MinorType.INT) =>
            _data = Array[ValueVector](new IntVector("vector1", allocator), new IntVector("vector2", allocator))
            _data.head.asInstanceOf[IntVector].setInitialCapacity(len)
            _data.head.asInstanceOf[IntVector].allocateNew()
            _data.last.asInstanceOf[IntVector].setInitialCapacity(len)
            _data.last.asInstanceOf[IntVector].allocateNew()
            for (i <- 0 until len){
              _data.head.asInstanceOf[IntVector].set(i, in.readObject().asInstanceOf[Int])
              _data.last.asInstanceOf[IntVector].set(i, in.readObject().asInstanceOf[Int])
            }
            _data.head.setValueCount(len)
            _data.last.setValueCount(len)
          case (MinorType.BIGINT, MinorType.BIGINT) =>
            _data = Array[ValueVector](new BigIntVector("vector1", allocator), new BigIntVector("vector2", allocator))
            _data.head.asInstanceOf[BigIntVector].setInitialCapacity(len)
            _data.head.asInstanceOf[BigIntVector].allocateNew()
            _data.last.asInstanceOf[BigIntVector].setInitialCapacity(len)
            _data.last.asInstanceOf[BigIntVector].allocateNew()
            for (i <- 0 until len){
              _data.head.asInstanceOf[BigIntVector].set(i, in.readObject().asInstanceOf[Long])
              _data.last.asInstanceOf[BigIntVector].set(i, in.readObject().asInstanceOf[Long])
            }
            _data.head.setValueCount(len)
            _data.last.setValueCount(len)
          case (MinorType.VARBINARY, MinorType.VARBINARY) =>
            _data = Array[ValueVector](new VarBinaryVector("vector1", allocator), new VarBinaryVector("vector2", allocator))
            _data.head.asInstanceOf[VarBinaryVector].setInitialCapacity(len)
            _data.head.asInstanceOf[VarBinaryVector].allocateNew()
            _data.last.asInstanceOf[VarBinaryVector].setInitialCapacity(len)
            _data.last.asInstanceOf[VarBinaryVector].allocateNew()
            for (i <- 0 until len){
              _data.head.asInstanceOf[VarBinaryVector].setSafe(i, in.readObject().asInstanceOf[Array[Byte]])
              _data.last.asInstanceOf[VarBinaryVector].setSafe(i, in.readObject().asInstanceOf[Array[Byte]])
            }
            _data.head.setValueCount(len)
            _data.last.setValueCount(len)
          case (MinorType.STRING, MinorType.STRING) =>
            _data = Array[ValueVector](new StringVector("vector1", allocator), new StringVector("vector2", allocator))
            _data.head.asInstanceOf[StringVector].setInitialCapacity(len)
            _data.head.asInstanceOf[StringVector].allocateNew()
            _data.last.asInstanceOf[StringVector].setInitialCapacity(len)
            _data.last.asInstanceOf[StringVector].allocateNew()
            for (i <- 0 until len){
              _data.head.asInstanceOf[StringVector].setSafe(i, in.readObject().asInstanceOf[String])
              _data.last.asInstanceOf[StringVector].setSafe(i, in.readObject().asInstanceOf[String])
            }
            _data.head.setValueCount(len)
            _data.last.setValueCount(len)
          case (MinorType.INT, MinorType.BIGINT) =>
            _data = Array[ValueVector](new IntVector("vector1", allocator), new BigIntVector("vector2", allocator))
            _data.head.asInstanceOf[IntVector].setInitialCapacity(len)
            _data.head.asInstanceOf[IntVector].allocateNew()
            _data.last.asInstanceOf[BigIntVector].setInitialCapacity(len)
            _data.last.asInstanceOf[BigIntVector].allocateNew()
            for (i <- 0 until len){
              _data.head.asInstanceOf[IntVector].set(i, in.readObject().asInstanceOf[Int])
              _data.last.asInstanceOf[BigIntVector].set(i, in.readObject().asInstanceOf[Long])
            }
            _data.head.setValueCount(len)
            _data.last.setValueCount(len)
          case (MinorType.BIGINT, MinorType.INT) =>
            _data = Array[ValueVector](new BigIntVector("vector1", allocator), new IntVector("vector2", allocator))
            _data.head.asInstanceOf[BigIntVector].setInitialCapacity(len)
            _data.head.asInstanceOf[BigIntVector].allocateNew()
            _data.last.asInstanceOf[IntVector].setInitialCapacity(len)
            _data.last.asInstanceOf[IntVector].allocateNew()
            for (i <- 0 until len){
              _data.head.asInstanceOf[BigIntVector].set(i, in.readObject().asInstanceOf[Long])
              _data.last.asInstanceOf[IntVector].set(i, in.readObject().asInstanceOf[Int])
            }
            _data.head.setValueCount(len)
            _data.last.setValueCount(len)
          case (MinorType.INT, MinorType.VARBINARY) =>
            _data = Array[ValueVector](new IntVector("vector1", allocator), new VarBinaryVector("vector2", allocator))
            _data.head.asInstanceOf[IntVector].setInitialCapacity(len)
            _data.head.asInstanceOf[IntVector].allocateNew()
            _data.last.asInstanceOf[VarBinaryVector].setInitialCapacity(len)
            _data.last.asInstanceOf[VarBinaryVector].allocateNew()
            for (i <- 0 until len){
              _data.head.asInstanceOf[IntVector].set(i, in.readObject().asInstanceOf[Int])
              _data.last.asInstanceOf[VarBinaryVector].setSafe(i, in.readObject().asInstanceOf[Array[Byte]])
            }
            _data.head.setValueCount(len)
            _data.last.setValueCount(len)
          case (MinorType.VARBINARY, MinorType.INT) =>
            _data = Array[ValueVector](new VarBinaryVector("vector1", allocator), new IntVector("vector2", allocator))
            _data.head.asInstanceOf[VarBinaryVector].setInitialCapacity(len)
            _data.head.asInstanceOf[VarBinaryVector].allocateNew()
            _data.last.asInstanceOf[IntVector].setInitialCapacity(len)
            _data.last.asInstanceOf[IntVector].allocateNew()
            for (i <- 0 until len){
              _data.head.asInstanceOf[VarBinaryVector].setSafe(i, in.readObject().asInstanceOf[Array[Byte]])
              _data.last.asInstanceOf[IntVector].set(i, in.readObject().asInstanceOf[Int])
            }
            _data.head.setValueCount(len)
            _data.last.setValueCount(len)
          case (MinorType.INT, MinorType.STRING) =>
            _data = Array[ValueVector](new IntVector("vector1", allocator), new StringVector("vector2", allocator))
            _data.head.asInstanceOf[IntVector].setInitialCapacity(len)
            _data.head.asInstanceOf[IntVector].allocateNew()
            _data.last.asInstanceOf[StringVector].setInitialCapacity(len)
            _data.last.asInstanceOf[StringVector].allocateNew()
            for (i <- 0 until len){
              _data.head.asInstanceOf[IntVector].set(i, in.readObject().asInstanceOf[Int])
              _data.last.asInstanceOf[StringVector].setSafe(i, in.readObject().asInstanceOf[String])
            }
            _data.head.setValueCount(len)
            _data.last.setValueCount(len)
          case (MinorType.STRING, MinorType.INT) =>
            _data = Array[ValueVector](new StringVector("vector1", allocator), new IntVector("vector2", allocator))
            _data.head.asInstanceOf[StringVector].setInitialCapacity(len)
            _data.head.asInstanceOf[StringVector].allocateNew()
            _data.last.asInstanceOf[IntVector].setInitialCapacity(len)
            _data.last.asInstanceOf[IntVector].allocateNew()
            for (i <- 0 until len){
              _data.head.asInstanceOf[StringVector].setSafe(i, in.readObject().asInstanceOf[String])
              _data.last.asInstanceOf[IntVector].set(i, in.readObject().asInstanceOf[Int])
            }
            _data.head.setValueCount(len)
            _data.last.setValueCount(len)
          case (MinorType.BIGINT, MinorType.VARBINARY) =>
            _data = Array[ValueVector](new BigIntVector("vector1", allocator), new VarBinaryVector("vector2", allocator))
            _data.head.asInstanceOf[BigIntVector].setInitialCapacity(len)
            _data.head.asInstanceOf[BigIntVector].allocateNew()
            _data.last.asInstanceOf[VarBinaryVector].setInitialCapacity(len)
            _data.last.asInstanceOf[VarBinaryVector].allocateNew()
            for (i <- 0 until len){
              _data.head.asInstanceOf[BigIntVector].set(i, in.readObject().asInstanceOf[Long])
              _data.last.asInstanceOf[VarBinaryVector].setSafe(i, in.readObject().asInstanceOf[Array[Byte]])
            }
            _data.head.setValueCount(len)
            _data.last.setValueCount(len)
          case (MinorType.VARBINARY, MinorType.BIGINT) =>
            _data = Array[ValueVector](new VarBinaryVector("vector1", allocator), new BigIntVector("vector2", allocator))
            _data.head.asInstanceOf[VarBinaryVector].setInitialCapacity(len)
            _data.head.asInstanceOf[VarBinaryVector].allocateNew()
            _data.last.asInstanceOf[BigIntVector].setInitialCapacity(len)
            _data.last.asInstanceOf[BigIntVector].allocateNew()
            for (i <- 0 until len){
              _data.head.asInstanceOf[VarBinaryVector].setSafe(i, in.readObject().asInstanceOf[Array[Byte]])
              _data.last.asInstanceOf[BigIntVector].set(i, in.readObject().asInstanceOf[Long])
            }
            _data.head.setValueCount(len)
            _data.last.setValueCount(len)
          case (MinorType.BIGINT, MinorType.STRING) =>
            _data = Array[ValueVector](new BigIntVector("vector1", allocator), new StringVector("vector2", allocator))
            _data.head.asInstanceOf[BigIntVector].setInitialCapacity(len)
            _data.head.asInstanceOf[BigIntVector].allocateNew()
            _data.last.asInstanceOf[StringVector].setInitialCapacity(len)
            _data.last.asInstanceOf[StringVector].allocateNew()
            for (i <- 0 until len){
              _data.head.asInstanceOf[BigIntVector].set(i, in.readObject().asInstanceOf[Long])
              _data.last.asInstanceOf[StringVector].setSafe(i, in.readObject().asInstanceOf[String])
            }
            _data.head.setValueCount(len)
            _data.last.setValueCount(len)
          case (MinorType.STRING, MinorType.BIGINT) =>
            _data = Array[ValueVector](new StringVector("vector1", allocator), new BigIntVector("vector2", allocator))
            _data.head.asInstanceOf[StringVector].setInitialCapacity(len)
            _data.head.asInstanceOf[StringVector].allocateNew()
            _data.last.asInstanceOf[BigIntVector].setInitialCapacity(len)
            _data.last.asInstanceOf[BigIntVector].allocateNew()
            for (i <- 0 until len){
              _data.head.asInstanceOf[StringVector].setSafe(i, in.readObject().asInstanceOf[String])
              _data.last.asInstanceOf[BigIntVector].set(i, in.readObject().asInstanceOf[Long])
            }
            _data.head.setValueCount(len)
            _data.last.setValueCount(len)
          case (_ , _) => throw new SparkException("Unsupported Arrow Vectors (tuple)")
        }
      case _ => throw new SparkException("Wrong number of vectors for readExternal method")
    }
  }

  def transformValues[U: ClassTag, T: ClassTag](f : T => U)(implicit tag: TypeTag[T], tag2 : TypeTag[U]) : ArrowPartition = {
    require(tag == tag2, "Otherwise it doesn't work")

    val ctag = classTag[T]
    for (i <- 0 until _data.head.getValueCount){
      if (ctag.equals(classTag[String]) || ctag.equals(classTag[java.lang.String])){
        _data.head.asInstanceOf[StringVector]
          .set(i, f.apply(_data.head.asInstanceOf[StringVector].get(i).asInstanceOf[T]).asInstanceOf[String])
      }
      else if (ctag.equals(classTag[Int])){
        _data.head.asInstanceOf[IntVector]
          .set(i, f.apply(_data.head.asInstanceOf[IntVector].get(i).asInstanceOf[T]).asInstanceOf[Int])
      }
      else if (ctag.equals(classTag[Long])){
        _data.head.asInstanceOf[BigIntVector]
          .set(i, f.apply(_data.head.asInstanceOf[BigIntVector].get(i).asInstanceOf[T]).asInstanceOf[Long])
      }
      else throw new SparkException("No Vector value conversion for classTag %s".format(ctag))
    }

    new ArrowPartition(_rddId, _slice, _data)
  }

  def convert[U: ClassTag, T: ClassTag](f: T => U)
                                       (implicit tag: TypeTag[U], tag2: TypeTag[T]) : ArrowPartition = {
    var vecRes : ValueVector = new ZeroVector
    /* As said above, the two vectors are required to be the same length */
    val count = _data.head.getValueCount
    val allocator = new RootAllocator(Integer.MAX_VALUE)
    val ctagU = classTag[U]

    if (_data.last.isInstanceOf[ZeroVector]){
      if (ctagU.equals(classTag[(_,_)])){
        val tpe = tag.tpe.typeArgs.last
        tpe match {
          case t if t =:= typeOf[Int] =>
            vecRes = new IntVector("vector", allocator)
            vecRes.asInstanceOf[IntVector].setInitialCapacity(count)
            vecRes.asInstanceOf[IntVector].allocateNew()
            for (i <- 0 until count){
              vecRes.asInstanceOf[IntVector]
                .set(i, f.apply(_data.head.getObject(i).asInstanceOf[T]).asInstanceOf[(T,U)]._2.asInstanceOf[Int])

            }
            vecRes.setValueCount(count)
          case t if t =:= typeOf[Long] =>
            vecRes = new  BigIntVector("vector", allocator)
            vecRes.asInstanceOf[BigIntVector].setInitialCapacity(count)
            vecRes.asInstanceOf[BigIntVector].allocateNew()
            for (i <- 0 until count){
              vecRes.asInstanceOf[BigIntVector]
                .set(i, f.apply(_data.head.getObject(i).asInstanceOf[T]).asInstanceOf[(T,U)]._2.asInstanceOf[Long])
            }
            vecRes.setValueCount(count)
          case t if t =:= typeOf[String] | t =:= typeOf[java.lang.String] =>
            vecRes = new StringVector("vector", allocator)
            vecRes.asInstanceOf[StringVector].setInitialCapacity(count)
            vecRes.asInstanceOf[StringVector].allocateNew()
            for (i <- 0 until count){
              vecRes.asInstanceOf[StringVector]
                .setSafe(i, f.apply(_data.head.getObject(i).asInstanceOf[T]).asInstanceOf[(T,U)]._2.asInstanceOf[String])
            }
            vecRes.setValueCount(count)
          case t if t =:= typeOf[Array[Byte]] =>
            vecRes = new VarBinaryVector("vector", allocator)
            vecRes.asInstanceOf[VarBinaryVector].setInitialCapacity(count)
            vecRes.asInstanceOf[VarBinaryVector].allocateNew()
            for (i <- 0 until count){
              vecRes.asInstanceOf[VarBinaryVector]
                .setSafe(i, f.apply(_data.head.getObject(i).asInstanceOf[T]).asInstanceOf[(T,U)]._2.asInstanceOf[Array[Byte]])
            }
            vecRes.setValueCount(count)
          case _ => throw new SparkException("Conversion between types %s and %s (in Tuple2[%s,%s] is not possible in Arrow-Spark"
            .format(tag2, tag, tag.tpe.typeArgs.head, tpe))
        }

        new ArrowPartition(_rddId, _slice, Array[ValueVector](_data.head, vecRes))
      }
      else {
        typeOf[U] match {
          case t if t =:= typeOf[Int] =>
            vecRes = new IntVector("vector", allocator)
            vecRes.asInstanceOf[IntVector].setInitialCapacity(count)
            vecRes.asInstanceOf[IntVector].allocateNew()
            for (i <- 0 until count){
              vecRes.asInstanceOf[IntVector]
                .set(i, f.apply(_data.head.getObject(i).asInstanceOf[T]).asInstanceOf[Int])
            }
            vecRes.setValueCount(count)
//            _data.head.close()
          case t if t =:= typeOf[Long] =>
            vecRes = new  BigIntVector("vector", allocator)
            vecRes.asInstanceOf[BigIntVector].setInitialCapacity(count)
            vecRes.asInstanceOf[BigIntVector].allocateNew()
            for (i <- 0 until count){
              vecRes.asInstanceOf[BigIntVector]
                .set(i, f.apply(_data.head.getObject(i).asInstanceOf[T]).asInstanceOf[Long])
            }
            vecRes.setValueCount(count)
//            _data.head.close()
          case t if t =:= typeOf[String] | t =:= typeOf[java.lang.String] =>
            vecRes = new StringVector("vector", allocator)
            vecRes.asInstanceOf[StringVector].setInitialCapacity(count)
            vecRes.asInstanceOf[StringVector].allocateNew()
            for (i <- 0 until count){
              vecRes.asInstanceOf[StringVector]
                .setSafe(i, f.apply(_data.head.getObject(i).asInstanceOf[T]).asInstanceOf[String])
            }
            vecRes.setValueCount(count)
//            _data.head.close()
          case t if t =:= typeOf[Array[Byte]] =>
            vecRes = new VarBinaryVector("vector", allocator)
            vecRes.asInstanceOf[VarBinaryVector].setInitialCapacity(count)
            vecRes.asInstanceOf[VarBinaryVector].allocateNew()
            for (i <- 0 until count){
              vecRes.asInstanceOf[VarBinaryVector]
                .setSafe(i, f.apply(_data.head.getObject(i).asInstanceOf[T]).asInstanceOf[Array[Byte]])
            }
            vecRes.setValueCount(count)
//            _data.head.close()
          case _ => throw new SparkException("Conversion between types %s and %s is not possible in Arrow-Spark"
          .format(tag2, tag))
        }

        new ArrowPartition(_rddId, _slice, Array[ValueVector](vecRes, _data.last))
      }
    }
    else {
      throw new SparkException("NOT YET IMPLEMENTED...")
    }
  }

  def min() : Int = {
    if (_data.head.isInstanceOf[IntVector] || _data.last.isInstanceOf[IntVector]){
      val min1 = _data.head.asInstanceOf[IntVector].getMin
      if (_data.last.isInstanceOf[ZeroVector]) min1
      else {
        val min2 = _data.last.asInstanceOf[IntVector].getMin
        if (min1 < min2) min1
        else min2
      }
    }
    else throw new SparkException("Function min() not yet implemented for this data type")
  }
}
