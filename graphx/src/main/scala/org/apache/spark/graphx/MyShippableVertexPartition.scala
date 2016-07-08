package org.apache.spark.graphx


import org.apache.spark.graphx.impl.VertexIdToIndexMap
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.internal.Logging
import org.apache.spark.util.collection.BitSet

import scala.reflect.ClassTag

/**
 * Created by sunny on 5/5/16.
 */
class MyShippableVertexPartition[VD](    val index: GraphXPrimitiveKeyOpenHashMap[VertexId, Int],
                                         val local2global: Array[VertexId],
                                         val values: Array[VD],
                                         val mask: BitSet) extends Serializable with Logging{

 // val capacity: Int = index.size

  def size: Int = mask.capacity

  def iterator: Iterator[(VertexId, VD)] =
    mask.iterator.map(ind => (local2global(ind), values(ind)))
//

  def aggregateUsingIndex[VD2: ClassTag](
                                          iter: Iterator[Product2[VertexId, VD2]],
                                          reduceFunc: (VD2, VD2) => VD2): MyShippableVertexPartition[VD2] = {
    val newMask = new BitSet(size)
    val newValues = new Array[VD2](size)
    iter.foreach { product =>
      val vid = product._1
      val vdata = product._2
      val pos = index.getOrElse(vid,-1)
      if (pos >= 0) {
        if (newMask.get(pos)) {
          newValues(pos) = reduceFunc(newValues(pos), vdata)
        } else { // otherwise just store the new value
          newMask.set(pos)
          newValues(pos) = vdata
        }
//        println("debug")
      }
    }
    new MyShippableVertexPartition(index, local2global, newValues, newMask)
  }


}
