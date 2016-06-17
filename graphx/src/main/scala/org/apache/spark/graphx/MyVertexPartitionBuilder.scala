package org.apache.spark.graphx


import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.util.collection.{PrimitiveVector, BitSet}
import scala.reflect.ClassTag

/**
 * Created by sunny on 4/27/16.
 */
class MyVertexPartitionBuilder[@specialized(Long, Int, Double) VD: ClassTag]
(size: Int = 64) {

  private[this] val vertex = new PrimitiveVector[(VertexId, Iterable[VertexId])](size)
  private[this] var vertexSize = 0

  /** Add a new edge to the partition. */
  def add(v: (VertexId, Iterable[VertexId])) {

    vertex += v

    vertexSize += v._2.size - 1
  }

  def toVertexPartition: MyVertexPartition[VD, Int] = {
    //    val edgeArray = edges.trim().array
    //    new Sorter(Edge.edgeArraySortDataFormat[ED])
    //      .sort(edgeArray, 0, edgeArray.length, Edge.lexicographicOrdering)

    println("vertexSize:" + vertexSize)
    val dstIds = new Array[VertexId](vertexSize)
    val global2local = new GraphXPrimitiveKeyOpenHashMap[VertexId, Int]
    val local2global = new PrimitiveVector[VertexId]
    val edgeAttrs = new Array[Int](vertexSize)
    val index = new GraphXPrimitiveKeyOpenHashMap[VertexId, (Int, Int)]
    var currLocalId = -1

    if (vertex.length > 0) {


      var i = 0
      var p = 0
      var p_pos = 0
      vertex.iterator.foreach(iter => {
        global2local.changeValue(iter._1, {
          currLocalId += 1;
          local2global += iter._1;
          currLocalId
        }, identity)
      })
      while (i < vertex.length) {

        val srcId = vertex(i)._1


        vertex(i)._2.foreach(dst => {

          if (dst >= 0) {
            val dstId = dst
            dstIds(p) = dstId
            global2local.changeValue(dstId, {
              currLocalId += 1;
              local2global += dstId;
              currLocalId
            }, identity)
            edgeAttrs(p) = 1
            p += 1
            p_pos += 1
          }
        })

        index.update(srcId, (p - p_pos, p_pos))
        p_pos = 0
        i += 1
      }

    }

    val active = new BitSet(global2local.size)
    active.setUntil(global2local.size)
    new MyVertexPartition(
      dstIds, new Array[VD](index.size), index, edgeAttrs, global2local, local2global.trim().array,active)
  }


}
