package org.apache.spark.graphx

import org.apache.spark.graphx.impl.{VertexIdToIndexMap, EdgeActiveness}
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.internal.Logging
import org.apache.spark.util.collection.{PrimitiveVector, BitSet}
import scala.reflect.ClassTag

/**
 * Created by sunny on 4/25/16.
 */
class MyVertexPartition[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) VD: ClassTag, ED: ClassTag]
(

  dstIds: Array[VertexId],
  attrs: Array[VD],
  vertexIds: GraphXPrimitiveKeyOpenHashMap[VertexId, (Int, Int)],
  edgeAttrs: Array[ED],
  global2local: GraphXPrimitiveKeyOpenHashMap[VertexId, Int],
  local2global: Array[VertexId],
  activeSet: BitSet)
  extends Serializable with Logging {


  // def numActives: Option[Int] = activeSet.map(_.size)
  def indexSize: Int = vertexIds.size

  val size: Int = attrs.size

  def iterator: Iterator[(VertexId, VD)] = new Iterator[(VertexId, VD)] {
    private[this] var pos = 0

    private[this] val id = local2global

    override def hasNext: Boolean = (pos < MyVertexPartition.this.size)

    override def next(): (VertexId, VD) = {
      val vid = id(pos)
      val attr = attrs(pos)
      pos += 1
      (vid, attr)
    }
  }

  def isActive(vid: VertexId): Boolean = {
    activeSet.get(global2local(vid))
  }

  def foreachEdgePartition(f :(VertexId,ED) => Unit): Unit ={
    val iter = vertexIds.iterator
    while (iter.hasNext) {
      val tuple = iter.next()
      val srcId = tuple._1
      val (dstIndex, dstPos) = tuple._2
      var i = 0
//      println("start : " +srcId + "  "+ dstIndex +" " +dstPos)
      while (i < dstPos) {
//          val dstId = dstIds(dstIndex + i)
//            val srcAttr = attrs(global2local(srcId))
           f(srcId, edgeAttrs(dstIndex + i))
          i += 1
        }


    }
  }


  def map[VD2: ClassTag](f: (VertexId, VD) => VD2): MyVertexPartition[VD2, ED] = {
    // Construct a view of the map transformation
    val newValues = new Array[VD2](size)
    var i = 0
    while (i < size) {
      newValues(i) = f(local2global(i), attrs(i))
      i += 1
    }
    new MyVertexPartition[VD2, ED](dstIds, newValues, vertexIds, edgeAttrs, global2local, local2global, activeSet)
  }


  def mapTriplets[ED2: ClassTag]( f: MyEdgeTriplet[VD, ED] => ED2): MyVertexPartition[VD,ED2] ={
    val newData = new Array[ED2](edgeAttrs.length)
    val iter = vertexIds.iterator
    while (iter.hasNext) {
      val tuple = iter.next()
      val srcId = tuple._1
      val (dstIndex, dstPos) = tuple._2
      var i = 0
      while (i < dstPos) {
        val triplet = new MyEdgeTriplet[VD, ED]
        triplet.dstId = dstIds(dstIndex + i)
        triplet.srcAttr = attrs(global2local(srcId))
        triplet.attr = edgeAttrs(dstIndex + i)
        newData(dstIndex + i) = f(triplet)
        i += 1
      }
    }
    new MyVertexPartition(
      dstIds, attrs, vertexIds, newData, global2local, local2global, activeSet)

  }




  def leftJoin[VD2: ClassTag, VD3: ClassTag]
  (other: Iterator[(VertexId, VD2)])
  (f: (VertexId, VD, Option[VD2]) => VD3): MyVertexPartition[VD3, ED] = {
    leftJoin(createUsingIndex(other))(f)
  }

  def leftJoin[VD2: ClassTag, VD3: ClassTag]
  (other: MyShippableVertexPartition[VD2])
  (f: (VertexId, VD, Option[VD2]) => VD3): MyVertexPartition[VD3, ED] = {
    if (global2local != other.index) {
      logWarning("Joining two VertexPartitions with different indexes is slow.")
      leftJoin(createUsingIndex(other.iterator))(f)
    } else {
      val newValues = new Array[VD3](size)

      //var i = other.mask.nextSetBit(0)

      var i = 0

      while (i < size) {
        val otherV: Option[VD2] = if (other.mask.get(i)) Some(other.values(i)) else None
        newValues(i) = f(local2global(i), attrs(i), otherV)
        if (attrs(i) == newValues(i)) {
          activeSet.unset(i)
        } else {
          activeSet.set(i)
        }
        i += 1
      }

      new MyVertexPartition[VD3, ED](dstIds, newValues, vertexIds, edgeAttrs, global2local, local2global, activeSet)
    }
  }

  def createUsingIndex[VD2: ClassTag](iter: Iterator[Product2[VertexId, VD2]])
  : MyShippableVertexPartition[VD2] = {
    val newMask = new BitSet(size)
    val newValues = new Array[VD2](size)
    iter.foreach { pair =>
      val pos = global2local.getOrElse(pair._1, -1)
      if (pos >= 0) {
        newMask.set(pos)
        newValues(pos) = pair._2
      }
    }
    new MyShippableVertexPartition[VD2](global2local, local2global, newValues, newMask)
  }


  def aggregateUsingIndex[VD2: ClassTag](
                                          iter: Iterator[Product2[VertexId, VD2]],
                                          reduceFunc: (VD2, VD2) => VD2): MyShippableVertexPartition[VD2] = {
    val ship = new MyShippableVertexPartition(global2local, local2global, new Array[VD2](size), new BitSet(size))
    ship.aggregateUsingIndex(iter, reduceFunc)
  }

  def aggregateMessagesEdgeScan[A: ClassTag](
                                              sendMsg: MyVertexContext[VD, ED, A] => Unit,
                                              mergeMsg: (A, A) => A,
                                              tripletFields: TripletFields,
                                              activeness: EdgeActiveness): Iterator[(VertexId, A)] = {

    //    println("global2local size: " + global2local.size +"  attrs size: " + attrs.length +"  iter size:  "+ vertexIds.size)
    val aggregates = new Array[A](global2local.size)
    val bitset = new BitSet(global2local.size)

    val ctx = new AggregatingVertexContext[VD, ED, A](mergeMsg, aggregates, bitset)

    var pos = activeSet.nextSetBit(0)
    while (pos >= 0) {

      val srcId = local2global(pos)
      val (dstIndex, dstPos) = vertexIds.getOrElse(srcId,(-1,-1))

        var i = 0
        while (i < dstPos) {
          val dstId = dstIds(dstIndex + i)
          val edgeIsActive =
            if (activeness == EdgeActiveness.Neither) true
            else if (activeness == EdgeActiveness.SrcOnly) isActive(srcId)
            else if (activeness == EdgeActiveness.DstOnly) isActive(dstId)
            else if (activeness == EdgeActiveness.Both) isActive(srcId) && isActive(dstId)
            else if (activeness == EdgeActiveness.Either) isActive(srcId) || isActive(dstId)
            else throw new Exception("unreachable")
          if (edgeIsActive) {

            val srcAttr = if (tripletFields.useSrc) attrs(global2local(srcId)) else null.asInstanceOf[VD]
            ctx.set(srcId, dstId, global2local(srcId), global2local(dstId), srcAttr, edgeAttrs(dstIndex + i))
            sendMsg(ctx)
          }
          i += 1
        }

      pos = activeSet.nextSetBit(pos+1)
      }



    bitset.iterator.map { localId => (local2global(localId), aggregates(localId)) }
  }


}

private class AggregatingVertexContext[VD, ED, A](
                                                   mergeMsg: (A, A) => A,
                                                   aggregates: Array[A],
                                                   bitset: BitSet)
  extends MyVertexContext[VD, ED, A] {

  private[this] var _srcId: VertexId = _
  private[this] var _dstId: VertexId = _
  private[this] var _localSrcId: Int = _
  private[this] var _localDstId: Int = _
  private[this] var _srcAttr: VD = _
  private[this] var _attr: ED = _

  def set(
           srcId: VertexId, dstId: VertexId,
           localSrcId: Int, localDstId: Int,
           srcAttr: VD, attr: ED) {
    _srcId = srcId
    _dstId = dstId
    _localSrcId = localSrcId
    _localDstId = localDstId
    _srcAttr = srcAttr
    _attr = attr
  }

  def setSrcOnly(srcId: VertexId, localSrcId: Int, srcAttr: VD) {
    _srcId = srcId
    _localSrcId = localSrcId
    _srcAttr = srcAttr
  }

  def setRest(dstId: VertexId, localDstId: Int, dstAttr: VD, attr: ED) {
    _dstId = dstId
    _localDstId = localDstId
    _attr = attr
  }

  override def srcId: VertexId = _srcId

  override def dstId: VertexId = _dstId

  override def srcAttr: VD = _srcAttr

  override def attr: ED = _attr

  override def sendToDst(msg: A) {
    send(_localDstId, msg)
  }

  override def sendToSrc(msg: A) {
    send(_localSrcId, msg)
  }

  @inline private def send(localId: Int, msg: A) {


    if (bitset.get(localId)) {
      aggregates(localId) = mergeMsg(aggregates(localId), msg)
    } else {
      aggregates(localId) = msg
      bitset.set(localId)
    }
  }
}