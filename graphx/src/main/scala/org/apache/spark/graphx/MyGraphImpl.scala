package org.apache.spark.graphx

import org.apache.spark.graphx.impl.EdgeActiveness
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

/**
 * Created by sunny on 4/25/16.
 */
class MyGraphImpl[VD: ClassTag, ED: ClassTag] protected
(
  @transient val vertices: MyVertexRDD[VD,ED])
//  @transient val myEdgeRDDImpl: MyEdgeRDDImpl[ED])
  extends MyGraph[VD, ED] with Serializable with Logging {




  override def cache(): MyGraph[VD, ED] = {
    vertices.cache()
//    myEdgeRDDImpl.cache()
    this
  }


  //override def mapEdges[ED2: ClassManifest](map: (PartitionID, Iterator[Edge[ED]]) => Iterator[ED2]): Graph[VD, ED2] = ???


  override def persist(newLevel: StorageLevel): MyGraph[VD, ED] = {
    vertices.persist(newLevel)
//    myEdgeRDDImpl.persist(newLevel)
    this
  }

  override def unpersist(blocking: Boolean = true): MyGraph[VD, ED] = {
    vertices.unpersist(blocking)
//    myEdgeRDDImpl.unpersist(blocking)
    this
  }


  override def unpersistVertices(blocking: Boolean = true): MyGraph[VD, ED] = {
    vertices.unpersist(blocking)
    // TODO: unpersist the replicated vertices in `replicatedVertexView` but leave the edges alone
    this
  }




  override def degreesRDD(edgeDirection: EdgeDirection): MyVertexMessage[Int] = {
    if (edgeDirection == EdgeDirection.In) {
      aggregateMessages(_.sendToDst(1), _ + _, TripletFields.None)
    } else if (edgeDirection == EdgeDirection.Out) {
      aggregateMessages(_.sendToSrc(1), _ + _, TripletFields.None)
    } else { // EdgeDirection.Either
      aggregateMessages(ctx => { ctx.sendToSrc(1); ctx.sendToDst(1) }, _ + _,
        TripletFields.None)
    }
  }
  override   def mapTriplets[ED2: ClassTag](
                                             map:  MyEdgeTriplet[VD, ED] => ED2,
                                             tripletFields: TripletFields): MyGraph[VD, ED2]= {

    val newVertices = vertices.mapEdgePartitions { (pid, part) =>
      part.mapTriplets(map)
    }.cache()
    new MyGraphImpl(newVertices)
  }

  override def mapVertices[VD2: ClassTag]
  (f: (VertexId, VD) => VD2)(implicit eq: VD =:= VD2 = null): MyGraph[VD2, ED] = {
    // The implicit parameter eq will be populated by the compiler if VD and VD2 are equal, and left
    // null if not
    if (eq != null) {
      vertices.cache()
      // The map preserves type, so we can use incremental replication
      val newVerts = vertices.mapVertexPartitions(f).cache()
      new MyGraphImpl(newVerts)
    } else {
      // The map does not preserve type, so we must re-replicate all vertices
      new MyGraphImpl(vertices.mapVertexPartitions(f))
    }
  }

 override def joinVertices[U: ClassTag](table: RDD[(VertexId, U)])(mapFunc: (VertexId, VD, U) => VD)
 : MyGraph[VD, ED]= {
    val uf = (id: VertexId, data: VD, o: Option[U]) => {
      o match {
        case Some(u) => mapFunc(id, data, u)
        case None => data
      }
    }
    outerJoinVertices(table)(uf)
  }


  override def outerJoinVertices[U: ClassTag, VD2: ClassTag]
  (other: RDD[(VertexId, U)])
  (updateF: (VertexId, VD, Option[U]) => VD2)
  (implicit eq: VD =:= VD2 = null): MyGraph[VD2, ED] = {
    // The implicit parameter eq will be populated by the compiler if VD and VD2 are equal, and left
    // null if not

      vertices.cache()

      // updateF preserves type, so we can use incremental replication
      val newVerts = vertices.leftJoin(other)(updateF).cache()
      new MyGraphImpl(newVerts)

  }


  override def mapReduceTriplets[A: ClassTag](
                                                                                  mapFunc: MyEdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
                                                                                  reduceFunc: (A, A) => A,
                                                                                  activeSetOpt: Option[(MyVertexRDD[_,_], EdgeDirection)] = None): MyVertexMessage[A] = {
    def sendMsg(ctx: MyVertexContext[VD, ED, A]) {
      mapFunc(ctx.toEdgeTriplet).foreach { kv =>
        val id = kv._1
        val msg = kv._2
        if (id == ctx.srcId) {
          ctx.sendToSrc(msg)
        } else {
          assert(id == ctx.dstId)
          ctx.sendToDst(msg)
        }
      }
    }
    aggregateMessagesWithActiveSet(
      sendMsg, reduceFunc, TripletFields.All, activeSetOpt)
  }

  override def aggregateMessagesWithActiveSet[A: ClassTag](
                                                            sendMsg: MyVertexContext[VD, ED, A] => Unit,
                                                            mergeMsg: (A, A) => A,
                                                            tripletFields: TripletFields,
                                                            activeSetOpt: Option[(MyVertexRDD[_,_], EdgeDirection)]): MyVertexMessage[A] = {
    val startTime = System.currentTimeMillis
    vertices.cache()
    // For each vertex, replicate its attribute only to partitions where it is
    // in the relevant position in an edge.
    val activeDirectionOpt = activeSetOpt.map(_._2)
    //debug

    // Map and combine.


    val preAgg = vertices.partitionsRDD.mapPartitions(_.flatMap {
      case (pid, vertexPartition) =>

        activeDirectionOpt match {
          case Some(EdgeDirection.Both) =>
            vertexPartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields, EdgeActiveness.Both)
          case Some(EdgeDirection.Either) =>
            vertexPartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields, EdgeActiveness.Either)
          case Some(EdgeDirection.Out) =>
          vertexPartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields, EdgeActiveness.SrcOnly)
          case Some(EdgeDirection.In) =>
            vertexPartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields, EdgeActiveness.DstOnly)
          case _ => // None
            vertexPartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields, EdgeActiveness.Neither)
        }

    }).setName("GraphImpl.aggregateMessages - preAgg")

//    val preNum =preAgg.count()
//
//    println("Number   "+ preNum +"  It took %d ms count preAgg".format(System.currentTimeMillis - startTime))
//    logInfo("impl DEBUG INFO ")
//    val mid = System.currentTimeMillis
     val r = vertices.aggregateUsingIndex(preAgg, mergeMsg)
//    val rNum = r.count()
//    println("Number   "+ rNum + "  It took %d ms count aggregate".format(System.currentTimeMillis - mid))
    r

  }
}

object MyGraphImpl {

  def fromExistingRDDs[VD: ClassTag, ED: ClassTag](
                                                    vertices: MyVertexRDD[VD,ED]
                                                    ): MyGraphImpl[VD, ED] = {
    new MyGraphImpl(vertices)
  }
  def fromEdgeList[VD: ClassTag]
  (
    edgeList: RDD[(VertexId,Iterable[VertexId])],
    defaultVertexAttr: VD,
    edgeStorageLevel: StorageLevel,
    vertexStorageLevel: StorageLevel): MyGraphImpl[VD, Int] = {
    val vertices:MyVertexRDD[VD,Int] =
      MyVertexRDD.fromEdges[VD,Int](edgeList)
        .withTargetStorageLevel(vertexStorageLevel)
    fromExistingRDDs(vertices)
  }

}
