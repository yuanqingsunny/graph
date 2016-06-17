package org.apache.spark.graphx

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

/**
 * Created by sunny on 4/25/16.
 */
class MyGraphImpl[VD: ClassTag, ED: ClassTag] protected
(
  @transient val vertices: MyVertexRDD[VD],
  @transient val replicatedVertexView: ReplicatedVertexView[VD, ED])
  extends MyGraph[VD, ED] with Serializable with Logging {


}

object MyGraphImpl {
  def fromEdgePartitions[VD: ClassTag, ED: ClassTag]
  (
                                                      edgePartitions: RDD[(PartitionID, MyEdgePartition[ED, VD])],
                                                      defaultVertexAttr: VD,
                                                      edgeStorageLevel: StorageLevel,
                                                      vertexStorageLevel: StorageLevel): MyGraphImpl[VD, ED] = {
    fromVertexRDD(EdgeRDD.fromEdgePartitions(edgePartitions), defaultVertexAttr, edgeStorageLevel,
      vertexStorageLevel)
  }

  private def fromVertexRDD[VD: ClassTag, ED: ClassTag](
                                                         edges: MyEdgeRDDImpl[ED, VD],
                                                         defaultVertexAttr: VD,
                                                         edgeStorageLevel: StorageLevel,
                                                         vertexStorageLevel: StorageLevel): MyGraphImpl[VD, ED] = {
    val edgesCached = edges.withTargetStorageLevel(edgeStorageLevel).cache()
    val vertices =
      VertexRDD.fromEdges(edgesCached, edgesCached.partitions.length, defaultVertexAttr)
        .withTargetStorageLevel(vertexStorageLevel)
    fromExistingRDDs(vertices, edgesCached)
  }
}
