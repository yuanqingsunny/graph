package org.apache.spark.graphx

import org.apache.spark.rdd.RDD

/**
 * Created by sunny on 4/26/16.
 */
class MyVertexRDDImpl[VD] private[graphx]
(
  @transient val  partitionsRDD: RDD[(PartitionID, MyVertexPartition[ED, VD])],
  val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
(implicit override protected val vdTag: ClassTag[VD])
  extends VertexRDD[VD](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

}
