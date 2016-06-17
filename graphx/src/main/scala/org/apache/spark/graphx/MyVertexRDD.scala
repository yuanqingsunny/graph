package org.apache.spark.graphx

import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

/**
 * Created by sunny on 4/25/16.
 */
abstract class MyVertexRDD[VD, ED](
                                    sc: SparkContext,
                                    deps: Seq[Dependency[_]]) extends RDD[(VertexId, VD)](sc, deps) {

  implicit protected def vdTag: ClassTag[VD]

  private[graphx] def partitionsRDD: RDD[(PartitionID, MyVertexPartition[VD, ED])]

  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  override def compute(part: Partition, context: TaskContext): Iterator[(VertexId, VD)] = {
    val p = firstParent[(PartitionID, MyVertexPartition[VD, _])].iterator(part, context)
    if (p.hasNext) {
      p.next()._2.iterator.map(_.copy())
    } else {
      Iterator.empty
    }
  }



  def leftJoin[VD2: ClassTag, VD3: ClassTag]
  (other: RDD[(VertexId, VD2)])
  (f: (VertexId, VD, Option[VD2]) => VD3)
  : MyVertexRDD[VD3,ED]

  private[graphx] def withTargetStorageLevel(
                                              targetStorageLevel: StorageLevel): MyVertexRDD[VD, ED]


  def withPartitionsRDD[VD2: ClassTag](partitionsRDD: RDD[(PartitionID, MyShippableVertexPartition[VD2])]): MyVertexMessage[VD2]

  def aggregateUsingIndex[VD2: ClassTag](
                                          messages: RDD[(VertexId, VD2)], reduceFunc: (VD2, VD2) => VD2): MyVertexMessage[VD2]


}

object MyVertexRDD {
  def fromEdges[VD: ClassTag, ED: ClassTag](edges: MyEdgeRDD[ED]): MyVertexRDD[VD, ED] = {


    val vertexPartitions = edges.partitionsRDD.mapPartitions(_.flatMap(Function.tupled(
      (pid: PartitionID, edgePartition: MyEdgePartition[ED]) => {
        //highlight
        val map = new GraphXPrimitiveKeyOpenHashMap[VertexId, Edge[ED]]
        edgePartition.iterator.foreach(e => map.changeValue(e.srcId, e, (b: Edge[ED]) => e))
        map
      })))

    //if need repartition
    val partition2impl = vertexPartitions.mapPartitionsWithIndex((vid, vp) => {
      val builder = new MyVertexPartitionBuilder[VD, ED]
      vp.foreach(i => builder.add(i._2))
      Iterator((vid, builder.toVertexPartition))
    })

    new MyVertexRDDImpl(partition2impl)

  }


}
