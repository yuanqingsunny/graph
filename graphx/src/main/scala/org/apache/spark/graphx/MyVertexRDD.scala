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
  def apply[VD: ClassTag](vertices: RDD[(VertexId, VD)])= {
    val vPartitioned: RDD[(VertexId, VD)] = vertices.partitioner match {
      case Some(p) => vertices
      case None => vertices.partitionBy(new HashPartitioner(vertices.partitions.length))
    }
//    val vertexPartitions = vPartitioned.mapPartitions(
//      iter => Iterator(new MyShippableVertexPartition(iter)),
//      preservesPartitioning = true)
//    new MyVertexRDDImpl(vertexPartitions)
  }
  def mapEdgePartitions[ VD2: ClassTag,ED2: ClassTag](
                                                       f: (PartitionID, MyVertexPartition[VD,ED]) => MyVertexPartition[VD2,ED2]): MyVertexRDDImpl[VD2,ED2]
  def leftJoin[VD2: ClassTag, VD3: ClassTag]
  (other: RDD[(VertexId, VD2)])
  (f: (VertexId, VD, Option[VD2]) => VD3)
  : MyVertexRDD[VD3, ED]

  private[graphx] def withTargetStorageLevel(
                                              targetStorageLevel: StorageLevel): MyVertexRDD[VD, ED]

  def leftZipJoin[VD2: ClassTag, VD3: ClassTag]
  (other: MyVertexMessage[VD2])(f: (VertexId, VD, Option[VD2]) => VD3): MyVertexRDD[VD3,ED]

  def foreachEdge(f :(VertexId,ED) => Unit): Unit
  def mapVertexPartitions[VD2: ClassTag](
                                          f: (VertexId, VD) => VD2)
  : MyVertexRDD[VD2,ED]


  def withPartitionsRDD[VD2: ClassTag](partitionsRDD: RDD[(PartitionID, MyVertexPartition[VD2,ED])]): MyVertexRDD[VD2,ED]
  def withPartitionsRDD[VD2: ClassTag](partitionsRDD: RDD[(PartitionID,MyShippableVertexPartition[VD2])]): MyVertexMessage[VD2]
  def aggregateUsingIndex[VD2: ClassTag](
                                          messages: RDD[(VertexId, VD2)], reduceFunc: (VD2, VD2) => VD2): MyVertexMessage[VD2]


}

object MyVertexRDD {
  def fromEdges[VD: ClassTag, ED: ClassTag](edges: RDD[(VertexId,Iterable[VertexId])]): MyVertexRDD[VD, Int] = {


//    val vertexPartitions = edges.partitionsRDD.mapPartitions(_.flatMap(Function.tupled(
//      (pid: PartitionID, edgePartition: MyEdgePartition[ED]) => {
//        //highlight
//        val map = new GraphXPrimitiveKeyOpenHashMap[VertexId,Edge[ED]]
//        edgePartition.iterator.foreach(e => map.changeValue(e.srcId, e, (b: Edge[ED]) => e))
//        map
//      })))
//
//    //if need repartition
//    val partition2impl = vertexPartitions.mapPartitionsWithIndex((vid, vp) => {
//      val builder = new MyVertexPartitionBuilder[VD, ED]
//      vp.foreach(i => builder.add(i._2))
//      Iterator((vid, builder.toVertexPartition))
//    })
//

//    edges.foreach(v =>{
//      println(v._1)
//      v._2.foreach(a =>print(a +" "))
//      println()
//    })
    val vertexPartitions = edges.mapPartitionsWithIndex((pid,list) =>{
      val builder = new MyVertexPartitionBuilder[VD]()
      println("pid: "+ pid)
      list.foreach(iter =>{
          builder.add(iter)}
        )
    Iterator((pid,builder.toVertexPartition))
    }).cache()



    new MyVertexRDDImpl(vertexPartitions)

  }


}
