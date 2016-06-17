package org.apache.spark.graphx

import org.apache.spark.{HashPartitioner, OneToOneDependency}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

/**
 * Created by sunny on 4/26/16.
 */
class MyVertexRDDImpl[ VD: ClassTag,ED: ClassTag] private[graphx]
(@transient val  partitionsRDD: RDD[(PartitionID, MyVertexPartition[VD,ED])],
  val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)

  extends MyVertexRDD[VD,ED](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

  override def aggregateUsingIndex[VD2: ClassTag](
                                                   messages: RDD[(VertexId, VD2)], reduceFunc: (VD2, VD2) => VD2): MyVertexMessage[VD2] = {
    val shuffled = messages.partitionBy(new HashPartitioner(partitionsRDD.partitions.length)).cache() //60ms
    val parts = partitionsRDD.zipPartitions(shuffled, true) { (thisIter, msgIter) =>
      thisIter.flatMap((pid) =>Iterable((pid._1,pid._2.aggregateUsingIndex(msgIter, reduceFunc))))
    }.cache()//add cache make it fast  important!!
    this.withPartitionsRDD[VD2](parts)
  }

  override  def withPartitionsRDD[VD2: ClassTag](
                                                  partitionsRDD: RDD[(PartitionID, MyVertexPartition[VD2,ED])]): MyVertexRDD[VD2,ED] = {
    new MyVertexRDDImpl(partitionsRDD, this.targetStorageLevel)
  }
  override def withPartitionsRDD[VD2: ClassTag](partitionsRDD: RDD[(PartitionID,MyShippableVertexPartition[VD2])]): MyVertexMessage[VD2] = {
    new MyVertexMessageImpl(partitionsRDD, this.targetStorageLevel)
  }

  override private[graphx] def withTargetStorageLevel(
                                                       targetStorageLevel: StorageLevel): MyVertexRDD[VD,ED] = {
    new MyVertexRDDImpl[VD,ED](this.partitionsRDD, targetStorageLevel)
  }




  override def leftJoin[VD2: ClassTag, VD3: ClassTag]
  (other: RDD[(VertexId, VD2)])
  (f: (VertexId, VD, Option[VD2]) => VD3)
  : MyVertexRDD[VD3,ED] = {
    // Test if the other vertex is a VertexRDD to choose the optimal join strategy.
    // If the other set is a VertexRDD then we use the much more efficient leftZipJoin
    other match {
      case other: MyVertexMessage[_] if this.partitioner == other.partitioner =>
        leftZipJoin(other)(f)
      case _ =>
        this.withPartitionsRDD(
          partitionsRDD.zipPartitions(
            other.partitionBy(this.partitioner.get), preservesPartitioning = true) {
            (partIter, msgs) => partIter.flatMap(v => Iterator((v._1,v._2.leftJoin(msgs)(f))))
          }
        )
    }
  }
  override def foreachEdge(f :(VertexId,ED) => Unit): Unit ={
    partitionsRDD.foreach(v =>{
      v._2.foreachEdgePartition(f)
    })
  }
  def mapEdgePartitions[ VD2: ClassTag,ED2: ClassTag](
                                                       f: (PartitionID, MyVertexPartition[VD,ED]) => MyVertexPartition[VD2,ED2]): MyVertexRDDImpl[VD2,ED2] = {
    this.withPartitionsRDD[VD2,ED2](partitionsRDD.mapPartitions({ iter =>
      if (iter.hasNext) {
        val (pid, ep) = iter.next()
        Iterator(Tuple2(pid, f(pid, ep)))
      } else {
        Iterator.empty
      }
    }, preservesPartitioning = true))
  }
  private[graphx] def withPartitionsRDD[ VD2: ClassTag,ED2: ClassTag](
                                                                       partitionsRDD: RDD[(PartitionID, MyVertexPartition[VD2,ED2])]): MyVertexRDDImpl[VD2,ED2] = {
    new MyVertexRDDImpl(partitionsRDD, this.targetStorageLevel)
  }
  override def leftZipJoin[VD2: ClassTag, VD3: ClassTag]
  (other: MyVertexMessage[VD2])(f: (VertexId, VD, Option[VD2]) => VD3): MyVertexRDD[VD3,ED] = {
    val newPartitionsRDD = partitionsRDD.zipPartitions(
      other.partitionsRDD, preservesPartitioning = true
    ) { (thisIter, otherIter) =>
      val thisPart = thisIter.next()
      val otherPart = otherIter.next()
      Iterator((thisPart._1,thisPart._2.leftJoin(otherPart._2)(f)))
    }
    this.withPartitionsRDD(newPartitionsRDD)
  }

  override def mapVertexPartitions[VD2: ClassTag](
                                                 f: (VertexId, VD) => VD2)
  : MyVertexRDD[VD2,ED] = {
    val newPartitionsRDD = partitionsRDD.mapPartitions(_.flatMap( v => Iterator((v._1,v._2.map(f)))), preservesPartitioning = true)
    this.withPartitionsRDD(newPartitionsRDD)
  }



}
