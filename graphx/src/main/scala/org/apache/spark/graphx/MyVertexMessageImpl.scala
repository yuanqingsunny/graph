package org.apache.spark.graphx

import org.apache.spark.OneToOneDependency
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

/**
 * Created by sunny on 5/5/16.
 */
class MyVertexMessageImpl[VD](@transient val  partitionsRDD: RDD[ MyShippableVertexPartition[VD]],
val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)

extends MyVertexMessage[VD](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

}
