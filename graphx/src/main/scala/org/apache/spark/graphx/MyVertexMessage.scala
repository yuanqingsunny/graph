package org.apache.spark.graphx



import org.apache.spark.rdd.RDD
import org.apache.spark.{TaskContext, Partition, Dependency, SparkContext}


import scala.reflect.ClassTag

/**
 * Created by sunny on 5/5/16.
 */
abstract class MyVertexMessage[VD] (sc: SparkContext,
                                          deps: Seq[Dependency[_]]) extends RDD[(VertexId, VD)](sc, deps) {

  private[graphx] def partitionsRDD: RDD[ MyShippableVertexPartition[VD]]

  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions


  override def compute(part: Partition, context: TaskContext): Iterator[(VertexId, VD)] = {
    val p = firstParent[ MyShippableVertexPartition[VD]].iterator(part, context)
    if (p.hasNext) {
      p.next().iterator.map(_.copy())
    } else {
      Iterator.empty
    }
  }


}
