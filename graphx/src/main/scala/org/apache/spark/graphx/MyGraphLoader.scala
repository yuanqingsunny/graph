package org.apache.spark.graphx

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel

/**
 * Created by sunny on 4/25/16.
 */
object MyGraphLoader  extends Logging{
  def edgeListFile(
                    sc: SparkContext,
                    path: String,
                    canonicalOrientation: Boolean = false,
                    numVertexPartitions: Int = -1,
                    edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
                    vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  : MyGraph[Int, Int] =
  {
    val startTime = System.currentTimeMillis

    // Parse the edge data table directly into edge partitions
    val lines =
      if (numVertexPartitions > 0) {
        sc.textFile(path, numVertexPartitions).coalesce(numVertexPartitions)
      } else {
        sc.textFile(path)
      }


    val mid_data = lines.map(line => {
      val parts = line.split("\\s+")
      (parts(0).toLong, parts(1).toLong)
    })++ lines.map(line => {
      val parts = line.split("\\s+")
      (parts(1).toLong,-1l)
    }) ++ lines.map(line => {
      val parts = line.split("\\s+")
      (parts(0).toLong,-1l)
    })


    val links = mid_data.groupByKey(new HashPartitioner(numVertexPartitions)).cache()



    println("It took %d ms to group".format(System.currentTimeMillis - startTime))
    //添加出度为0的节点的邻接表项 如：(4,()) (5,())...



    MyGraphImpl.fromEdgeList(links, defaultVertexAttr = 1, edgeStorageLevel = edgeStorageLevel,
      vertexStorageLevel = vertexStorageLevel)
  } // end of edgeListFile
}
