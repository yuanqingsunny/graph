package org.apache.spark.examples

import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by sunny on 5/25/16.
 */
object BreadFirstSearch {

  def main(args: Array[String]) {
    val startTime = System.currentTimeMillis()
    val conf = new SparkConf().setAppName("BreadFirstSearch")
    val spark = new SparkContext(conf)
    val workGraph = MyGraphLoader.edgeListFile(spark, args(0), false, 2)

    def sendMessage(edge: EdgeTriplet[Long, Int]): Iterator[(VertexId, Long)] = {
      if (edge.srcAttr < Long.MaxValue) {
        Iterator((edge.dstId, edge.srcAttr + 1))
      } else {
        Iterator.empty
      }
    }


    val iniGraph = workGraph.mapVertices({ case (vid, attr) =>
      if (vid == 1) {
        0L
      }
      else {
        Long.MaxValue
      }
    }).cache()


    val maxIter = if(args.length < 3) 30 else args(2).toInt

    MyPregel(iniGraph, Long.MaxValue,maxIter)(

      vprog = (id, attr, msg) => math.min(attr, msg),
      sendMsg = sendMessage,
      mergeMsg = (a, b) => math.min(a, b)
    ).vertices.saveAsTextFile(args(1)+ "//bfs_result"+ startTime)
    spark.stop()
  }
}
