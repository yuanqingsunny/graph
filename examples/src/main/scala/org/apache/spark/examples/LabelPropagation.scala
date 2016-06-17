package org.apache.spark.examples

import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by sunny on 5/26/16.
 */
object LabelPropagation {


  def main(args: Array[String]) {

    val startTime = System.currentTimeMillis()
    val conf = new SparkConf().setAppName("BreadFirstSearch")
    val spark = new SparkContext(conf)
    val workGraph = MyGraphLoader.edgeListFile(spark, args(0), false, 2)


    val iniGraph = workGraph.mapVertices { case (vid, _) => vid }

    def sendMessage(e: EdgeTriplet[VertexId, Int]): Iterator[(VertexId, Map[VertexId, VertexId])] = {

      Iterator((e.srcId, Map(e.dstAttr -> 1L)), (e.dstId, Map(e.srcAttr -> 1L)))
    }
    //合并，计算vertexId相同的邻居结点中有哪些社区id，每个id出现了几次，得到一个(vertexId,(communityId -> num)）的集合
    def mergeMessage(count1: Map[VertexId, Long], count2: Map[VertexId, Long])
    : Map[VertexId, Long] = {
      (count1.keySet ++ count2.keySet).map { i =>
        val count1Val = count1.getOrElse(i, 0L)
        val count2Val = count2.getOrElse(i, 0L)
        i -> (count1Val + count2Val)
      }.toMap
    }

    def vertexProgram(vid: VertexId, attr: Long, message: Map[VertexId, Long]): VertexId = {
      if (message.isEmpty) attr else message.maxBy(_._2)._1
    }
    val initialMessage = Map[VertexId, Long]()

    val maxIter = if(args.length < 3) 30 else args(2).toInt
    MyPregel(iniGraph, initialMessage, maxIterations = maxIter)(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage).vertices.saveAsTextFile(args(1) + "//lp_result"+ startTime)

    spark.stop()
  }

}
