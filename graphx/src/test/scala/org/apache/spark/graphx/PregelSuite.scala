/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.graphx

import org.apache.spark.SparkFunSuite

import scala.reflect.ClassTag

class PregelSuite extends SparkFunSuite with LocalSparkContext {


  test("start iteration") {
    withSpark { sc =>
      val myStartTime = System.currentTimeMillis
      val workGraph = MyGraphLoader.edgeListFile(sc, "Facebook_genGragh_18.txt", false, 4)
      println("start pregel " + (System.currentTimeMillis - myStartTime))


    }
  }



  test("Pregel iteration") {
    withSpark { sc =>
      val myStartTime = System.currentTimeMillis
      val workGraph = GraphLoader.edgeListFile(sc, "Facebook_genGragh_18.txt", false, 8)
      println("  It took %d ms loadGraph".format(System.currentTimeMillis - myStartTime))
      val iniGraph: Graph[Double, Double] = workGraph
        .outerJoinVertices(workGraph.outDegrees) { (vid, attr, degree) => degree.getOrElse(0) }
        .mapTriplets( e => 1.0 / e.srcAttr, TripletFields.Src)
        .mapVertices( (id, attr) => 1.0 ).cache()

      val initialMessage = 1.0
      def vertexProgram(id: VertexId, attr: Double, msgSum: Double): Double = {
        val newRank = 0.15 + (1.0 - 0.15) * msgSum
        newRank
      }
      def sendMessage(edge: EdgeTriplet[Double, Double]) = {
        Iterator((edge.dstId, edge.srcAttr * edge.attr))
      }

      def mergeMessage(a: Double, b: Double): Double = a + b

      val resultGraph = Pregel(iniGraph, initialMessage, activeDirection = EdgeDirection.Out, maxIterations = 15) (vertexProgram, sendMessage, mergeMessage)
      println("pregel " + (System.currentTimeMillis - myStartTime))
    }
  }


  test("MyPregel iteration") {
    withSpark { sc =>
      val myStartTime = System.currentTimeMillis
      val workGraph = MyGraphLoader.edgeListFile(sc, "Facebook_genGragh_18.txt", false, 8)
//      workGraph.vertices.count()
      println("  It took %d ms loadGraph".format(System.currentTimeMillis - myStartTime))
      val iniGraph: MyGraph[Double, Double] = workGraph
        .outerJoinVertices(workGraph.outDegrees) { (vid, attr, degree) => degree.getOrElse(0) }
        .mapTriplets( e => 1.0 / e.srcAttr, TripletFields.Src)
        .mapVertices( (id, attr) => 1.0 ).cache()

      val initialMessage = 1.0
      def vertexProgram(id: VertexId, attr: Double, msgSum: Double): Double = {
        val newRank = 0.15 + (1.0 - 0.15) * msgSum
        newRank
      }
      def sendMessage(edge: MyEdgeTriplet[Double, Double]) = {
        Iterator((edge.dstId, edge.srcAttr * edge.attr))
      }

      def mergeMessage(a: Double, b: Double): Double = a + b

      val resultGraph = MyPregel(iniGraph, initialMessage, activeDirection = EdgeDirection.Out, maxIterations = 15) (vertexProgram, sendMessage, mergeMessage)
      println("My pregel " + (System.currentTimeMillis - myStartTime))
    }
  }




//  test("pregel iteration") {
//    withSpark { sc =>
//      val startTime = System.currentTimeMillis
//      val workGraph = GraphLoader.edgeListFile(sc, "Facebook_genGragh_15.txt", false, 8)
//
//      println("  It took %d ms loadGraph".format(System.currentTimeMillis - startTime))
//      def sendMessage(edge: EdgeTriplet[Long, Int]): Iterator[(VertexId, Long)] = {
//        if (edge.srcAttr < Long.MaxValue && edge.srcAttr + 1 < edge.dstAttr) {
//          Iterator((edge.dstId, edge.srcAttr + 1))
//        } else {
//          Iterator.empty
//        }
//      }
//
//
//      val iniGraph = workGraph.mapVertices({ case (vid, attr) =>
//        if (vid == 1) {
//          0L
//        }
//        else {
//          Long.MaxValue
//        }
//      }).cache()
//
//
//
//      val resultGraph = Pregel(iniGraph, Long.MaxValue)(
//
//        vprog = (id, attr, msg) => math.min(attr, msg),
//        sendMsg = sendMessage,
//        mergeMsg = (a, b) => math.min(a, b)
//      )
//
//
//
//            println("pregel")
////            for (i <- resultGraph.vertices) {
////              println(i._1, i._2)
////            }
//    }
//
//
//  }
//
//  test("MyPregel iteration") {
//    withSpark { sc =>
//      val myStartTime = System.currentTimeMillis
//      val workGraph1 = MyGraphLoader.edgeListFile(sc, "Facebook_genGragh_15.txt", false, 8)
//      println("  It took %d ms loadGraph".format(System.currentTimeMillis - myStartTime))
//
//      def mySendMessage(edge: MyEdgeTriplet[Long, Int]): Iterator[(VertexId, Long)] = {
//        if (edge.srcAttr < Long.MaxValue) {
//          Iterator((edge.dstId, edge.srcAttr + 1))
//        } else {
//          Iterator.empty
//        }
//      }
//
//
//      val iniGraph1 = workGraph1.mapVertices({ case (vid, attr) =>
//        if (vid == 1) {
//          0L
//        }
//        else {
//          Long.MaxValue
//        }
//      }).cache()
//
//
//
//      val resultGraph1 = MyPregel(iniGraph1, Long.MaxValue,30)(
//
//        vprog = (id, attr, msg) => math.min(attr, msg),
//        sendMsg = mySendMessage,
//        mergeMsg = (a, b) => math.min(a, b)
//      )
//
//      println("My pregel")
//
//



//      for (i <- resultGraph.vertices) {
//        println(i._1, i._2)
//      }
//    }
//  }







//  test("mypregel iteration") {
//    withSpark { sc =>
//      val myStartTime = System.currentTimeMillis
//      val workGraph1 = MyGraphLoader.edgeListFile(sc, "Facebook_genGragh_18.txt", false, 8)
//      println("  It took %d ms loadGraph".format(System.currentTimeMillis - myStartTime))
//      val iniGraph = workGraph1.mapVertices { case (vid, _) => vid }
//
//      def sendMessage(e: MyEdgeTriplet[VertexId, Int]): Iterator[(VertexId, Map[VertexId, VertexId])] = {
//
//        Iterator((e.dstId, Map(e.srcAttr -> 1L)))
//      }
//
//      def mergeMessage(count1: Map[VertexId, Long], count2: Map[VertexId, Long])
//      : Map[VertexId, Long] = {
//        (count1.keySet ++ count2.keySet).map { i =>
//          val count1Val = count1.getOrElse(i, 0L)
//          val count2Val = count2.getOrElse(i, 0L)
//          i -> (count1Val + count2Val)
//        }.toMap
//      }
//
//      def vertexProgram(vid: VertexId, attr: Long, message: Map[VertexId, Long]): VertexId = {
//        if (message.isEmpty) attr else message.maxBy(_._2)._1
//      }
//      val initialMessage = Map[VertexId, Long]()
//
//
//      MyPregel(iniGraph, initialMessage, maxIterations = 15)(
//        vprog = vertexProgram,
//        sendMsg = sendMessage,
//        mergeMsg = mergeMessage)
//
//
//
////
////
//
//      println("My pregel " + (System.currentTimeMillis - myStartTime))
//
//
//    }
//  }

//
//  test("pregel iteration") {
//    withSpark { sc =>
//      val myStartTime = System.currentTimeMillis
//      val workGraph1 = GraphLoader.edgeListFile(sc, "Facebook_genGragh_18.txt", false, 8)
//      println("  It took %d ms loadGraph".format(System.currentTimeMillis - myStartTime))
//      val iniGraph = workGraph1.mapVertices { case (vid, _) => vid }
//
//      def sendMessage(e: EdgeTriplet[VertexId, Int]): Iterator[(VertexId, Map[VertexId, VertexId])] = {
//
//        Iterator((e.dstId, Map(e.srcAttr -> 1L)))
//      }
//      //合并，计算vertexId相同的邻居结点中有哪些社区id，每个id出现了几次，得到一个(vertexId,(communityId -> num)）的集合
//      def mergeMessage(count1: Map[VertexId, Long], count2: Map[VertexId, Long])
//      : Map[VertexId, Long] = {
//        (count1.keySet ++ count2.keySet).map { i =>
//          val count1Val = count1.getOrElse(i, 0L)
//          val count2Val = count2.getOrElse(i, 0L)
//          i -> (count1Val + count2Val)
//        }.toMap
//      }
//
//      def vertexProgram(vid: VertexId, attr: Long, message: Map[VertexId, Long]): VertexId = {
//        if (message.isEmpty) attr else message.maxBy(_._2)._1
//      }
//      val initialMessage = Map[VertexId, Long]()
//
//
//      Pregel(iniGraph, initialMessage, maxIterations = 15)(
//        vprog = vertexProgram,
//        sendMsg = sendMessage,
//        mergeMsg = mergeMessage)
//
//
//
//      //
//      //
//
//      println(" pregel " + (System.currentTimeMillis - myStartTime))
//
//
//    }
//  }

  }
//
//
//
//