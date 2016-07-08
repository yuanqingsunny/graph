package org.apache.spark.graphx

import org.apache.spark.internal.Logging

import scala.reflect.ClassTag

/**
 * Created by sunny on 5/6/16.
 */
object MyPregel extends Logging {

  def apply[VD: ClassTag, ED: ClassTag, A: ClassTag]
  (graph: MyGraph[VD, ED],
   initialMsg: A,
   maxIterations: Int = Int.MaxValue,
   activeDirection: EdgeDirection = EdgeDirection.Either)
  (vprog: (VertexId, VD, A) => VD,
   sendMsg: MyEdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
   mergeMsg: (A, A) => A)
  : MyGraph[VD, ED] =
  {
    require(maxIterations > 0, s"Maximum of iterations must be greater than 0," +
      s" but got ${maxIterations}")

    var g = graph.mapVertices((vid, vdata) => vprog(vid, vdata, initialMsg)).cache()

    // compute the messages
    var messages = graph.mapReduceTriplets(sendMsg, mergeMsg)
    var activeMessages = messages.count()
    // Loop
//    var prevG: MyGraph[VD, ED] = null
    var i = 0
    while (activeMessages > 0 && i < maxIterations) {
      val startTime = System.currentTimeMillis
      // Receive the messages and update the vertices.
//      prevG = g

     g = g.joinVertices(messages)(vprog)

//      println("iteration  " +i +"  It took %d ms count graph vertices".format(System.currentTimeMillis - startTime))
//      val oldMessages = messages

      messages = g.mapReduceTriplets(sendMsg, mergeMsg)


      activeMessages = messages.count()
      println("iteration  " +i +"  It took %d ms count message".format(System.currentTimeMillis - startTime))

//      oldMessages.unpersist(blocking = false)
//      prevG.unpersistVertices(blocking = false)

      i += 1

    }
//    messages.unpersist(blocking = false)
    g
  } // end of apply
}
