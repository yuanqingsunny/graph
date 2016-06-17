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

package org.apache.spark.sql.execution.streaming

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row, SQLContext}
import org.apache.spark.sql.catalyst.encoders.{encoderFor, RowEncoder}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

object MemoryStream {
  protected val currentBlockId = new AtomicInteger(0)
  protected val memoryStreamId = new AtomicInteger(0)

  def apply[A : Encoder](implicit sqlContext: SQLContext): MemoryStream[A] =
    new MemoryStream[A](memoryStreamId.getAndIncrement(), sqlContext)
}

/**
 * A [[Source]] that produces value stored in memory as they are added by the user.  This [[Source]]
 * is primarily intended for use in unit tests as it can only replay data when the object is still
 * available.
 */
case class MemoryStream[A : Encoder](id: Int, sqlContext: SQLContext)
    extends Source with Logging {
  protected val encoder = encoderFor[A]
  protected val logicalPlan = StreamingRelation(this)
  protected val output = logicalPlan.output
  protected val batches = new ArrayBuffer[Dataset[A]]

  protected var currentOffset: LongOffset = new LongOffset(-1)

  def schema: StructType = encoder.schema

  def toDS()(implicit sqlContext: SQLContext): Dataset[A] = {
    Dataset(sqlContext, logicalPlan)
  }

  def toDF()(implicit sqlContext: SQLContext): DataFrame = {
    Dataset.ofRows(sqlContext, logicalPlan)
  }

  def addData(data: A*): Offset = {
    addData(data.toTraversable)
  }

  def addData(data: TraversableOnce[A]): Offset = {
    import sqlContext.implicits._
    this.synchronized {
      currentOffset = currentOffset + 1
      val ds = data.toVector.toDS()
      logDebug(s"Adding ds: $ds")
      batches.append(ds)
      currentOffset
    }
  }

  override def toString: String = s"MemoryStream[${output.mkString(",")}]"

  override def getOffset: Option[Offset] = if (batches.isEmpty) {
    None
  } else {
    Some(currentOffset)
  }

  /**
   * Returns the next batch of data that is available after `start`, if any is available.
   */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val startOrdinal =
      start.map(_.asInstanceOf[LongOffset]).getOrElse(LongOffset(-1)).offset.toInt + 1
    val endOrdinal = end.asInstanceOf[LongOffset].offset.toInt + 1
    val newBlocks = batches.slice(startOrdinal, endOrdinal)

    logDebug(
      s"MemoryBatch [$startOrdinal, $endOrdinal]: ${newBlocks.flatMap(_.collect()).mkString(", ")}")
    newBlocks
      .map(_.toDF())
      .reduceOption(_ union _)
      .getOrElse {
        sys.error("No data selected!")
      }
  }
}

/**
 * A sink that stores the results in memory. This [[Sink]] is primarily intended for use in unit
 * tests and does not provide durability.
 */
class MemorySink(schema: StructType) extends Sink with Logging {
  /** An order list of batches that have been written to this [[Sink]]. */
  private val batches = new ArrayBuffer[Array[Row]]()

  /** Returns all rows that are stored in this [[Sink]]. */
  def allData: Seq[Row] = synchronized {
    batches.flatten
  }

  def toDebugString: String = synchronized {
    batches.zipWithIndex.map { case (b, i) =>
      val dataStr = try b.mkString(" ") catch {
        case NonFatal(e) => "[Error converting to string]"
      }
      s"$i: $dataStr"
    }.mkString("\n")
  }

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (batchId == batches.size) {
      logDebug(s"Committing batch $batchId")
      batches.append(data.collect())
    } else {
      logDebug(s"Skipping already committed batch: $batchId")
    }
  }
}

