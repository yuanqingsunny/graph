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

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.ContinuousQueryListener
import org.apache.spark.sql.util.ContinuousQueryListener._
import org.apache.spark.util.UninterruptibleThread

/**
 * Manages the execution of a streaming Spark SQL query that is occurring in a separate thread.
 * Unlike a standard query, a streaming query executes repeatedly each time new data arrives at any
 * [[Source]] present in the query plan. Whenever new data arrives, a [[QueryExecution]] is created
 * and the results are committed transactionally to the given [[Sink]].
 */
class StreamExecution(
    val sqlContext: SQLContext,
    override val name: String,
    val checkpointRoot: String,
    private[sql] val logicalPlan: LogicalPlan,
    val sink: Sink) extends ContinuousQuery with Logging {

  /** An monitor used to wait/notify when batches complete. */
  private val awaitBatchLock = new Object
  private val startLatch = new CountDownLatch(1)
  private val terminationLatch = new CountDownLatch(1)

  /** Minimum amount of time in between the start of each batch. */
  private val minBatchTime = 10

  /**
   * Tracks how much data we have processed and committed to the sink or state store from each
   * input source.
   */
  private[sql] var committedOffsets = new StreamProgress

  /**
   * Tracks the offsets that are available to be processed, but have not yet be committed to the
   * sink.
   */
  private var availableOffsets = new StreamProgress

  /** The current batchId or -1 if execution has not yet been initialized. */
  private var currentBatchId: Long = -1

  /** All stream sources present the query plan. */
  private val sources =
    logicalPlan.collect { case s: StreamingRelation => s.source }

  /** A list of unique sources in the query plan. */
  private val uniqueSources = sources.distinct

  /** Defines the internal state of execution */
  @volatile
  private var state: State = INITIALIZED

  @volatile
  private[sql] var lastExecution: QueryExecution = null

  @volatile
  private[sql] var streamDeathCause: ContinuousQueryException = null

  /** The thread that runs the micro-batches of this stream. */
  private[sql] val microBatchThread =
    new UninterruptibleThread(s"stream execution thread for $name") {
      override def run(): Unit = { runBatches() }
    }

  /**
   * A write-ahead-log that records the offsets that are present in each batch. In order to ensure
   * that a given batch will always consist of the same data, we write to this log *before* any
   * processing is done.  Thus, the Nth record in this log indicated data that is currently being
   * processed and the N-1th entry indicates which offsets have been durably committed to the sink.
   */
  private val offsetLog =
    new HDFSMetadataLog[CompositeOffset](sqlContext, checkpointFile("offsets"))

  /** Whether the query is currently active or not */
  override def isActive: Boolean = state == ACTIVE

  /** Returns current status of all the sources. */
  override def sourceStatuses: Array[SourceStatus] = {
    sources.map(s => new SourceStatus(s.toString, availableOffsets.get(s))).toArray
  }

  /** Returns current status of the sink. */
  override def sinkStatus: SinkStatus =
    new SinkStatus(sink.toString, committedOffsets.toCompositeOffset(sources))

  /** Returns the [[ContinuousQueryException]] if the query was terminated by an exception. */
  override def exception: Option[ContinuousQueryException] = Option(streamDeathCause)

  /** Returns the path of a file with `name` in the checkpoint directory. */
  private def checkpointFile(name: String): String =
    new Path(new Path(checkpointRoot), name).toUri.toString

  /**
   * Starts the execution. This returns only after the thread has started and [[QueryStarted]] event
   * has been posted to all the listeners.
   */
  private[sql] def start(): Unit = {
    microBatchThread.setDaemon(true)
    microBatchThread.start()
    startLatch.await()  // Wait until thread started and QueryStart event has been posted
  }

  /**
   * Repeatedly attempts to run batches as data arrives.
   *
   * Note that this method ensures that [[QueryStarted]] and [[QueryTerminated]] events are posted
   * such that listeners are guaranteed to get a start event before a termination. Furthermore, this
   * method also ensures that [[QueryStarted]] event is posted before the `start()` method returns.
   */
  private def runBatches(): Unit = {
    try {
      // Mark ACTIVE and then post the event. QueryStarted event is synchronously sent to listeners,
      // so must mark this as ACTIVE first.
      state = ACTIVE
      postEvent(new QueryStarted(this)) // Assumption: Does not throw exception.

      // Unblock starting thread
      startLatch.countDown()

      // While active, repeatedly attempt to run batches.
      SQLContext.setActive(sqlContext)
      populateStartOffsets()
      logDebug(s"Stream running from $committedOffsets to $availableOffsets")
      while (isActive) {
        if (dataAvailable) runBatch()
        commitAndConstructNextBatch()
        Thread.sleep(minBatchTime) // TODO: Could be tighter
      }
    } catch {
      case _: InterruptedException if state == TERMINATED => // interrupted by stop()
      case NonFatal(e) =>
        streamDeathCause = new ContinuousQueryException(
          this,
          s"Query $name terminated with exception: ${e.getMessage}",
          e,
          Some(committedOffsets.toCompositeOffset(sources)))
        logError(s"Query $name terminated with error", e)
    } finally {
      state = TERMINATED
      sqlContext.streams.notifyQueryTermination(StreamExecution.this)
      postEvent(new QueryTerminated(this))
      terminationLatch.countDown()
    }
  }

  /**
   * Populate the start offsets to start the execution at the current offsets stored in the sink
   * (i.e. avoid reprocessing data that we have already processed). This function must be called
   * before any processing occurs and will populate the following fields:
   *  - currentBatchId
   *  - committedOffsets
   *  - availableOffsets
   */
  private def populateStartOffsets(): Unit = {
    offsetLog.getLatest() match {
      case Some((batchId, nextOffsets)) =>
        logInfo(s"Resuming continuous query, starting with batch $batchId")
        currentBatchId = batchId + 1
        availableOffsets = nextOffsets.toStreamProgress(sources)
        logDebug(s"Found possibly uncommitted offsets $availableOffsets")

        offsetLog.get(batchId - 1).foreach {
          case lastOffsets =>
            committedOffsets = lastOffsets.toStreamProgress(sources)
            logDebug(s"Resuming with committed offsets: $committedOffsets")
        }

      case None => // We are starting this stream for the first time.
        logInfo(s"Starting new continuous query.")
        currentBatchId = 0
        commitAndConstructNextBatch()
    }
  }

  /**
   * Returns true if there is any new data available to be processed.
   */
  private def dataAvailable: Boolean = {
    availableOffsets.exists {
      case (source, available) =>
        committedOffsets
            .get(source)
            .map(committed => committed < available)
            .getOrElse(true)
    }
  }

  /**
   * Queries all of the sources to see if any new data is available. When there is new data the
   * batchId counter is incremented and a new log entry is written with the newest offsets.
   *
   * Note that committing the offsets for a new batch implicitly marks the previous batch as
   * finished and thus this method should only be called when all currently available data
   * has been written to the sink.
   */
  private def commitAndConstructNextBatch(): Boolean = {
    // Update committed offsets.
    committedOffsets ++= availableOffsets

    // There is a potential dead-lock in Hadoop "Shell.runCommand" before 2.5.0 (HADOOP-10622).
    // If we interrupt some thread running Shell.runCommand, we may hit this issue.
    // As "FileStreamSource.getOffset" will create a file using HDFS API and call "Shell.runCommand"
    // to set the file permission, we should not interrupt "microBatchThread" when running this
    // method. See SPARK-14131.
    //
    // Check to see what new data is available.
    val newData = microBatchThread.runUninterruptibly {
      uniqueSources.flatMap(s => s.getOffset.map(o => s -> o))
    }
    availableOffsets ++= newData

    if (dataAvailable) {
      // There is a potential dead-lock in Hadoop "Shell.runCommand" before 2.5.0 (HADOOP-10622).
      // If we interrupt some thread running Shell.runCommand, we may hit this issue.
      // As "offsetLog.add" will create a file using HDFS API and call "Shell.runCommand" to set
      // the file permission, we should not interrupt "microBatchThread" when running this method.
      // See SPARK-14131.
      microBatchThread.runUninterruptibly {
        assert(
          offsetLog.add(currentBatchId, availableOffsets.toCompositeOffset(sources)),
          s"Concurrent update to the log.  Multiple streaming jobs detected for $currentBatchId")
      }
      currentBatchId += 1
      logInfo(s"Committed offsets for batch $currentBatchId.")
      true
    } else {
      noNewData = true
      awaitBatchLock.synchronized {
        // Wake up any threads that are waiting for the stream to progress.
        awaitBatchLock.notifyAll()
      }

      false
    }
  }

  /**
   * Processes any data available between `availableOffsets` and `committedOffsets`.
   */
  private def runBatch(): Unit = {
    val startTime = System.nanoTime()

    // Request unprocessed data from all sources.
    val newData = availableOffsets.flatMap {
      case (source, available) if committedOffsets.get(source).map(_ < available).getOrElse(true) =>
        val current = committedOffsets.get(source)
        val batch = source.getBatch(current, available)
        logDebug(s"Retrieving data from $source: $current -> $available")
        Some(source -> batch)
      case _ => None
    }.toMap

    // A list of attributes that will need to be updated.
    var replacements = new ArrayBuffer[(Attribute, Attribute)]
    // Replace sources in the logical plan with data that has arrived since the last batch.
    val withNewSources = logicalPlan transform {
      case StreamingRelation(source, output) =>
        newData.get(source).map { data =>
          val newPlan = data.logicalPlan
          assert(output.size == newPlan.output.size,
            s"Invalid batch: ${output.mkString(",")} != ${newPlan.output.mkString(",")}")
          replacements ++= output.zip(newPlan.output)
          newPlan
        }.getOrElse {
          LocalRelation(output)
        }
    }

    // Rewire the plan to use the new attributes that were returned by the source.
    val replacementMap = AttributeMap(replacements)
    val newPlan = withNewSources transformAllExpressions {
      case a: Attribute if replacementMap.contains(a) => replacementMap(a)
    }

    val optimizerStart = System.nanoTime()

    lastExecution = new QueryExecution(sqlContext, newPlan)
    val executedPlan = lastExecution.executedPlan
    val optimizerTime = (System.nanoTime() - optimizerStart).toDouble / 1000000
    logDebug(s"Optimized batch in ${optimizerTime}ms")

    val nextBatch = Dataset.ofRows(sqlContext, newPlan)
    sink.addBatch(currentBatchId - 1, nextBatch)

    awaitBatchLock.synchronized {
      // Wake up any threads that are waiting for the stream to progress.
      awaitBatchLock.notifyAll()
    }

    val batchTime = (System.nanoTime() - startTime).toDouble / 1000000
    logInfo(s"Completed up to $availableOffsets in ${batchTime}ms")
    postEvent(new QueryProgress(this))
  }

  private def postEvent(event: ContinuousQueryListener.Event) {
    sqlContext.streams.postListenerEvent(event)
  }

  /**
   * Signals to the thread executing micro-batches that it should stop running after the next
   * batch. This method blocks until the thread stops running.
   */
  override def stop(): Unit = {
    // Set the state to TERMINATED so that the batching thread knows that it was interrupted
    // intentionally
    state = TERMINATED
    if (microBatchThread.isAlive) {
      microBatchThread.interrupt()
      microBatchThread.join()
    }
    logInfo(s"Query $name was stopped")
  }

  /**
   * Blocks the current thread until processing for data from the given `source` has reached at
   * least the given `Offset`. This method is indented for use primarily when writing tests.
   */
  def awaitOffset(source: Source, newOffset: Offset): Unit = {
    def notDone = !committedOffsets.contains(source) || committedOffsets(source) < newOffset

    while (notDone) {
      logInfo(s"Waiting until $newOffset at $source")
      awaitBatchLock.synchronized { awaitBatchLock.wait(100) }
    }
    logDebug(s"Unblocked at $newOffset for $source")
  }

  /** A flag to indicate that a batch has completed with no new data available. */
  @volatile private var noNewData = false

  override def processAllAvailable(): Unit = {
    noNewData = false
    while (!noNewData) {
      awaitBatchLock.synchronized { awaitBatchLock.wait(10000) }
      if (streamDeathCause != null) { throw streamDeathCause }
    }
    if (streamDeathCause != null) { throw streamDeathCause }
  }

  override def awaitTermination(): Unit = {
    if (state == INITIALIZED) {
      throw new IllegalStateException("Cannot wait for termination on a query that has not started")
    }
    terminationLatch.await()
    if (streamDeathCause != null) {
      throw streamDeathCause
    }
  }

  override def awaitTermination(timeoutMs: Long): Boolean = {
    if (state == INITIALIZED) {
      throw new IllegalStateException("Cannot wait for termination on a query that has not started")
    }
    require(timeoutMs > 0, "Timeout has to be positive")
    terminationLatch.await(timeoutMs, TimeUnit.MILLISECONDS)
    if (streamDeathCause != null) {
      throw streamDeathCause
    } else {
      !isActive
    }
  }

  override def toString: String = {
    s"Continuous Query - $name [state = $state]"
  }

  def toDebugString: String = {
    val deathCauseStr = if (streamDeathCause != null) {
      "Error:\n" + stackTraceToString(streamDeathCause.cause)
    } else ""
    s"""
       |=== Continuous Query ===
       |Name: $name
       |Current Offsets: $committedOffsets
       |
       |Current State: $state
       |Thread State: ${microBatchThread.getState}
       |
       |Logical Plan:
       |$logicalPlan
       |
       |$deathCauseStr
     """.stripMargin
  }

  trait State
  case object INITIALIZED extends State
  case object ACTIVE extends State
  case object TERMINATED extends State
}

private[sql] object StreamExecution {
  private val nextId = new AtomicInteger()

  def nextName: String = s"query-${nextId.getAndIncrement}"
}
