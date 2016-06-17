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

package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, Row, SQLContext}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, UnknownPartitioning}
import org.apache.spark.sql.catalyst.util.toCommentSafeString
import org.apache.spark.sql.execution.datasources.parquet.{DefaultSource => ParquetSource}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{BaseRelation, HadoopFsRelation}
import org.apache.spark.sql.types.DataType

object RDDConversions {
  def productToRowRdd[A <: Product](data: RDD[A], outputTypes: Seq[DataType]): RDD[InternalRow] = {
    data.mapPartitions { iterator =>
      val numColumns = outputTypes.length
      val mutableRow = new GenericMutableRow(numColumns)
      val converters = outputTypes.map(CatalystTypeConverters.createToCatalystConverter)
      iterator.map { r =>
        var i = 0
        while (i < numColumns) {
          mutableRow(i) = converters(i)(r.productElement(i))
          i += 1
        }

        mutableRow
      }
    }
  }

  /**
   * Convert the objects inside Row into the types Catalyst expected.
   */
  def rowToRowRdd(data: RDD[Row], outputTypes: Seq[DataType]): RDD[InternalRow] = {
    data.mapPartitions { iterator =>
      val numColumns = outputTypes.length
      val mutableRow = new GenericMutableRow(numColumns)
      val converters = outputTypes.map(CatalystTypeConverters.createToCatalystConverter)
      iterator.map { r =>
        var i = 0
        while (i < numColumns) {
          mutableRow(i) = converters(i)(r(i))
          i += 1
        }

        mutableRow
      }
    }
  }
}

/** Logical plan node for scanning data from an RDD. */
private[sql] case class LogicalRDD(
    output: Seq[Attribute],
    rdd: RDD[InternalRow])(sqlContext: SQLContext)
  extends LogicalPlan with MultiInstanceRelation {

  override def children: Seq[LogicalPlan] = Nil

  override protected final def otherCopyArgs: Seq[AnyRef] = sqlContext :: Nil

  override def newInstance(): LogicalRDD.this.type =
    LogicalRDD(output.map(_.newInstance()), rdd)(sqlContext).asInstanceOf[this.type]

  override def sameResult(plan: LogicalPlan): Boolean = plan match {
    case LogicalRDD(_, otherRDD) => rdd.id == otherRDD.id
    case _ => false
  }

  override def producedAttributes: AttributeSet = outputSet

  @transient override lazy val statistics: Statistics = Statistics(
    // TODO: Instead of returning a default value here, find a way to return a meaningful size
    // estimate for RDDs. See PR 1238 for more discussions.
    sizeInBytes = BigInt(sqlContext.conf.defaultSizeInBytes)
  )
}

/** Physical plan node for scanning data from an RDD. */
private[sql] case class PhysicalRDD(
    output: Seq[Attribute],
    rdd: RDD[InternalRow],
    override val nodeName: String) extends LeafNode {

  private[sql] override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"))

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    rdd.mapPartitionsInternal { iter =>
      val proj = UnsafeProjection.create(schema)
      iter.map { r =>
        numOutputRows += 1
        proj(r)
      }
    }
  }

  override def simpleString: String = {
    s"Scan $nodeName${output.mkString("[", ",", "]")}"
  }
}

/** Physical plan node for scanning data from a relation. */
private[sql] case class DataSourceScan(
    output: Seq[Attribute],
    rdd: RDD[InternalRow],
    @transient relation: BaseRelation,
    override val metadata: Map[String, String] = Map.empty)
  extends LeafNode with CodegenSupport {

  override val nodeName: String = relation.toString

  // Ignore rdd when checking results
  override def sameResult(plan: SparkPlan ): Boolean = plan match {
    case other: DataSourceScan => relation == other.relation && metadata == other.metadata
    case _ => false
  }

  private[sql] override lazy val metrics = if (canProcessBatches()) {
    Map("numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"),
      "scanTime" -> SQLMetrics.createTimingMetric(sparkContext, "scan time"))
  } else {
    Map("numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"))
  }

  val outputUnsafeRows = relation match {
    case r: HadoopFsRelation if r.fileFormat.isInstanceOf[ParquetSource] =>
      !SQLContext.getActive().get.conf.getConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED)
    case _: HadoopFsRelation => true
    case _ => false
  }

  override val outputPartitioning = {
    val bucketSpec = relation match {
      // TODO: this should be closer to bucket planning.
      case r: HadoopFsRelation if r.sqlContext.conf.bucketingEnabled => r.bucketSpec
      case _ => None
    }

    def toAttribute(colName: String): Attribute = output.find(_.name == colName).getOrElse {
      throw new AnalysisException(s"bucket column $colName not found in existing columns " +
        s"(${output.map(_.name).mkString(", ")})")
    }

    bucketSpec.map { spec =>
      val numBuckets = spec.numBuckets
      val bucketColumns = spec.bucketColumnNames.map(toAttribute)
      HashPartitioning(bucketColumns, numBuckets)
    }.getOrElse {
      UnknownPartitioning(0)
    }
  }

  private def canProcessBatches(): Boolean = {
    relation match {
      case r: HadoopFsRelation if r.fileFormat.isInstanceOf[ParquetSource] &&
        SQLContext.getActive().get.conf.getConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED) &&
        SQLContext.getActive().get.conf.getConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED) =>
        true
      case _ =>
        false
    }
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val unsafeRow = if (outputUnsafeRows) {
      rdd
    } else {
      rdd.mapPartitionsInternal { iter =>
        val proj = UnsafeProjection.create(schema)
        iter.map(proj)
      }
    }

    val numOutputRows = longMetric("numOutputRows")
    unsafeRow.map { r =>
      numOutputRows += 1
      r
    }
  }

  override def simpleString: String = {
    val metadataEntries = for ((key, value) <- metadata.toSeq.sorted) yield s"$key: $value"
    s"Scan $nodeName${output.mkString("[", ",", "]")}${metadataEntries.mkString(" ", ", ", "")}"
  }

  override def upstreams(): Seq[RDD[InternalRow]] = {
    rdd :: Nil
  }

  private def genCodeColumnVector(ctx: CodegenContext, columnVar: String, ordinal: String,
    dataType: DataType, nullable: Boolean): ExprCode = {
    val javaType = ctx.javaType(dataType)
    val value = ctx.getValue(columnVar, dataType, ordinal)
    val isNullVar = if (nullable) { ctx.freshName("isNull") } else { "false" }
    val valueVar = ctx.freshName("value")
    val str = s"columnVector[$columnVar, $ordinal, ${dataType.simpleString}]"
    val code = s"/* ${toCommentSafeString(str)} */\n" + (if (nullable) {
      s"""
        boolean ${isNullVar} = ${columnVar}.isNullAt($ordinal);
        $javaType ${valueVar} = ${isNullVar} ? ${ctx.defaultValue(dataType)} : ($value);
      """
    } else {
      s"$javaType ${valueVar} = $value;"
    }).trim
    ExprCode(code, isNullVar, valueVar)
  }

  // Support codegen so that we can avoid the UnsafeRow conversion in all cases. Codegen
  // never requires UnsafeRow as input.
  override protected def doProduce(ctx: CodegenContext): String = {
    val columnarBatchClz = "org.apache.spark.sql.execution.vectorized.ColumnarBatch"
    val columnVectorClz = "org.apache.spark.sql.execution.vectorized.ColumnVector"
    val input = ctx.freshName("input")
    val idx = ctx.freshName("batchIdx")
    val rowidx = ctx.freshName("rowIdx")
    val batch = ctx.freshName("batch")
    // PhysicalRDD always just has one input
    ctx.addMutableState("scala.collection.Iterator", input, s"$input = inputs[0];")
    ctx.addMutableState(columnarBatchClz, batch, s"$batch = null;")
    ctx.addMutableState("int", idx, s"$idx = 0;")
    val colVars = output.indices.map(i => ctx.freshName("colInstance" + i))
    val columnAssigns = colVars.zipWithIndex.map { case (name, i) =>
      ctx.addMutableState(columnVectorClz, name, s"$name = null;")
      s"$name = ${batch}.column($i);" }

    val row = ctx.freshName("row")
    val numOutputRows = metricTerm(ctx, "numOutputRows")

    // The input RDD can either return (all) ColumnarBatches or InternalRows. We determine this
    // by looking at the first value of the RDD and then calling the function which will process
    // the remaining. It is faster to return batches.
    // TODO: The abstractions between this class and SqlNewHadoopRDD makes it difficult to know
    // here which path to use. Fix this.

    val exprRows =
        output.zipWithIndex.map(x => new BoundReference(x._2, x._1.dataType, x._1.nullable))
    ctx.INPUT_ROW = row
    ctx.currentVars = null
    val columnsRowInput = exprRows.map(_.gen(ctx))
    val inputRow = if (outputUnsafeRows) row else null
    val scanRows = ctx.freshName("processRows")
    ctx.addNewFunction(scanRows,
      s"""
         | private void $scanRows(InternalRow $row) throws java.io.IOException {
         |   boolean firstRow = true;
         |   while (!shouldStop() && (firstRow || $input.hasNext())) {
         |     if (firstRow) {
         |       firstRow = false;
         |     } else {
         |       $row = (InternalRow) $input.next();
         |     }
         |     $numOutputRows.add(1);
         |     ${consume(ctx, columnsRowInput, inputRow).trim}
         |   }
         | }""".stripMargin)

    // Timers for how long we spent inside the scan. We can only maintain this when using batches,
    // otherwise the overhead is too high.
    if (canProcessBatches()) {
      val scanTimeMetric = metricTerm(ctx, "scanTime")
      val getBatchStart = ctx.freshName("scanStart")
      val scanTimeTotalNs = ctx.freshName("scanTime")
      ctx.currentVars = null
      val columnsBatchInput = (output zip colVars).map { case (attr, colVar) =>
        genCodeColumnVector(ctx, colVar, rowidx, attr.dataType, attr.nullable) }
      val scanBatches = ctx.freshName("processBatches")
      ctx.addMutableState("long", scanTimeTotalNs, s"$scanTimeTotalNs = 0;")

      ctx.addNewFunction(scanBatches,
        s"""
        | private void $scanBatches() throws java.io.IOException {
        |  while (true) {
        |     int numRows = $batch.numRows();
        |     if ($idx == 0) {
        |       ${columnAssigns.mkString("", "\n", "\n")}
        |       $numOutputRows.add(numRows);
        |     }
        |
        |     while (!shouldStop() && $idx < numRows) {
        |       int $rowidx = $idx++;
        |       ${consume(ctx, columnsBatchInput).trim}
        |     }
        |     if (shouldStop()) return;
        |
        |     long $getBatchStart = System.nanoTime();
        |     if (!$input.hasNext()) {
        |       $batch = null;
        |       $scanTimeMetric.add($scanTimeTotalNs / (1000 * 1000));
        |       break;
        |     }
        |     $batch = ($columnarBatchClz)$input.next();
        |     $scanTimeTotalNs += System.nanoTime() - $getBatchStart;
        |     $idx = 0;
        |   }
        | }""".stripMargin)

      val value = ctx.freshName("value")
      s"""
         | if ($batch != null) {
         |   $scanBatches();
         | } else if ($input.hasNext()) {
         |   Object $value = $input.next();
         |   if ($value instanceof $columnarBatchClz) {
         |     $batch = ($columnarBatchClz)$value;
         |     $scanBatches();
         |   } else {
         |     $scanRows((InternalRow) $value);
         |   }
         | }
       """.stripMargin
    } else {
      s"""
         |if ($input.hasNext()) {
         |  $scanRows((InternalRow) $input.next());
         |}
       """.stripMargin
    }
  }
}

private[sql] object DataSourceScan {
  // Metadata keys
  val INPUT_PATHS = "InputPaths"
  val PUSHED_FILTERS = "PushedFilters"
}
