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

package org.apache.spark.sql.hive.orc

import java.net.URI
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.ql.io.orc._
import org.apache.hadoop.hive.ql.io.orc.OrcFile.OrcTableProperties
import org.apache.hadoop.hive.serde2.objectinspector.{SettableStructObjectInspector, StructObjectInspector}
import org.apache.hadoop.hive.serde2.typeinfo.{StructTypeInfo, TypeInfoUtils}
import org.apache.hadoop.io.{NullWritable, Writable}
import org.apache.hadoop.mapred.{InputFormat => MapRedInputFormat, JobConf, OutputFormat => MapRedOutputFormat, RecordWriter, Reporter}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{HadoopRDD, RDD}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.hive.{HiveInspectors, HiveMetastoreTypes, HiveShim}
import org.apache.spark.sql.sources.{Filter, _}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.util.collection.BitSet

private[sql] class DefaultSource
  extends FileFormat with DataSourceRegister with Serializable {

  override def shortName(): String = "orc"

  override def toString: String = "ORC"

  override def inferSchema(
      sqlContext: SQLContext,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    OrcFileOperator.readSchema(
      files.map(_.getPath.toUri.toString),
      Some(sqlContext.sparkContext.hadoopConfiguration)
    )
  }

  override def prepareWrite(
      sqlContext: SQLContext,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    val compressionCodec: Option[String] = options
        .get("compression")
        .map { codecName =>
          // Validate if given compression codec is supported or not.
          val shortOrcCompressionCodecNames = OrcRelation.shortOrcCompressionCodecNames
          if (!shortOrcCompressionCodecNames.contains(codecName.toLowerCase)) {
            val availableCodecs = shortOrcCompressionCodecNames.keys.map(_.toLowerCase)
            throw new IllegalArgumentException(s"Codec [$codecName] " +
                s"is not available. Available codecs are ${availableCodecs.mkString(", ")}.")
          }
          codecName.toLowerCase
        }

    compressionCodec.foreach { codecName =>
      job.getConfiguration.set(
        OrcTableProperties.COMPRESSION.getPropName,
        OrcRelation
          .shortOrcCompressionCodecNames
          .getOrElse(codecName, CompressionKind.NONE).name())
    }

    job.getConfiguration match {
      case conf: JobConf =>
        conf.setOutputFormat(classOf[OrcOutputFormat])
      case conf =>
        conf.setClass(
          "mapred.output.format.class",
          classOf[OrcOutputFormat],
          classOf[MapRedOutputFormat[_, _]])
    }

    new OutputWriterFactory {
      override def newInstance(
          path: String,
          bucketId: Option[Int],
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        new OrcOutputWriter(path, bucketId, dataSchema, context)
      }
    }
  }

  override def buildInternalScan(
      sqlContext: SQLContext,
      dataSchema: StructType,
      requiredColumns: Array[String],
      filters: Array[Filter],
      bucketSet: Option[BitSet],
      inputFiles: Seq[FileStatus],
      broadcastedConf: Broadcast[SerializableConfiguration],
      options: Map[String, String]): RDD[InternalRow] = {
    val output = StructType(requiredColumns.map(dataSchema(_))).toAttributes
    OrcTableScan(sqlContext, output, filters, inputFiles).execute()
  }

  override def buildReader(
      sqlContext: SQLContext,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String]): (PartitionedFile) => Iterator[InternalRow] = {
    val orcConf = new Configuration(sqlContext.sparkContext.hadoopConfiguration)

    if (sqlContext.conf.orcFilterPushDown) {
      // Sets pushed predicates
      OrcFilters.createFilter(filters.toArray).foreach { f =>
        orcConf.set(OrcTableScan.SARG_PUSHDOWN, f.toKryo)
        orcConf.setBoolean(ConfVars.HIVEOPTINDEXFILTER.varname, true)
      }
    }

    val broadcastedConf = sqlContext.sparkContext.broadcast(new SerializableConfiguration(orcConf))

    (file: PartitionedFile) => {
      val conf = broadcastedConf.value.value

      // SPARK-8501: Empty ORC files always have an empty schema stored in their footer. In this
      // case, `OrcFileOperator.readSchema` returns `None`, and we can't read the underlying file
      // using the given physical schema. Instead, we simply return an empty iterator.
      val maybePhysicalSchema = OrcFileOperator.readSchema(Seq(file.filePath), Some(conf))
      if (maybePhysicalSchema.isEmpty) {
        Iterator.empty
      } else {
        val physicalSchema = maybePhysicalSchema.get
        OrcRelation.setRequiredColumns(conf, physicalSchema, requiredSchema)

        val orcRecordReader = {
          val job = Job.getInstance(conf)
          FileInputFormat.setInputPaths(job, file.filePath)

          val inputFormat = new OrcNewInputFormat
          val fileSplit = new FileSplit(
            new Path(new URI(file.filePath)), file.start, file.length, Array.empty
          )

          val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
          val hadoopAttemptContext = new TaskAttemptContextImpl(conf, attemptId)
          inputFormat.createRecordReader(fileSplit, hadoopAttemptContext)
        }

        // Unwraps `OrcStruct`s to `UnsafeRow`s
        val unsafeRowIterator = OrcRelation.unwrapOrcStructs(
          file.filePath, conf, requiredSchema, new RecordReaderIterator[OrcStruct](orcRecordReader)
        )

        // Appends partition values
        val fullOutput = requiredSchema.toAttributes ++ partitionSchema.toAttributes
        val joinedRow = new JoinedRow()
        val appendPartitionColumns = GenerateUnsafeProjection.generate(fullOutput, fullOutput)

        unsafeRowIterator.map { dataRow =>
          appendPartitionColumns(joinedRow(dataRow, file.partitionValues))
        }
      }
    }
  }
}

private[orc] class OrcOutputWriter(
    path: String,
    bucketId: Option[Int],
    dataSchema: StructType,
    context: TaskAttemptContext)
  extends OutputWriter with HiveInspectors {

  private val serializer = {
    val table = new Properties()
    table.setProperty("columns", dataSchema.fieldNames.mkString(","))
    table.setProperty("columns.types", dataSchema.map { f =>
      HiveMetastoreTypes.toMetastoreType(f.dataType)
    }.mkString(":"))

    val serde = new OrcSerde
    val configuration = context.getConfiguration
    serde.initialize(configuration, table)
    serde
  }

  // Object inspector converted from the schema of the relation to be written.
  private val structOI = {
    val typeInfo =
      TypeInfoUtils.getTypeInfoFromTypeString(
        HiveMetastoreTypes.toMetastoreType(dataSchema))

    OrcStruct.createObjectInspector(typeInfo.asInstanceOf[StructTypeInfo])
      .asInstanceOf[SettableStructObjectInspector]
  }

  // `OrcRecordWriter.close()` creates an empty file if no rows are written at all.  We use this
  // flag to decide whether `OrcRecordWriter.close()` needs to be called.
  private var recordWriterInstantiated = false

  private lazy val recordWriter: RecordWriter[NullWritable, Writable] = {
    recordWriterInstantiated = true

    val conf = context.getConfiguration
    val uniqueWriteJobId = conf.get("spark.sql.sources.writeJobUUID")
    val taskAttemptId = context.getTaskAttemptID
    val partition = taskAttemptId.getTaskID.getId
    val bucketString = bucketId.map(BucketingUtils.bucketIdToString).getOrElse("")
    val compressionExtension = {
      val name = conf.get(OrcTableProperties.COMPRESSION.getPropName)
      OrcRelation.extensionsForCompressionCodecNames.getOrElse(name, "")
    }
    // It has the `.orc` extension at the end because (de)compression tools
    // such as gunzip would not be able to decompress this as the compression
    // is not applied on this whole file but on each "stream" in ORC format.
    val filename = f"part-r-$partition%05d-$uniqueWriteJobId$bucketString$compressionExtension.orc"

    new OrcOutputFormat().getRecordWriter(
      new Path(path, filename).getFileSystem(conf),
      conf.asInstanceOf[JobConf],
      new Path(path, filename).toString,
      Reporter.NULL
    ).asInstanceOf[RecordWriter[NullWritable, Writable]]
  }

  override def write(row: Row): Unit =
    throw new UnsupportedOperationException("call writeInternal")

  private def wrapOrcStruct(
      struct: OrcStruct,
      oi: SettableStructObjectInspector,
      row: InternalRow): Unit = {
    val fieldRefs = oi.getAllStructFieldRefs
    var i = 0
    while (i < fieldRefs.size) {

      oi.setStructFieldData(
        struct,
        fieldRefs.get(i),
        wrap(
          row.get(i, dataSchema(i).dataType),
          fieldRefs.get(i).getFieldObjectInspector,
          dataSchema(i).dataType))
      i += 1
    }
  }

  val cachedOrcStruct = structOI.create().asInstanceOf[OrcStruct]

  override protected[sql] def writeInternal(row: InternalRow): Unit = {
    wrapOrcStruct(cachedOrcStruct, structOI, row)

    recordWriter.write(
      NullWritable.get(),
      serializer.serialize(cachedOrcStruct, structOI))
  }

  override def close(): Unit = {
    if (recordWriterInstantiated) {
      recordWriter.close(Reporter.NULL)
    }
  }
}

private[orc] case class OrcTableScan(
    @transient sqlContext: SQLContext,
    attributes: Seq[Attribute],
    filters: Array[Filter],
    @transient inputPaths: Seq[FileStatus])
  extends Logging
  with HiveInspectors {

  def execute(): RDD[InternalRow] = {
    val job = Job.getInstance(sqlContext.sparkContext.hadoopConfiguration)
    val conf = job.getConfiguration

    // Tries to push down filters if ORC filter push-down is enabled
    if (sqlContext.conf.orcFilterPushDown) {
      OrcFilters.createFilter(filters).foreach { f =>
        conf.set(OrcTableScan.SARG_PUSHDOWN, f.toKryo)
        conf.setBoolean(ConfVars.HIVEOPTINDEXFILTER.varname, true)
      }
    }

    // Figure out the actual schema from the ORC source (without partition columns) so that we
    // can pick the correct ordinals.  Note that this assumes that all files have the same schema.
    val orcFormat = new DefaultSource
    val dataSchema =
      orcFormat
        .inferSchema(sqlContext, Map.empty, inputPaths)
        .getOrElse(sys.error("Failed to read schema from target ORC files."))
    // Sets requested columns
    OrcRelation.setRequiredColumns(conf, dataSchema, StructType.fromAttributes(attributes))

    if (inputPaths.isEmpty) {
      // the input path probably be pruned, return an empty RDD.
      return sqlContext.sparkContext.emptyRDD[InternalRow]
    }
    FileInputFormat.setInputPaths(job, inputPaths.map(_.getPath): _*)

    val inputFormatClass =
      classOf[OrcInputFormat]
        .asInstanceOf[Class[_ <: MapRedInputFormat[NullWritable, Writable]]]

    val rdd = sqlContext.sparkContext.hadoopRDD(
      conf.asInstanceOf[JobConf],
      inputFormatClass,
      classOf[NullWritable],
      classOf[Writable]
    ).asInstanceOf[HadoopRDD[NullWritable, Writable]]

    val wrappedConf = new SerializableConfiguration(conf)

    rdd.mapPartitionsWithInputSplit { case (split: OrcSplit, iterator) =>
      val writableIterator = iterator.map(_._2)
      OrcRelation.unwrapOrcStructs(
        split.getPath.toString,
        wrappedConf.value,
        StructType.fromAttributes(attributes),
        writableIterator
      )
    }
  }
}

private[orc] object OrcTableScan {
  // This constant duplicates `OrcInputFormat.SARG_PUSHDOWN`, which is unfortunately not public.
  private[orc] val SARG_PUSHDOWN = "sarg.pushdown"
}

private[orc] object OrcRelation extends HiveInspectors {
  // The ORC compression short names
  val shortOrcCompressionCodecNames = Map(
    "none" -> CompressionKind.NONE,
    "uncompressed" -> CompressionKind.NONE,
    "snappy" -> CompressionKind.SNAPPY,
    "zlib" -> CompressionKind.ZLIB,
    "lzo" -> CompressionKind.LZO)

  // The extensions for ORC compression codecs
  val extensionsForCompressionCodecNames = Map(
    CompressionKind.NONE.name -> "",
    CompressionKind.SNAPPY.name -> ".snappy",
    CompressionKind.ZLIB.name -> ".zlib",
    CompressionKind.LZO.name -> ".lzo"
  )

  def unwrapOrcStructs(
      filePath: String,
      conf: Configuration,
      dataSchema: StructType,
      iterator: Iterator[Writable]): Iterator[InternalRow] = {
    val deserializer = new OrcSerde
    val maybeStructOI = OrcFileOperator.getObjectInspector(filePath, Some(conf))
    val mutableRow = new SpecificMutableRow(dataSchema.map(_.dataType))
    val unsafeProjection = UnsafeProjection.create(dataSchema)

    def unwrap(oi: StructObjectInspector): Iterator[InternalRow] = {
      val (fieldRefs, fieldOrdinals) = dataSchema.zipWithIndex.map {
        case (field, ordinal) => oi.getStructFieldRef(field.name) -> ordinal
      }.unzip

      val unwrappers = fieldRefs.map(unwrapperFor)

      iterator.map { value =>
        val raw = deserializer.deserialize(value)
        var i = 0
        while (i < fieldRefs.length) {
          val fieldValue = oi.getStructFieldData(raw, fieldRefs(i))
          if (fieldValue == null) {
            mutableRow.setNullAt(fieldOrdinals(i))
          } else {
            unwrappers(i)(fieldValue, mutableRow, fieldOrdinals(i))
          }
          i += 1
        }
        unsafeProjection(mutableRow)
      }
    }

    maybeStructOI.map(unwrap).getOrElse(Iterator.empty)
  }

  def setRequiredColumns(
      conf: Configuration, physicalSchema: StructType, requestedSchema: StructType): Unit = {
    val ids = requestedSchema.map(a => physicalSchema.fieldIndex(a.name): Integer)
    val (sortedIDs, sortedNames) = ids.zip(requestedSchema.fieldNames).sorted.unzip
    HiveShim.appendReadColumns(conf, sortedIDs, sortedNames)
  }
}
