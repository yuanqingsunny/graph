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

package org.apache.spark.sql.execution.joins

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.memory.{StaticMemoryManager, TaskMemoryManager}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.unsafe.map.BytesToBytesMap
import org.apache.spark.util.collection.CompactBuffer

class HashedRelationSuite extends SparkFunSuite with SharedSQLContext {

  test("UnsafeHashedRelation") {
    val schema = StructType(StructField("a", IntegerType, true) :: Nil)
    val data = Array(InternalRow(0), InternalRow(1), InternalRow(2), InternalRow(2))
    val toUnsafe = UnsafeProjection.create(schema)
    val unsafeData = data.map(toUnsafe(_).copy()).toArray

    val buildKey = Seq(BoundReference(0, IntegerType, false))
    val keyGenerator = UnsafeProjection.create(buildKey)
    val hashed = UnsafeHashedRelation(unsafeData.iterator, keyGenerator, 1)
    assert(hashed.isInstanceOf[UnsafeHashedRelation])

    assert(hashed.get(unsafeData(0)) === CompactBuffer[InternalRow](unsafeData(0)))
    assert(hashed.get(unsafeData(1)) === CompactBuffer[InternalRow](unsafeData(1)))
    assert(hashed.get(toUnsafe(InternalRow(10))) === null)

    val data2 = CompactBuffer[InternalRow](unsafeData(2).copy())
    data2 += unsafeData(2).copy()
    assert(hashed.get(unsafeData(2)) === data2)

    val os = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(os)
    hashed.asInstanceOf[UnsafeHashedRelation].writeExternal(out)
    out.flush()
    val in = new ObjectInputStream(new ByteArrayInputStream(os.toByteArray))
    val hashed2 = new UnsafeHashedRelation()
    hashed2.readExternal(in)
    assert(hashed2.get(unsafeData(0)) === CompactBuffer[InternalRow](unsafeData(0)))
    assert(hashed2.get(unsafeData(1)) === CompactBuffer[InternalRow](unsafeData(1)))
    assert(hashed2.get(toUnsafe(InternalRow(10))) === null)
    assert(hashed2.get(unsafeData(2)) === data2)

    val os2 = new ByteArrayOutputStream()
    val out2 = new ObjectOutputStream(os2)
    hashed2.asInstanceOf[UnsafeHashedRelation].writeExternal(out2)
    out2.flush()
    // This depends on that the order of items in BytesToBytesMap.iterator() is exactly the same
    // as they are inserted
    assert(java.util.Arrays.equals(os2.toByteArray, os.toByteArray))
  }

  test("test serialization empty hash map") {
    val taskMemoryManager = new TaskMemoryManager(
      new StaticMemoryManager(
        new SparkConf().set("spark.memory.offHeap.enabled", "false"),
        Long.MaxValue,
        Long.MaxValue,
        1),
      0)
    val binaryMap = new BytesToBytesMap(taskMemoryManager, 1, 1)
    val os = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(os)
    val hashed = new UnsafeHashedRelation(1, binaryMap)
    hashed.writeExternal(out)
    out.flush()
    val in = new ObjectInputStream(new ByteArrayInputStream(os.toByteArray))
    val hashed2 = new UnsafeHashedRelation()
    hashed2.readExternal(in)

    val schema = StructType(StructField("a", IntegerType, true) :: Nil)
    val toUnsafe = UnsafeProjection.create(schema)
    val row = toUnsafe(InternalRow(0))
    assert(hashed2.get(row) === null)

    val os2 = new ByteArrayOutputStream()
    val out2 = new ObjectOutputStream(os2)
    hashed2.writeExternal(out2)
    out2.flush()
    assert(java.util.Arrays.equals(os2.toByteArray, os.toByteArray))
  }

  test("LongArrayRelation") {
    val unsafeProj = UnsafeProjection.create(
      Seq(BoundReference(0, IntegerType, false), BoundReference(1, IntegerType, true)))
    val rows = (0 until 100).map(i => unsafeProj(InternalRow(i, i + 1)).copy())
    val keyProj = UnsafeProjection.create(Seq(BoundReference(0, IntegerType, false)))
    val longRelation = LongHashedRelation(rows.iterator, keyProj, 100)
    assert(longRelation.isInstanceOf[LongArrayRelation])
    val longArrayRelation = longRelation.asInstanceOf[LongArrayRelation]
    (0 until 100).foreach { i =>
      val row = longArrayRelation.getValue(i)
      assert(row.getInt(0) === i)
      assert(row.getInt(1) === i + 1)
    }

    val os = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(os)
    longArrayRelation.writeExternal(out)
    out.flush()
    val in = new ObjectInputStream(new ByteArrayInputStream(os.toByteArray))
    val relation = new LongArrayRelation()
    relation.readExternal(in)
    (0 until 100).foreach { i =>
      val row = longArrayRelation.getValue(i)
      assert(row.getInt(0) === i)
      assert(row.getInt(1) === i + 1)
    }
  }
}
