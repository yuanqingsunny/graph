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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData, TypeUtils}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Returns an Array containing the evaluation of all children expressions.
 */
case class CreateArray(children: Seq[Expression]) extends Expression {

  override def foldable: Boolean = children.forall(_.foldable)

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForSameTypeInputExpr(children.map(_.dataType), "function array")

  override def dataType: DataType = {
    ArrayType(
      children.headOption.map(_.dataType).getOrElse(NullType),
      containsNull = children.exists(_.nullable))
  }

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    new GenericArrayData(children.map(_.eval(input)).toArray)
  }

  override def genCode(ctx: CodegenContext, ev: ExprCode): String = {
    val arrayClass = classOf[GenericArrayData].getName
    val values = ctx.freshName("values")
    s"""
      final boolean ${ev.isNull} = false;
      final Object[] $values = new Object[${children.size}];
    """ +
      children.zipWithIndex.map { case (e, i) =>
        val eval = e.gen(ctx)
        eval.code + s"""
          if (${eval.isNull}) {
            $values[$i] = null;
          } else {
            $values[$i] = ${eval.value};
          }
         """
      }.mkString("\n") +
      s"final ArrayData ${ev.value} = new $arrayClass($values);"
  }

  override def prettyName: String = "array"
}

/**
 * Returns a catalyst Map containing the evaluation of all children expressions as keys and values.
 * The children are a flatted sequence of kv pairs, e.g. (key1, value1, key2, value2, ...)
 */
case class CreateMap(children: Seq[Expression]) extends Expression {
  private[sql] lazy val keys = children.indices.filter(_ % 2 == 0).map(children)
  private[sql] lazy val values = children.indices.filter(_ % 2 != 0).map(children)

  override def foldable: Boolean = children.forall(_.foldable)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.size % 2 != 0) {
      TypeCheckResult.TypeCheckFailure(s"$prettyName expects an positive even number of arguments.")
    } else if (keys.map(_.dataType).distinct.length > 1) {
      TypeCheckResult.TypeCheckFailure("The given keys of function map should all be the same " +
        "type, but they are " + keys.map(_.dataType.simpleString).mkString("[", ", ", "]"))
    } else if (values.map(_.dataType).distinct.length > 1) {
      TypeCheckResult.TypeCheckFailure("The given values of function map should all be the same " +
        "type, but they are " + values.map(_.dataType.simpleString).mkString("[", ", ", "]"))
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def dataType: DataType = {
    MapType(
      keyType = keys.headOption.map(_.dataType).getOrElse(NullType),
      valueType = values.headOption.map(_.dataType).getOrElse(NullType),
      valueContainsNull = values.exists(_.nullable))
  }

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val keyArray = keys.map(_.eval(input)).toArray
    if (keyArray.contains(null)) {
      throw new RuntimeException("Cannot use null as map key!")
    }
    val valueArray = values.map(_.eval(input)).toArray
    new ArrayBasedMapData(new GenericArrayData(keyArray), new GenericArrayData(valueArray))
  }

  override def genCode(ctx: CodegenContext, ev: ExprCode): String = {
    val arrayClass = classOf[GenericArrayData].getName
    val mapClass = classOf[ArrayBasedMapData].getName
    val keyArray = ctx.freshName("keyArray")
    val valueArray = ctx.freshName("valueArray")
    val keyData = s"new $arrayClass($keyArray)"
    val valueData = s"new $arrayClass($valueArray)"
    s"""
      final boolean ${ev.isNull} = false;
      final Object[] $keyArray = new Object[${keys.size}];
      final Object[] $valueArray = new Object[${values.size}];
    """ + keys.zipWithIndex.map {
      case (key, i) =>
        val eval = key.gen(ctx)
        s"""
          ${eval.code}
          if (${eval.isNull}) {
            throw new RuntimeException("Cannot use null as map key!");
          } else {
            $keyArray[$i] = ${eval.value};
          }
        """
    }.mkString("\n") + values.zipWithIndex.map {
      case (value, i) =>
        val eval = value.gen(ctx)
        s"""
          ${eval.code}
          if (${eval.isNull}) {
            $valueArray[$i] = null;
          } else {
            $valueArray[$i] = ${eval.value};
          }
        """
    }.mkString("\n") + s"final MapData ${ev.value} = new $mapClass($keyData, $valueData);"
  }

  override def prettyName: String = "map"
}

/**
 * Returns a Row containing the evaluation of all children expressions.
 */
case class CreateStruct(children: Seq[Expression]) extends Expression {

  override def foldable: Boolean = children.forall(_.foldable)

  override lazy val dataType: StructType = {
    val fields = children.zipWithIndex.map { case (child, idx) =>
      child match {
        case ne: NamedExpression =>
          StructField(ne.name, ne.dataType, ne.nullable, ne.metadata)
        case _ =>
          StructField(s"col${idx + 1}", child.dataType, child.nullable, Metadata.empty)
      }
    }
    StructType(fields)
  }

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    InternalRow(children.map(_.eval(input)): _*)
  }

  override def genCode(ctx: CodegenContext, ev: ExprCode): String = {
    val rowClass = classOf[GenericInternalRow].getName
    val values = ctx.freshName("values")
    s"""
      boolean ${ev.isNull} = false;
      final Object[] $values = new Object[${children.size}];
    """ +
      children.zipWithIndex.map { case (e, i) =>
        val eval = e.gen(ctx)
        eval.code + s"""
          if (${eval.isNull}) {
            $values[$i] = null;
          } else {
            $values[$i] = ${eval.value};
          }
         """
      }.mkString("\n") +
      s"final InternalRow ${ev.value} = new $rowClass($values);"
  }

  override def prettyName: String = "struct"
}


/**
 * Creates a struct with the given field names and values
 *
 * @param children Seq(name1, val1, name2, val2, ...)
 */
case class CreateNamedStruct(children: Seq[Expression]) extends Expression {

  /**
   * Returns Aliased [[Expression]]s that could be used to construct a flattened version of this
   * StructType.
   */
  def flatten: Seq[NamedExpression] = valExprs.zip(names).map {
    case (v, n) => Alias(v, n.toString)()
  }

  private lazy val (nameExprs, valExprs) =
    children.grouped(2).map { case Seq(name, value) => (name, value) }.toList.unzip

  private lazy val names = nameExprs.map(_.eval(EmptyRow))

  override lazy val dataType: StructType = {
    val fields = names.zip(valExprs).map { case (name, valExpr) =>
      StructField(name.asInstanceOf[UTF8String].toString,
        valExpr.dataType, valExpr.nullable, Metadata.empty)
    }
    StructType(fields)
  }

  override def foldable: Boolean = valExprs.forall(_.foldable)

  override def nullable: Boolean = false

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.size % 2 != 0) {
      TypeCheckResult.TypeCheckFailure(s"$prettyName expects an even number of arguments.")
    } else {
      val invalidNames = nameExprs.filterNot(e => e.foldable && e.dataType == StringType)
      if (invalidNames.nonEmpty) {
        TypeCheckResult.TypeCheckFailure(
          s"Only foldable StringType expressions are allowed to appear at odd position , got :" +
            s" ${invalidNames.mkString(",")}")
      } else if (!names.contains(null)) {
        TypeCheckResult.TypeCheckSuccess
      } else {
        TypeCheckResult.TypeCheckFailure("Field name should not be null")
      }
    }
  }

  override def eval(input: InternalRow): Any = {
    InternalRow(valExprs.map(_.eval(input)): _*)
  }

  override def genCode(ctx: CodegenContext, ev: ExprCode): String = {
    val rowClass = classOf[GenericInternalRow].getName
    val values = ctx.freshName("values")
    s"""
      boolean ${ev.isNull} = false;
      final Object[] $values = new Object[${valExprs.size}];
    """ +
      valExprs.zipWithIndex.map { case (e, i) =>
        val eval = e.gen(ctx)
        eval.code + s"""
          if (${eval.isNull}) {
            $values[$i] = null;
          } else {
            $values[$i] = ${eval.value};
          }
         """
      }.mkString("\n") +
      s"final InternalRow ${ev.value} = new $rowClass($values);"
  }

  override def prettyName: String = "named_struct"
}

/**
 * Returns a Row containing the evaluation of all children expressions. This is a variant that
 * returns UnsafeRow directly. The unsafe projection operator replaces [[CreateStruct]] with
 * this expression automatically at runtime.
 */
case class CreateStructUnsafe(children: Seq[Expression]) extends Expression {

  override def foldable: Boolean = children.forall(_.foldable)

  override lazy val resolved: Boolean = childrenResolved

  override lazy val dataType: StructType = {
    val fields = children.zipWithIndex.map { case (child, idx) =>
      child match {
        case ne: NamedExpression =>
          StructField(ne.name, ne.dataType, ne.nullable, ne.metadata)
        case _ =>
          StructField(s"col${idx + 1}", child.dataType, child.nullable, Metadata.empty)
      }
    }
    StructType(fields)
  }

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    InternalRow(children.map(_.eval(input)): _*)
  }

  override def genCode(ctx: CodegenContext, ev: ExprCode): String = {
    val eval = GenerateUnsafeProjection.createCode(ctx, children)
    ev.isNull = eval.isNull
    ev.value = eval.value
    eval.code
  }

  override def prettyName: String = "struct_unsafe"
}


/**
 * Creates a struct with the given field names and values. This is a variant that returns
 * UnsafeRow directly. The unsafe projection operator replaces [[CreateStruct]] with
 * this expression automatically at runtime.
 *
 * @param children Seq(name1, val1, name2, val2, ...)
 */
case class CreateNamedStructUnsafe(children: Seq[Expression]) extends Expression {

  private lazy val (nameExprs, valExprs) =
    children.grouped(2).map { case Seq(name, value) => (name, value) }.toList.unzip

  private lazy val names = nameExprs.map(_.eval(EmptyRow).toString)

  override lazy val dataType: StructType = {
    val fields = names.zip(valExprs).map { case (name, valExpr) =>
      StructField(name, valExpr.dataType, valExpr.nullable, Metadata.empty)
    }
    StructType(fields)
  }

  override def foldable: Boolean = valExprs.forall(_.foldable)

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    InternalRow(valExprs.map(_.eval(input)): _*)
  }

  override def genCode(ctx: CodegenContext, ev: ExprCode): String = {
    val eval = GenerateUnsafeProjection.createCode(ctx, valExprs)
    ev.isNull = eval.isNull
    ev.value = eval.value
    eval.code
  }

  override def prettyName: String = "named_struct_unsafe"
}
