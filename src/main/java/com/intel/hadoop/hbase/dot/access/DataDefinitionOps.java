/**
 * Copyright 2007 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.intel.hadoop.hbase.dot.access;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;

import com.intel.hadoop.hbase.dot.DotConstants;
import com.intel.hadoop.hbase.dot.DotException;
import com.intel.hadoop.hbase.dot.DotInvalidIOException;
import com.intel.hadoop.hbase.dot.DotUtil;

/**
 * All the data definition relate operations are defined in the class, like: <br>
 * create a table; <br>
 * delete a table; <br>
 * alter a table; <br>
 * alter a column; <br>
 * ...
 *
 */
public class DataDefinitionOps extends BaseMasterObserver {
  private static final Log LOG = LogFactory.getLog(DataDefinitionOps.class);

  private void checkColumn(HTableDescriptor desc, HColumnDescriptor column)
      throws IOException {
    if (DotUtil.isDot(desc))
      checkDotColumn(desc, column);
  }

  /**
   * Check the column family contains dot required properties.
   * @param desc
   * @param column
   * @throws IOException
   */
  private void checkDotColumn(HTableDescriptor desc, HColumnDescriptor column)
      throws IOException {
    String docElements = column
        .getValue(DotConstants.HBASE_DOT_COLUMNFAMILY_DOC_ELEMENT);
    if (null == docElements || docElements.equals("")) {
      LOG.error("No DOC element is defined in column family: "
          + column.getNameAsString() + " of a DOT table: "
          + desc.getNameAsString());
      throw new DotInvalidIOException(DotException.DOC_ELEMENTS_EMPTY);
    }

    String serializer = column
        .getValue(DotConstants.HBASE_DOT_COLUMNFAMILY_DOC_SERIALIZER_CLASS);
    if (null == serializer || serializer.equals("")) {
      // set the default serializer for current column family.
      column.setValue(DotConstants.HBASE_DOT_COLUMNFAMILY_DOC_SERIALIZER_CLASS,
          DotConstants.HBASE_DOT_COLUMNFAMILY_DOC_SERIALIZER_CLASS_DEFAULT);
    }

    for (String doc : StringUtils.getStrings(docElements)) {
      String schema = column
          .getValue(DotConstants.HBASE_DOT_COLUMNFAMILY_DOC_SCHEMA_PREFIX + doc);
      if (null == schema || schema.equals("")) {
        LOG.error("No DOC schmea is defined for DOC:" + doc
            + " in column family:" + column.getNameAsString()
            + " of a DOT table: " + desc.getNameAsString());
        throw new DotInvalidIOException(DotException.DOC_SCHEMA_EMPTY);
      }
    }

  }

  /**
   * To check if the current table is a DOT table, if it is <br>
   * 1. check supported dot type, if null set default type "ANALYTICAL" <br>
   * 2. check any new column family declared, if so <br>
   * 2.1 to validate its doc element and doc schema, otherwise throw exception <br>
   * 2.2 otherwise, just ignore<br>
   *
   * If it isn't, just ignore
   */
  @Override
  public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {

    if (DotUtil.isDot(desc)) {

      String dotType = desc.getValue(DotConstants.HBASE_DOT_TABLE_TYPE);
      if (null == dotType) {
        desc.setValue(DotConstants.HBASE_DOT_TABLE_TYPE,
            DotConstants.HBASE_DOT_TABLE_TYPE_DEFAULT);
      } else {
        if (!dotType.equals(DotConstants.HBASE_DOT_TABLE_TYPE_DEFAULT)) {
          LOG.error(DotInvalidIOException.INVALID_DOT_TYPE + ": " + dotType);
          throw new DotInvalidIOException(DotException.INVALID_DOT_TYPE + ": "
              + dotType);
        }
      }

      for (HColumnDescriptor column : desc.getColumnFamilies()) {
        checkDotColumn(desc, column);
      }

    }
  }

  /**
   * To check if the current table is a DOT table, if it is<br>
   * 1. check the new column family declared with doc element and schema,
   * otherwise throw exception<br>
   * If it isn't, just ignore
   */
  @Override
  public void preAddColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
      byte[] tableName, HColumnDescriptor column) throws IOException {
    HTableDescriptor desc = ctx.getEnvironment().getTable(tableName)
        .getTableDescriptor();
    checkColumn(desc, column);
  }

  /**
   * To check if the user to modify table descriptor, only for dot specified<br>
   * items <br>
   * 1. isDot <br>
   * 2. dot type <br>
   * 3. column family (doc_element, doc_schema)<br>
   */
  @Override
  public void preModifyTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      byte[] tableName, HTableDescriptor htd) throws IOException {
    HTableDescriptor oldDesc = ctx.getEnvironment().getTable(tableName)
        .getTableDescriptor();

    // TODO need to allow other operations such as modify block_size
    if (DotUtil.isDot(oldDesc) || DotUtil.isDot(htd)) {
      LOG.error(DotException.ILLEGAL_DOT_OPRATION + ": modifyTable");
      throw new DotInvalidIOException(DotException.ILLEGAL_DOT_OPRATION);
    }
  }

  /**
   * To check if the user modify column descriptor, only for dot specified items<br>
   * 1. dot element list <br>
   * 2. dot schema or schema file <br>
   */
  @Override
  public void preModifyColumn(
      ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName,
      HColumnDescriptor descriptor) throws IOException {
    HTableDescriptor desc = ctx.getEnvironment().getTable(tableName)
        .getTableDescriptor();

    // TODO need to allow other operations
    if (DotUtil.isDot(desc)) {
      LOG.error(DotException.ILLEGAL_DOT_OPRATION + ": modifyColumn");
      throw new DotInvalidIOException(DotException.ILLEGAL_DOT_OPRATION);
    }
  }

  /**
   * To delete a column in dot table. Currently, it is not allowed to delete a
   * column faimily in dot table after it is created.
   */
  @Override
  public void preDeleteColumn(
      ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName,
      byte[] c) throws IOException {
    HTableDescriptor desc = ctx.getEnvironment().getTable(tableName)
        .getTableDescriptor();
    if (DotUtil.isDot(desc)) {
      LOG.error(DotException.ILLEGAL_DOT_OPRATION + ": deletColumn");
      throw new DotInvalidIOException(DotException.ILLEGAL_DOT_OPRATION);
    }
  }

}
