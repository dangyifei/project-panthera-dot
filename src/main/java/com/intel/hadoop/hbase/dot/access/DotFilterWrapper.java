/*
 * Copyright The Apache Software Foundation
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;


/**
 * This is a Filter wrapper class to do extra works for Dot upon real filter.
 */
public class DotFilterWrapper implements Filter {
  Filter filter = null;
  DataManipulationOps agent = null;

  public Filter getFilterInstance() {
    return this.filter;
  }

  public DotFilterWrapper( Filter filter, DataManipulationOps agent ) {
    if (null == filter) {
      // ensure the filter instance is not null
      throw new NullPointerException("Cannot create FilterWrapper with null filter");
    }

    if (null == agent){
      // ensure the DatamanipulationOps instance is not null
      throw new NullPointerException("Cannot create FilterWrapper with null agent");
    }
      
    this.filter = filter;
    this.agent = agent;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // TODO need to write our extra info?
    this.filter.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // TODO need to write our extra info?
    this.filter.readFields(in);
  }

  @Override
  public void reset() {
    this.filter.reset();
  }

  @Override
  public boolean filterAllRemaining() {
    // TODO add co-processor supporting if necessary
    return this.filter.filterAllRemaining();
  }

  @Override
  public boolean filterRow() {
    return this.filter.filterRow();
  }

  @Override
  public boolean hasFilterRow() {
    return this.filter.hasFilterRow();
  }

  @Override
  public KeyValue getNextKeyHint(KeyValue raw) {
    try {
      return agent.doGetNextKeyHint(raw, this.filter);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean filterRowKey(byte[] buffer, int offset, int length) {
      return this.filter.filterRowKey(buffer, offset, length);
  }

  @Override
  public ReturnCode filterKeyValue(KeyValue v) {
    try {
      return agent.doFilterKeyValue(v, this.filter);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public KeyValue transform(KeyValue raw) {
    try {
      return agent.doFilterTransform(raw, this.filter);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void filterRow(List<KeyValue> kvs) {
    try {
      agent.doFilterRow(kvs, this.filter);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
