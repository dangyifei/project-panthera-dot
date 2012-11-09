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
package com.intel.hadoop.hbase.dot.doc.serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.Path;

import com.intel.hadoop.hbase.dot.doc.Document;

/**
 * TODO PB encoder and decoder for document.
 *
 *
 */
public class PBDoc extends Document {

  @Override
  public void loadSchema(String schema) throws IOException {

  }

  @Override
  public void loadSchema(Path schemafile) throws IOException {

  }

  @Override
  public void loadSchema(Object schema) throws IOException {

  }

  @Override
  public Object getSchema() throws IOException {

    return null;
  }

  @Override
  public String getSchemaInJSON() throws IOException {

    return null;
  }

  @Override
  public Object getEncoder() throws IOException {

    return null;
  }

  @Override
  public void setEncoder(Object encoder) throws IOException {

  }

  @Override
  public Object getDecoder() throws IOException {

    return null;
  }

  @Override
  public void setDecoder(Object decoder) throws IOException {

  }

  @Override
  public void setOutputStream(OutputStream out) throws IOException {

  }

  @Override
  public OutputStream getOutputStream() throws IOException {

    return null;
  }

  @Override
  public byte[] getValue(byte[] field) throws IOException {

    return null;
  }

  @Override
  public void setValue(byte[] field, byte[] value) throws IOException {

  }

  @Override
  public byte[] getDoc() throws IOException {

    return null;
  }

  @Override
  public void setDoc(byte[] data) throws IOException {

  }

  @Override
  public List<DocSchemaField> getFields() throws IOException {
    return null;
  }

  @Override
  public boolean allValueInitialized() throws IOException {

    return false;
  }

  @Override
  public void initialize(Path schemafile, byte[] data) throws IOException {

  }

  @Override
  public void initialize(String schema, byte[] data) throws IOException {

  }

  @Override
  public void initialize(String schema, byte[] data, Object decoder)
      throws IOException {

  }

  @Override
  public void initialize(Path schema, byte[] data, Object decoder)
      throws IOException {

  }

  @Override
  public void initialize(String schema, OutputStream out) throws IOException {

  }

  @Override
  public void initialize(String schema, Object encoder, OutputStream out)
      throws IOException {

  }

  @Override
  public void initialize(Path schemafile, OutputStream out) throws IOException {

  }

  @Override
  public void initialize(Path schemafile, Object encoder, OutputStream out)
      throws IOException {

  }

  @Override
  public boolean allFieldsIncluded(List<byte[]> fieldlist) throws IOException {

    return false;
  }

  @Override
  public void write(DataOutput out) throws IOException {

  }

  @Override
  public void readFields(DataInput in) throws IOException {

  }

  @Override
  public void initialize(Object schema, byte[] data, Object decoder)
      throws IOException {

  }

  @Override
  public void initialize(Object schema, Object encoder, OutputStream out)
      throws IOException {

  }

  @Override
  protected Object _parseSchema(String schema) {

    return null;
  }

  @Override
  protected Object _parseSchema(Path schemafile) {

    return null;
  }

  @Override
  public void setDoc(byte[] buffer, int offset, int len) throws IOException {

  }

}
