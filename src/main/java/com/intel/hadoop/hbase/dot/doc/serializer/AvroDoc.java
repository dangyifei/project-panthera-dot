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

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

import com.intel.hadoop.hbase.dot.doc.DocSchemaMissMatchException;
import com.intel.hadoop.hbase.dot.doc.Document;

/**
 * Avro encoder and decoder for document
 * TODO: should catch all avro exceptions and re-throw an IOException
 */
public class AvroDoc extends Document implements Writable{
  private static final Log LOG = LogFactory.getLog(AvroDoc.class);
  private DecoderFactory decoderfactory = new DecoderFactory();
  private EncoderFactory encoderfactory = new EncoderFactory();
  private Schema schema = null;
  private Encoder encoder = null;
  private Decoder decoder = null;
  private Set<byte[]> fields = null;

  private ByteArrayOutputStream out = null;

  private GenericRecord record = null;

  @Override
  protected Schema _parseSchema(String schema) {
    return Schema.parse(schema);
  }

  protected Schema _parseSchema(Path schemafile) {
    return null;
  }

  @Override
  public void loadSchema(String schema) throws IOException {
    // TODO check if the primitive type is bytes only
    this.schema = Schema.parse(schema);
  }

  public void loadSchema(Object schema) throws IOException {
    if (schema == null)
      return;
    if (schema instanceof Schema) {
      this.schema = (Schema) schema;
    }
  }

  @Override
  public void loadSchema(Path schemafile) throws IOException {

    // TODO check if the primitive type is bytes only
    // class file

    // avpr file
  }

  @Override
  public Object getSchema() throws IOException {
    return schema;
  }

  @Override
  public String getSchemaInJSON() throws IOException {
    return schema.toString();
  }

  @Override
  public Object getEncoder() throws IOException {
    return this.encoder;
  }

  @Override
  public byte[] getValue(byte[] field) {
    Object value = this.record.get(new String(field));
    if (null != value) {
      ByteBuffer buf = (ByteBuffer) value;
      // when reusing record, ByteBuffer size could be larger than actual data
      // need to check the limit and return byte[] only contain the real data.
      return buf.limit() == buf.capacity() ? buf.array() : Bytes.getBytes(buf);
    }
    return null;
  }

  @Override
  public void setValue(byte[] field, byte[] value) throws IOException {
    try {
      this.record.put(new String(field), ByteBuffer.wrap(value));
    } catch (AvroRuntimeException are) {
      throw new DocSchemaMissMatchException(
          DocSchemaMissMatchException.INVALID_DOC_OR_FIELD);
    } catch (NullPointerException npe) {
      throw new DocSchemaMissMatchException(
          DocSchemaMissMatchException.INVALID_DOC_OR_FIELD);
    } catch (Exception e) {
      LOG.info("Unexpected exception: " + e);
      LOG.info(e.getStackTrace());
      throw new IOException(e);
    }
  }

  @Override
  public byte[] getDoc() throws IOException {
    if (this.record == null || this.schema == null || this.encoder == null
        || this.out == null) {
      return null;
    } else {
      // LOG.info("record: " + this.record.toString());
      GenericDatumWriter writer = new GenericDatumWriter(this.schema);
      writer.write(this.record, this.encoder);
      this.encoder.flush();
      return this.out.toByteArray();
    }
  }

  @Override
  public Set<byte[]> getFields() throws IOException {
    if(this.fields != null) return this.fields;
    this.fields = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
    for (Schema.Field f : this.schema.getFields()) {
      fields.add(f.name().getBytes());
    }
    return fields;
  }

  @Override
  public boolean allValueInitialized() throws IOException {
    boolean isInitialized = true;
    for (Schema.Field f : this.schema.getFields()) {
      // LOG.info("default: " + f.defaultValue());
      String name = f.name();
      if (this.record.get(name) == null) {
        this.record.put(name, ByteBuffer.wrap(this.nullStr.getBytes()));
        isInitialized = false;
        // Don't early out, so that all empty fields will be filled with nullStr
      }
    }
    return isInitialized;
  }

  @Override
  public boolean allFieldsIncluded(List<byte[]> fieldlist) throws IOException {
    boolean isAllIncluded = true;
    if (this.schema.getFields().size() == fieldlist.size()) {
      for (byte[] f : fieldlist) {
        if (null == this.schema.getField(new String(f))) {
          isAllIncluded = false;
          break;
        }
      }
    } else {
      isAllIncluded = false;
    }
    return isAllIncluded;
  }

  @Override
  public Object getDecoder() throws IOException {
    return this.decoder;
  }

  @Override
  public void setDoc(byte[] data) throws IOException {
    setDoc(data, 0, data.length);
  }

  @Override
  public void setDoc(byte[] buffer, int offset, int len) throws IOException {
    this.decoder = decoderfactory.binaryDecoder(buffer, offset, len,
        (BinaryDecoder) this.decoder);
    GenericDatumReader<GenericRecord> recordreader = new GenericDatumReader<GenericRecord>(
        this.schema);
    this.record = (GenericRecord) recordreader.read(this.record, this.decoder);
  }

  @Override
  public void setEncoder(Object encoder) throws IOException {
    this.encoder = (Encoder) encoder;
  }

  @Override
  public void setDecoder(Object decoder) throws IOException {
    this.decoder = (Decoder) decoder;
  }

  @Override
  public OutputStream getOutputStream() throws IOException {
    return this.out;
  }

  @Override
  public void setOutputStream(OutputStream out) throws IOException {
    this.out = (ByteArrayOutputStream) out;
  }

  @Override
  public void initialize(Path schemafile, byte[] data) throws IOException {
    //TODO
  }

  @Override
  public void initialize(String schema, byte[] data) throws IOException {
    initialize(schema, data, (Decoder) null);
  }

  @Override
  public void initialize(Path schema, byte[] data, Object decoder)
      throws IOException {
    //TODO
  }

  @Override
  public void initialize(String schema, OutputStream out) throws IOException {
    initialize(schema, (Encoder) null, out);
  }

  @Override
  public void initialize(Path schemafile, OutputStream out) throws IOException {
    //TODO
  }

  @Override
  public void initialize(Path schemafile, Object encoder, OutputStream out)
      throws IOException {
    //TODO
  }

  public void initialize(String schema, byte[] data, Object decoder)
      throws IOException {
    // TODO Auto-generated constructor stub
    loadSchema(schema);
    setDecoder(decoder);
    setDoc(data);
  }

  public void initialize(String schema, Object encoder, OutputStream out)
      throws IOException {
    loadSchema(schema);
    record = new GenericData.Record(this.schema);
    setEncoder(encoder);
    setOutputStream(out);
    if (this.out == null)
      this.out = new ByteArrayOutputStream();
    if (null == encoder) {
      // this.encoder = encoderfactory.jsonEncoder(this.schema, this.out);
      this.encoder = encoderfactory.binaryEncoder(out, null);
    }
  }

  @Override
  public void initialize(Object schema, byte[] data, Object decoder)
      throws IOException {
    loadSchema(schema);
    setDecoder(decoder);
    setDoc(data);
  }

  @Override
  public void initialize(Object schema, Object encoder, OutputStream out)
      throws IOException {

    if (schema instanceof Schema) {
      this.schema = (Schema) schema;
    }
    record = new GenericData.Record(this.schema);

    setEncoder(encoder);
    setOutputStream(out);
    if (this.out == null)
      this.out = new ByteArrayOutputStream();
    if (null == encoder) {
      // this.encoder = encoderfactory.jsonEncoder(this.schema, this.out);
      this.encoder = encoderfactory.binaryEncoder(out, null);
    }
  }

  /**
   * DOC|AVRO|DATA_LEN|DATA_BYTE_ARRAY
   *
   *
   */
  @Override
  public void write(DataOutput out) throws IOException {
    //TODO
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    //TODO
  }

}
