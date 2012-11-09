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
package com.intel.hadoop.hbase.dot.doc;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.Writable;

import org.apache.hadoop.fs.Path;

import com.intel.hadoop.hbase.dot.DotConstants;

/**
 * The document APIs for all kinds of serializers, like Avro, PB and so on.
 *
 */

public abstract class Document implements Writable {
  protected String nullStr = null;
  
  public class DocSchemaField {
    // field in raw byte[]
    public byte[] field = null;
    // doc+field in raw byte[]
    public byte[] docWithField = null;
    // field in String format
    public String name = null;
  }
  
  /**
   * The only way to create a serializer instance
   */
  public static Document createDoc(String serializerClassName) {
    Document instance = null;
    try {
      instance = (Document) Class.forName(serializerClassName).newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Failed to create a document instance", e);
    }
    return instance;
  }

  /**
   * Parsing schema string into an object
   *
   * @param serializerClassName
   * @param schema
   * @return schema object
   */
  static public Object parseSchema(String serializerClassName, String schema) {
    Document instance = Document.createDoc(serializerClassName);
    return instance._parseSchema(schema);
  }

  public void setNullFormat(String nul) {
    nullStr = nul;
  }
  /**
   * Load schema from a string content
   *
   * @param schema
   * @throws IOException
   */
  abstract public void loadSchema(String schema) throws IOException;

  /**
   * Load schema from a file content
   *
   * @param schemafile
   *          Path
   * @throws IOException
   */
  abstract public void loadSchema(Path schemafile) throws IOException;

  /**
   * Load a schema from a existing schema object
   *
   * @param schema
   *          schema Object
   * @throws IOException
   */
  abstract public void loadSchema(Object schema) throws IOException;

  /**
   * Get the current schema object
   *
   * @return schema object
   * @throws IOException
   */
  abstract public Object getSchema() throws IOException;

  /**
   * Get the current schema in JSON format
   *
   * @return string in json format
   * @throws IOException
   */
  abstract public String getSchemaInJSON() throws IOException;

  /**
   * Get the current encoder, which can be re-used
   *
   * @return
   * @throws IOException
   */
  abstract public Object getEncoder() throws IOException;

  /**
   * Set the current encoder, which can be re-used
   *
   * @param encoder
   * @throws IOException
   */
  abstract public void setEncoder(Object encoder) throws IOException;

  /**
   * Get the current decoder, which can be re-used
   *
   * @return
   * @throws IOException
   */
  abstract public Object getDecoder() throws IOException;

  /**
   * Set the current decoder, which can be re-used
   *
   * @param decoder
   * @throws IOException
   */
  abstract public void setDecoder(Object decoder) throws IOException;

  /**
   * Set output stream after decoding
   *
   * @param out
   * @throws IOException
   */
  abstract public void setOutputStream(OutputStream out) throws IOException;

  /**
   * Get output stream after decoding
   *
   * @return
   * @throws IOException
   */
  abstract public OutputStream getOutputStream() throws IOException;

  /**
   * Get the corresponding value for certain field
   *
   * @param field
   * @return
   * @throws IOException
   */
  abstract public byte[] getValue(byte[] field) throws IOException;

  
  /**
   * Get all fields' value
   * 
   * @return
   * @throws IOException
   */
  public List<byte[]> getValues() throws IOException{
    return null;
  }

  /**
   * Set the corresponding value for certain field
   *
   * @param field
   * @param value
   * @throws IOException
   */
  abstract public void setValue(byte[] field, byte[] value) throws IOException;

  /**
   * Get the document content in byte array
   *
   * @return
   * @throws IOException
   */
  abstract public byte[] getDoc() throws IOException;

  /**
   * Set the document content with byte array
   *
   * @param data
   * @throws IOException
   */
  abstract public void setDoc(byte[] data) throws IOException;

  /**
   * Set the document content with byte array.
   *
   * @param buffer
   * @param offset
   * @param len
   * @throws IOException
   */
  abstract public void setDoc(byte[] buffer, int offset, int len)
      throws IOException;

  /**
   * Get the whole field list in a document
   *
   * @return
   * @throws IOException
   */
  abstract public List<DocSchemaField> getFields() throws IOException;

  /**
   * Check if all fields have been initialized with correct value.
   *
   * @return
   * @throws IOException
   */
  abstract public boolean allValueInitialized() throws IOException;

  /**
   * Check if the field list is same as the document's
   *
   * @param fieldlist
   * @return
   * @throws IOException
   */
  abstract public boolean allFieldsIncluded(List<byte[]> fieldlist)
      throws IOException;

  /**
   * Parse schema from string format. This function won't be called by the user.
   * It can only used inside of the serializer.
   *
   * @param schema
   * @return
   */
  abstract protected Object _parseSchema(String schema);

  /**
   * Parse schema from file format. This function won't be called by the user.
   * It can only used inside of the serializer.
   *
   *
   * @param schemafile
   * @return
   */
  abstract protected Object _parseSchema(Path schemafile);

  /********************************************
   * data to structured information
   ********************************************/

  /**
   * Initialize a document object with its schemafile and encoded data
   *
   * @param schemafile
   * @param data
   * @throws IOException
   */
  abstract public void initialize(Path schemafile, byte[] data)
      throws IOException;

  /**
   * Initialize a document object with its schema string and encoded data
   *
   * @param schemafile
   * @param data
   * @throws IOException
   */
  abstract public void initialize(String schema, byte[] data)
      throws IOException;

  /**
   * Initialize a document object with its schema string , encoded data and
   * specified decoder.
   *
   * @param schemafile
   * @param data
   * @param decoder
   *          if it is null, the serializer will create a new instance itself;
   *          otherwise, the document will reuse that decoder.
   * @throws IOException
   */
  abstract public void initialize(String schema, byte[] data, Object decoder)
      throws IOException;

  /**
   * Initialize a document object with its schemafile , encoded data and
   * specified decoder.
   *
   * @param schemafile
   * @param data
   * @param decoder
   *          if it is null, the serializer will create a new instance itself;
   *          otherwise, the document will reuse that decoder.
   * @throws IOException
   */
  abstract public void initialize(Path schema, byte[] data, Object decoder)
      throws IOException;

  /**
   * Initialize a document object with its schema object , encoded data and
   * specified decoder.
   *
   * @param schemafile
   * @param data
   * @param decoder
   *          if it is null, the serializer will create a new instance itself;
   *          otherwise, the document will reuse that decoder.
   * @throws IOException
   */
  abstract public void initialize(Object schema, byte[] data, Object decoder)
      throws IOException;

  /********************************************
   * structured information to data
   ********************************************/
  /**
   * Initialize a document with its schema string, and the outputStream(which
   * stores the decoded data)
   *
   * @param schema
   * @param out
   * @throws IOException
   */
  abstract public void initialize(String schema, OutputStream out)
      throws IOException;

  /**
   * Initialize a document with its schema file, and the outputStream(which
   * stores the decoded data)
   *
   * @param schema
   * @param out
   * @throws IOException
   */
  abstract public void initialize(Path schemafile, OutputStream out)
      throws IOException;

  /**
   * Initialize a document with its schema string, the outputStream(which stores
   * the decoded data) , and encoder object.
   *
   * @param schema
   * @param encoder
   *          if it is null, the serializer will create a new instance itself;
   *          otherwise, the document will reuse that decoder.
   * @param out
   * @throws IOException
   */
  abstract public void initialize(String schema, Object encoder,
      OutputStream out) throws IOException;

  /**
   * Initialize a document with its schema file, the outputStream(which stores
   * the decoded data) , and encoder object.
   *
   * @param schema
   * @param encoder
   *          if it is null, the serializer will create a new instance itself;
   *          otherwise, the document will reuse that decoder.
   * @param out
   * @throws IOException
   */
  abstract public void initialize(Path schemafile, Object encoder,
      OutputStream out) throws IOException;

  /**
   * Initialize a document with its schema object, the outputStream(which stores
   * the decoded data) , and encoder object.
   *
   * @param schema
   * @param encoder
   *          if it is null, the serializer will create a new instance itself;
   *          otherwise, the document will reuse that decoder.
   * @param out
   * @throws IOException
   */
  abstract public void initialize(Object schema, Object encoder,
      OutputStream out) throws IOException;

}
