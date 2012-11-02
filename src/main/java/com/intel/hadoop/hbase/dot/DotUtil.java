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
package com.intel.hadoop.hbase.dot;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.intel.hadoop.hbase.dot.doc.DocSchemaMissMatchException;

/**
 * All the utilities are defined in this class.
 *
 */
public class DotUtil {
  private static final Log LOG = LogFactory.getLog(DotUtil.class);

  /**
   * Check if the current HBase table is document oriented.
   *
   * @param desc
   *          HTableDescriptor
   * @return is(true) or isn't(false) a dot table
   */
  public static boolean isDot(HTableDescriptor desc) {
    byte[] isDot = desc.getValue(DotConstants.HBASE_TABLE_IS_A_DOT_TABLE_BYTES);
    return (null == isDot) ? false : 
      (Bytes.compareTo(isDot, DotConstants.HBASE_TABLE_IS_A_DOT_TABLE_DEFAULT_BYTES) == 0);
  }

  /**
   * Automatically generate a schema from the column map<br>
   *
   * @param columns
   *          the column list: {cf:doc1.field1, cf:doc1.field2, HBASE_ROW_KEY,
   *          cf:doc1.field3}<br>
   * @param htd
   *          HTableDescriptor
   * @return cf => doc => schemaString
   * @throws IOException
   */
  public static Map<byte[], Map<byte[], JSONObject>> genSchema(
      String columns[], HTableDescriptor htd) throws IOException {
    if (!isDot(htd))
      return null;
    return genSchema(columns);
  }

  /**
   * Automatically generate a schema from the column map<br>
   *
   * @param columns
   *          the column list: {cf:doc1.field1, cf:doc1.field2, HBASE_ROW_KEY,
   *          cf:doc1.field3}<br>
   * @return cf => doc => schemaString
   * @throws IOException
   */
  public static Map<byte[], Map<byte[], JSONObject>> genSchema(String columns[])
      throws IOException {
    Map<byte[], Map<byte[], JSONObject>> schemas = new TreeMap<byte[], Map<byte[], JSONObject>>(
        Bytes.BYTES_COMPARATOR);

    for (String aColumn : columns) {

      byte[] columnName = aColumn.getBytes();
      // we are only concerned with the first one (in case this is a cf:cq)
      byte[][] cfvsdoc = org.apache.hadoop.hbase.KeyValue
          .parseColumn(columnName);

      byte[] columnfamily = cfvsdoc[0];
      byte[][] df = getDocAndField(cfvsdoc[1], 0, cfvsdoc[1].length);

      Map<byte[], JSONObject> docSchemaString = schemas.get(columnfamily);
      if (docSchemaString == null) {
        docSchemaString = new TreeMap<byte[], JSONObject>(
            Bytes.BYTES_COMPARATOR);
        schemas.put(columnfamily, docSchemaString);
      }

      try {
        JSONObject json = docSchemaString.get(df[0]);
        if (json == null) {
          json = new JSONObject();
          json.putOpt("name", Bytes.toString(df[0]));
          json.putOpt("type", "record");
          json.putOpt("fields", new JSONArray());
          schemas.get(columnfamily).put(df[0], json);
        }

        JSONObject field = new JSONObject();
        field.put("name", Bytes.toString(df[1]));
        field.put("type", "bytes");
        json.getJSONArray("fields").put(field);
      } catch (JSONException e) {
        throw new IOException("JSON format issue", e);
      }
    }
    return schemas;
  }

  /**
   * Prepare a dot table, set its required configuration items in
   * HTableDescriptor
   *
   * @param conf
   * @param htd
   *          HTableDescriptor
   * @return HTableDescriptor
   */
  public static HTableDescriptor prepareDotTable(Configuration conf,
      HTableDescriptor htd) {
    boolean isDot = conf.getBoolean(DotConstants.HBASE_TABLE_IS_A_DOT_TABLE,
        false);
    if (isDot) {
      htd.setValue(DotConstants.HBASE_TABLE_IS_A_DOT_TABLE,
          String.valueOf(true));
      htd.setValue(DotConstants.HBASE_DOT_TABLE_TYPE, conf.get(
          DotConstants.HBASE_DOT_TABLE_TYPE,
          DotConstants.HBASE_DOT_TABLE_TYPE_DEFAULT));
    }
    return htd;
  }

  /**
   * Prepare a column for dot table
   *
   * @param conf
   * @param hcd
   *          HColumnDescriptor
   * @param htd
   *          HTableDescriptor
   * @param schemas
   *          cf => doc => schemaString
   * @return HColumnDescriptor
   */
  public static HColumnDescriptor prepareDotColumn(Configuration conf,
      HColumnDescriptor hcd, HTableDescriptor htd,
      Map<byte[], Map<byte[], JSONObject>> schemas) {
    if (!isDot(htd))
      return hcd;

    byte[] cf = hcd.getName();
    Map<byte[], JSONObject> docSchemaString = schemas.get(cf);
    String[] elements = new String[docSchemaString.keySet().size()];
    int index = 0;
    Iterator iter = docSchemaString.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<byte[], JSONObject> entry = (Entry<byte[], JSONObject>) iter
          .next();
      byte[] doc = entry.getKey();
      elements[index] = Bytes.toString(doc);
      LOG.info("doc: " + elements[index]);
      hcd.setValue(DotConstants.HBASE_DOT_COLUMNFAMILY_DOC_SCHEMA_PREFIX
          + elements[index], entry.getValue().toString());
      index++;
    }

    hcd.setValue(DotConstants.HBASE_DOT_COLUMNFAMILY_DOC_ELEMENT,
        StringUtils.arrayToString(elements));
    return hcd;
  }

  /**
   * Get the doc and the field name respectively from "doc.field"
   *
   * @param fieldNameWithDoc
   *          sth. like doc.field
   * @return byte[][] {doc, field}
   */
  static public byte[][] splitDocAndField(byte[] c, int offset, int length) {
    final int index = KeyValue.getDelimiter(c, offset, length,
        DotConstants.HBASE_DOT_DOC_AND_FIELD_SEPERATOR);
    // LOG.info(Bytes.toStringBinary(c, offset, length) + " index: " + index);
    int relativeIndex = index - offset;
    if (index == -1) {
      byte[] doc = new byte[length];
      System.arraycopy(c, offset, doc, 0, doc.length);
      // LOG.info("doc(index=-1): " + Bytes.toString(doc));
      return new byte[][] { doc, null };
    } else if (relativeIndex == length - 1) {
      byte[] doc = new byte[length - 1];
      System.arraycopy(c, offset, doc, 0, doc.length);
      return new byte[][] { doc, null };
    } else if (relativeIndex == 0) {
      byte[] doc = new byte[length - 1];
      System.arraycopy(c, offset + 1, doc, 0, doc.length);
      return new byte[][] { null, doc };
    }
    final byte[][] result = new byte[2][];
    result[0] = new byte[relativeIndex];
    System.arraycopy(c, offset, result[0], 0, relativeIndex);
    final int len = length - ( relativeIndex + 1 );
    result[1] = new byte[len];
    System.arraycopy(c, index + 1, result[1], 0, len);
    return result;
  }

  /**
   * Combine the field name with it doc name into one byte array.
   *
   * @param doc
   * @param field
   * @return byte[] {"doc.field"}
   */
  static public byte[] combineDocAndField(byte[] doc, byte[] field) {
    byte[] fieldNameWithDoc = new byte[doc.length + field.length + 1];
    System.arraycopy(doc, 0, fieldNameWithDoc, 0, doc.length);
    System.arraycopy(
        new byte[] { DotConstants.HBASE_DOT_DOC_AND_FIELD_SEPERATOR }, 0,
        fieldNameWithDoc, doc.length, 1);
    System.arraycopy(field, 0, fieldNameWithDoc, doc.length + 1, field.length);

    return fieldNameWithDoc;
  }

  /**
   * Combine a column name by concating both family name and its qualifier.<br>
   * No additional bound symbol in the middle of them.
   *
   * @param columnfamilyName
   *          "cf"
   * @param docName
   *          "doc"
   * @return comes to "cfdoc"
   */
  static public String getDocNameWithColumnfamilyName(String columnfamilyName,
      String docName) {
    return columnfamilyName + docName;
  }

  /**
   * Get "columnfamily""doc" altogether.
   *
   * @param kv
   *          KeyValue
   * @return comes to new String of "cfdoc", "cf" - column family name, "doc" -
   *         document name
   */
  static public String getDocNameWithColumnfamilyName(KeyValue kv) {
    return Bytes.toString(kv.getBuffer(), kv.getFamilyOffset(),
        kv.getQualifierLength() + kv.getFamilyLength());
  }

  /**
   * Split "doc.field" to "doc" and "field" respectively.
   *
   * @param fieldNameWithDoc
   *          data buffer
   * @param offset
   *          start point
   * @param len
   *          length
   * @return byte[][]{"doc", "field"}
   * @throws IOException
   */
  static public byte[][] getDocAndField(byte[] fieldNameWithDoc, int offset,
      int len) throws IOException {
    byte[][] df = null;
    if (fieldNameWithDoc == null || len == 0) {
      LOG.error("fieldNameWithDoc is empty");
      throw new DotInvalidIOException("fieldNameWithDoc is empty");
    }

    df = splitDocAndField(fieldNameWithDoc, offset, len);

    if (df[0] == null || df[0].length == 0) {
      LOG.error("Cannot identify doc name from "
          + new String(fieldNameWithDoc, offset, len));
      throw new DocSchemaMissMatchException(
          DocSchemaMissMatchException.INVALID_DOC_OR_FIELD);
    }

    if (df[1] == null || df[1].length == 0) {
      LOG.warn("fieldName is empty");
    }
    return df;
  }

  /**
   * Return a new byte array, which contains column faimiliy name
   *
   * @param kv
   * @return
   */
  static public byte[] getKVColumnfamily(KeyValue kv) {
    return Arrays.copyOfRange(kv.getBuffer(), kv.getFamilyOffset(),
        kv.getFamilyOffset() + kv.getFamilyLength());
  }

  /**
   * Return a new byte array, which contains qualifier name
   *
   * @param kv
   * @return
   */
  static public byte[] getKVQualifier(KeyValue kv) {
    return Arrays.copyOfRange(kv.getBuffer(), kv.getQualifierOffset(),
        kv.getQualifierOffset() + kv.getQualifierLength());
  }

  /**
   * Return a new byte array, which contains the value in KV.
   *
   * @param kv
   * @return
   */
  static public byte[] getKVValue(KeyValue kv) {
    return Arrays.copyOfRange(kv.getBuffer(), kv.getValueOffset(),
        kv.getValueOffset() + kv.getValueLength());

  }

  /**
   * Get "columnfamily""doc" altogether.
   *
   * @param kv
   *          KeyValue
   * @return comes to new byte array of "cfdoc", "cf" - column family name,
   *         "doc" - document name
   */
  static public byte[] getKVColumn(KeyValue kv) {
    return Arrays.copyOfRange(kv.getBuffer(), kv.getFamilyOffset(),
        kv.getFamilyOffset() + kv.getQualifierLength() + kv.getFamilyLength());
  }

}
