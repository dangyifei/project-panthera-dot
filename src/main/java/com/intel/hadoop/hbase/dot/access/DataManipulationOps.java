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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.util.StringUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.DocStoreScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.intel.hadoop.hbase.dot.DotConstants;
import com.intel.hadoop.hbase.dot.DotException;
import com.intel.hadoop.hbase.dot.DotInvalidIOException;
import com.intel.hadoop.hbase.dot.DotUtil;
import com.intel.hadoop.hbase.dot.doc.DocSchemaMissMatchException;
import com.intel.hadoop.hbase.dot.doc.Document;

/**
 * All the data manipulation relate operations are defined in the class, like: <br>
 * put records; <br>
 * delete records; <br>
 * get records; <br>
 * scan records; <br>
 * ...
 *
 */
public class DataManipulationOps extends BaseRegionObserver {
  private static final Log LOG = LogFactory.getLog(DataManipulationOps.class);

  // docName_with_colfamily => (SchemaObject, serClz)
  private ConcurrentMap<String, Pair<Object, String>> schemaMap = new ConcurrentHashMap<String, Pair<Object, String>>();

  private String nullFormat = null;
  private byte[] nullFormatBytes = null;

  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
    nullFormat = e.getConfiguration().get(DotConstants.SERIALIZATION_NUL_FORMAT,
      DotConstants.DEFAULT_SERIALIZATION_NUL_FORMAT);
    nullFormatBytes = nullFormat.getBytes();
  }

  /**
   * Generate a schema and its serializer and cache them into the
   * {@link #schemaMap }
   *
   * @param hcd
   * @param docName
   *          like "doc1"
   * @return <schemaObject, serializer>
   */
  private Object loadSchema(HColumnDescriptor hcd, String docName) {
    String columnfamilyName = hcd.getNameAsString();
    String docNameWithColumnfamilyName = DotUtil
        .getDocNameWithColumnfamilyName(columnfamilyName, docName);
    String serializer = hcd
        .getValue(DotConstants.HBASE_DOT_COLUMNFAMILY_DOC_SERIALIZER_CLASS);

    // always put the schema object into the schemaMap
    String schema = hcd
        .getValue(DotConstants.HBASE_DOT_COLUMNFAMILY_DOC_SCHEMA_PREFIX
            + docName);
    schemaMap.put(docNameWithColumnfamilyName,
        Pair.newPair(Document.parseSchema(serializer, schema), serializer));

    return schemaMap.get(docNameWithColumnfamilyName);
  }

  /**
   * Get the schema and its serializer from the {@link #schemaMap }
   *
   * @param hcd
   * @param docName
   *          like "doc1"
   * @return <schemaObject, serializer>
   */
  private Pair<Object, String> getSchema(HColumnDescriptor hcd, String docName) {
    String columnfamilyName = hcd.getNameAsString();
    String docNameWithColumnfamilyName = DotUtil
        .getDocNameWithColumnfamilyName(columnfamilyName, docName);
    Pair<Object, String> schemaObj = schemaMap.get(docNameWithColumnfamilyName);
    assert null != schemaObj : "Cannot find schemaObj for "
        + docNameWithColumnfamilyName;
    return schemaObj;
  }

  /**
   * Get the schema and its serializer from the {@link #schemaMap }
   *
   * @param kv
   * @return <schemaObject, serializer>
   */
  private Pair<Object, String> getSchema(KeyValue kv) {
    String docNameWithColumnfamilyName = DotUtil.getDocNameWithColumnfamilyName(kv);
    Pair<Object, String> schemaObj = schemaMap.get(docNameWithColumnfamilyName);
    assert null != schemaObj : "Cannot find schemaObj for "
        + docNameWithColumnfamilyName;
    return schemaObj;
  }

  @Override
  public void postOpen(ObserverContext<RegionCoprocessorEnvironment> e) {
    HTableDescriptor desc = e.getEnvironment().getRegion().getTableDesc();
    if (!DotUtil.isDot(desc)) {
      return;
    }
    for (HColumnDescriptor hcd : desc.getFamilies()) {
      String[] docs = StringUtils.getStrings(hcd
          .getValue(DotConstants.HBASE_DOT_COLUMNFAMILY_DOC_ELEMENT));
      for (String doc : docs) {
        // to load schema for each doc in advance
        loadSchema(hcd, doc);
      }
    }
  }

  /**
   * Expand document map according to the input kv
   *
   * @param fields
   *          doc => field => value
   * @param kv
   * @throws IOException
   */
  private void expandDocMap(Map<byte[], Map<byte[], byte[]>> fields, KeyValue kv)
      throws IOException {
    byte[] value = DotUtil.getKVValue(kv);

    byte[][] df = DotUtil.getDocAndField(kv.getBuffer(),
        kv.getQualifierOffset(), kv.getQualifierLength());

    if (df[1] == null || df[1].length == 0) {
      throw new DocSchemaMissMatchException(
          DocSchemaMissMatchException.INVALID_DOC_OR_FIELD);
    }
    // for each column-family has one map
    // < doc -> Map( field, value) >
    Map<byte[], byte[]> fieldValueMap = fields.get(df[0]);
    if (fieldValueMap == null) {
      fieldValueMap = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
      fields.put(df[0], fieldValueMap);
    }
    fieldValueMap.put(df[1], value);
  }

  /**
   * Expand document list according to the input kv
   *
   * @param fields
   *          doc => fieldList
   * @param kv
   * @throws IOException
   */
  private void expandDocList(Map<byte[], List<byte[]>> fields, KeyValue kv)
      throws IOException {
    // LOG.info("kv: " + kv);
    byte[][] df = DotUtil.getDocAndField(kv.getBuffer(),
        kv.getQualifierOffset(), kv.getQualifierLength());
    List<byte[]> fieldList = fields.get(df[0]);
    if (fieldList == null) {
      fieldList = new ArrayList<byte[]>();
      fields.put(df[0], fieldList);
    }
    if (df[1] != null && df[1].length != 0) {
      fieldList.add(df[1]);
    }
  }

  /**
   * Expand document list according to known qualifier
   *
   * @param fields
   *          doc => fieldList
   * @param qualifier
   *          "doc1.field1"
   * @throws IOException
   */
  private void expandDocList(Map<byte[], Set<byte[]>> fields, byte[] qualifier)
      throws IOException {
    byte[][] df = DotUtil.getDocAndField(qualifier, 0, qualifier.length);
    Set<byte[]> fieldList = fields.get(df[0]);
    if (fieldList == null) {
      fieldList = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
      fields.put(df[0], fieldList);
    }
    if (df[1] != null && !df[1].equals("")) {
      fieldList.add(df[1]);
    }
  }

  /**
   * Check if all fields for certain document are included
   *
   * @param docName
   *          'doc1'
   * @param fields
   *          fieldsList[] {'field1', 'field2', 'field3'}
   * @param schemaPair
   *          <schemaObject, serializer>
   * @return
   * @throws IOException
   */
  private boolean isAllFieldIncluded(String docName, List<byte[]> fields,
      Pair<Object, String> schemaPair) throws IOException {
    Document docObject = Document.createDoc(schemaPair.getSecond());
    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    // TODO choose the schema format (either file or string )
    docObject.initialize(schemaPair.getFirst(), (Object) null, bao);
    if (!fields.isEmpty() && !docObject.allFieldsIncluded(fields)) {
      LOG.error("Only partial fields are included in: " + docName);
      throw new DocSchemaMissMatchException(
          DocSchemaMissMatchException.DOC_CONTENT_IS_NOT_COMPLETE);
    }
    return true;
  }

  /**
   * Get the entire document value in byte array
   * TODO this function need to be put into the DotUtil
   * @param docName
   *          'doc1'
   * @param fields
   *          fields => values
   * @param schemaPair
   *          <schemaObject, serializer>
   * @return
   * @throws IOException
   */
  public byte[] getDocValue(String docName, Map<byte[], byte[]> fields,
      Pair<Object, String> schemaPair, Configuration conf) throws IOException {
    Document docObject = Document.createDoc(schemaPair.getSecond());
    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    docObject.setNullFormat(this.nullFormat);
    // TODO choose the schema format (either file or string )
    docObject.initialize(schemaPair.getFirst(), (Object) null, bao);

    for (Map.Entry<byte[], byte[]> entry : fields.entrySet()) {
      // put value for each field into doc object
      docObject.setValue(entry.getKey(), entry.getValue());
    }

    if (!docObject.allValueInitialized()) {
      LOG.warn("Not all fields in document: " + docName + " are put ");
    }

    // return doc's value
    return docObject.getDoc();
  }

  /**
   * Check the timestamp.<br>
   * Currently, it only supports LATEST_TIMESTAMP.
   *
   * @param kv
   * @throws IOException
   */
  private void checkTimeStamp(KeyValue kv) throws IOException {
    if (HConstants.LATEST_TIMESTAMP != Bytes.toLong(kv.getBuffer(),
        kv.getTimestampOffset())) {
      throw new DotInvalidIOException(DotException.ILLEGAL_DOT_OPRATION
          + ": specify non-latest TimeStamp");
    }
  }

  /**
   * Update the column name ( "cf:doc.field" ) with "cf:doc"<br>
   * Meanwhile, it makes sure that all fields under the same document are
   * initialized altogether
   */
  @Override
  public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e,
      final Put put, final WALEdit edit, final boolean writeToWAL)
      throws IOException {
    HTableDescriptor desc = e.getEnvironment().getRegion().getTableDesc();
    if (!DotUtil.isDot(desc)) {
      return;
    }
    Map<byte[], List<KeyValue>> mapper = put.getFamilyMap();
    Map<byte[], Map<byte[], byte[]>> newContentMap = new HashMap();

    for (Map.Entry<byte[], List<KeyValue>> mapperEntry : mapper.entrySet()) {
      byte[] colfamily = mapperEntry.getKey();
      // each column family
      HColumnDescriptor coldef = desc.getFamily(colfamily);
      List<KeyValue> kvs = (List<KeyValue>) mapperEntry.getValue();
      // docName, < field, value>
      Map<byte[], Map<byte[], byte[]>> fields = new TreeMap<byte[], Map<byte[], byte[]>>(
          Bytes.BYTES_COMPARATOR);

      newContentMap.put(colfamily, new TreeMap<byte[], byte[]>(
          Bytes.BYTES_COMPARATOR));

      try {
        // LOG.debug("total column number: " + kvs.size());
        for (KeyValue kv : kvs) {
          checkTimeStamp(kv);
          expandDocMap(fields, kv);
        }
      } catch (DotInvalidIOException ioe) {
        LOG.warn("Found Empty qualifier");
        continue;
      }

      Map<byte[], byte[]> valueMap = newContentMap.get(colfamily);
      newContentMap.put(colfamily, valueMap);
      for (Map.Entry<byte[], Map<byte[], byte[]>> entry : fields.entrySet()) {
        String docName = Bytes.toString(entry.getKey());
        // LOG.debug("docname: " + docName);
        Pair<Object, String> schemaPair = this.getSchema(coldef, docName);
        List<byte[]> fieldList = new ArrayList();
        fieldList.addAll(entry.getValue().keySet());
        byte[] docValue = getDocValue(docName, entry.getValue(), schemaPair, e
            .getEnvironment().getConfiguration());

        valueMap.put(docName.getBytes(), docValue);
      }
    }
    mapper.clear();

    // TODO add version information
    // update put data
    for (Map.Entry<byte[], Map<byte[], byte[]>> entry : newContentMap.entrySet()) {
      for (Map.Entry<byte[], byte[]> ent : entry.getValue().entrySet()) {
        put.add(entry.getKey(), ent.getKey(), ent.getValue());
      }
    }
  }

  /**
   * Update the column name ( "cf:doc.field" ) with "cf:doc"<br>
   * Meanwhile, it makes sure that all fields under the same document are
   * deleted altogether
   */
  @Override
  public void preDelete(final ObserverContext<RegionCoprocessorEnvironment> e,
      final Delete delete, final WALEdit edit, final boolean writeToWAL)
      throws IOException {
    HTableDescriptor desc = e.getEnvironment().getRegion().getTableDesc();
    if (!DotUtil.isDot(desc)) {
      return;
    }

    Map<byte[], List<KeyValue>> mapper = delete.getFamilyMap();
    Map<byte[], Map<byte[], List<byte[]>>> newDeleteList = new TreeMap(
        Bytes.BYTES_COMPARATOR);

    Iterator it = mapper.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<byte[], List<KeyValue>> mapperEntry = (Entry<byte[], List<KeyValue>>) it
          .next();
      byte[] colfamily = mapperEntry.getKey();
      // each column family
      HColumnDescriptor coldef = desc.getFamily(colfamily);
      Map<byte[], List<byte[]>> fields = new TreeMap<byte[], List<byte[]>>(
          Bytes.BYTES_COMPARATOR);
      newDeleteList.put(colfamily, fields);

      try {
        for (KeyValue kv : mapperEntry.getValue()) {
          checkTimeStamp(kv);
          expandDocList(fields, kv);
        }
      } catch (DotInvalidIOException ioe) {
        LOG.warn("Found Empty qualifier");
        continue;
      }

      for (Map.Entry<byte[], List<byte[]>> entry : fields.entrySet()) {
        String docName = Bytes.toString(entry.getKey());
        LOG.info("docname: " + docName);
        Pair<Object, String> schemaPair = this.getSchema(coldef, docName);
        // If the user tries to delete partial fields, some exception will be
        // expected here.
        isAllFieldIncluded(docName, entry.getValue(), schemaPair);
      }
      // To remove the old element
      it.remove();
    }

    // TODO add version information
    // update delete data
    for (Map.Entry<byte[], Map<byte[], List<byte[]>>> entry : newDeleteList.entrySet()) {
      for (byte[] q : entry.getValue().keySet()) {
        delete.deleteColumns(entry.getKey(), q);
      }
    }
  }

  /**
   * Replace Standard StoreScanner with DocStoreScanner
   */
  @Override
  public KeyValueScanner preStoreScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> e,
      final Store store, final Scan scan, final NavigableSet<byte[]> targetCols,
      final KeyValueScanner s) throws IOException {

    HTableDescriptor desc = e.getEnvironment().getRegion().getTableDesc();
    if (!DotUtil.isDot(desc)) {
      return null;
    }

    DocStoreScanner scanner = new DocStoreScanner(store, scan, targetCols);
    scanner.init(this.nullFormatBytes);
    return scanner;
  }

}
