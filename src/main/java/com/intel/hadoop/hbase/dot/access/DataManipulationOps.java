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
import java.lang.reflect.InvocationTargetException;
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
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.filter.FilterWrapper;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
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

  // scanner => Scan
  private ConcurrentMap<InternalScanner, Scan> scanMap = new ConcurrentHashMap<InternalScanner, Scan>();

  // docName_with_colfamily => (SchemaObject, serClz)
  private ConcurrentMap<String, Pair<Object, String>> schemaMap = new ConcurrentHashMap<String, Pair<Object, String>>();

  private ConcurrentMap<Object, Map<byte[], Map<byte[], Set<byte[]>>>> requiredFields = new ConcurrentHashMap();

  private ConcurrentMap<Filter, Map<byte[], Document>> docObjectCache = new ConcurrentHashMap<Filter, Map<byte[], Document>>();

  // filter => rowkey => KVs
  private ConcurrentMap<Filter, Map<byte[], Set<KeyValue>>> filterKVs = new ConcurrentHashMap<Filter, Map<byte[], Set<KeyValue>>>();

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

  /**
   * Get the client side filter. On the server-side, scanner only uses
   * {@link org.apache.hadoop.hbase.filter.FilterWrapper FilterWrapper} instead.
   *
   * @param flt
   * @return
   */
  private Filter getFilterInstance(Filter flt) {
    Filter result = flt instanceof FilterWrapper
        ? ( (FilterWrapper) flt ).getFilterInstance() : flt;

    return result instanceof DotFilterWrapper
        ? ((DotFilterWrapper) result).getFilterInstance() : result;
  }

  /**
   * replace filter with a DotFilterWrapper to handle DOT.
   * @param flt
   * @return the wrapper.
   */
  private Filter wrapFilterInstance(Filter flt) {
    if (flt == null)
      return null;

    boolean haveWrapper = (flt instanceof FilterWrapper);
    Filter old = null;
    Filter result = null;
    if (haveWrapper) {
      old = ((FilterWrapper)flt).getFilterInstance();
    } else {
      old = flt;
    }

    result = new DotFilterWrapper(old, this);
    return haveWrapper ? new FilterWrapper(result) : result;
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
   * Do the preparation for either {@link org.apache.hadoop.hbase.client.Scan
   * Scan} or {@link org.apache.hadoop.hbase.client.Get Get}<br>
   * Update the column name ( "cf:doc.field" ) with "cf:doc"<br>
   *
   * 1. Expand the required column(if specified) of "cf:docX.fieldY" into a map
   * of "cf => doc => fieldList" <br>
   * 2. Store required column into {@link #requiredFields}<br>
   * 3. Put the filter(client side) instance into {@link #filterKVs}<br>
   *
   *
   * @param operator
   *          either {@link org.apache.hadoop.hbase.client.Scan Scan} or
   *          {@link org.apache.hadoop.hbase.client.Get Get}
   *
   * @throws IOException
   * @throws IllegalAccessException
   * @throws IllegalArgumentException
   * @throws InvocationTargetException
   * @throws NoSuchMethodException
   * @throws SecurityException
   */
  private void prepareGetOrScan(Object operator) throws IOException,
      IllegalAccessException, IllegalArgumentException,
      InvocationTargetException, NoSuchMethodException, SecurityException {
    Class clz = operator.getClass();

    Map<byte[], NavigableSet<byte[]>> mapper = (Map<byte[], NavigableSet<byte[]>>) clz
        .getMethod("getFamilyMap").invoke(operator);
    // columnfamily -> < doc, list of fields >
    Map<byte[], Map<byte[], Set<byte[]>>> fieldsMap = new TreeMap(
        Bytes.BYTES_COMPARATOR);

    boolean hasFields = false;
    Iterator iter = mapper.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<byte[], NavigableSet<byte[]>> entry = (Entry<byte[], NavigableSet<byte[]>>) iter
          .next();
      byte[] colfamily = entry.getKey();
      // each column family
      NavigableSet<byte[]> qualifiers = (NavigableSet<byte[]>) entry.getValue();
      if (qualifiers == null || qualifiers.size() == 0) {
        continue;
      }
      // < doc, list of fields >
      Map<byte[], Set<byte[]>> docmap = new TreeMap<byte[], Set<byte[]>>(
          Bytes.BYTES_COMPARATOR);
      fieldsMap.put(colfamily, docmap);
      try {
        // LOG.debug("total column number: " + kvs.size());
        for (byte[] qualifier : qualifiers) {
          // for each "doc.fields" or "doc"
          expandDocList(docmap, qualifier);
          hasFields = true;
        }
      } catch (DotInvalidIOException ioe) {
        LOG.warn("Found Empty qualifier");
        continue;
      }

      // To remove the old element
      iter.remove();
    }

    // TODO add version information
    for (Map.Entry<byte[], Map<byte[], Set<byte[]>>> entry : fieldsMap.entrySet()) {
      byte[] cfm = entry.getKey();
      for (Map.Entry<byte[], Set<byte[]>> etr : entry.getValue().entrySet()) {
        byte[] q = etr.getKey();
        // LOG.debug("updated docname: " + q);
        clz.getMethod("addColumn", new Class[] { byte[].class, byte[].class })
            .invoke(operator, cfm, q);
      }
    }

    if (hasFields) {
      // record the field array into the get's attribute.
      requiredFields.put(operator, fieldsMap);
    }

    Filter oldFilter = (Filter) clz.getMethod("getFilter").invoke(operator);
    Filter realFilter = getFilterInstance(oldFilter);
    Filter newFilter = wrapFilterInstance(oldFilter);

    if (newFilter != null) {
      clz.getMethod("setFilter", Filter.class).invoke(operator, newFilter);
      Map<byte[], Set<KeyValue>> rows = new TreeMap<byte[], Set<KeyValue>>(Bytes.BYTES_COMPARATOR);
      this.filterKVs.put(realFilter, rows);
      this.docObjectCache.put(realFilter, new TreeMap<byte[], Document>(
          Bytes.BYTES_COMPARATOR));
    }
  }

  @Override
  public void preGet(final ObserverContext<RegionCoprocessorEnvironment> e,
      final Get get, final List<KeyValue> results) throws IOException {
    HTableDescriptor desc = e.getEnvironment().getRegion().getTableDesc();
    if (!DotUtil.isDot(desc)) {
      return;
    }

    try {
      prepareGetOrScan(get);
    } catch (IllegalAccessException e1) {
      throw new IOException("Failures with operator", e1);
    } catch (IllegalArgumentException e1) {
      throw new IOException("Failures with operator", e1);
    } catch (InvocationTargetException e1) {
      throw new IOException("Failures with operator", e1);
    } catch (NoSuchMethodException e1) {
      throw new IOException("Failures with operator", e1);
    } catch (SecurityException e1) {
      throw new IOException("Failures with operator", e1);
    }

  }

  abstract private class KVListBase {
    abstract void addKVItem(KeyValue kv);
    void flush(){};
  }

  /**
   * Decode all document element, and return back "doc.field" and its value one
   * by one.
   */
  @Override
  public void postGet(final ObserverContext<RegionCoprocessorEnvironment> e,
      final Get get, final List<KeyValue> results) throws IOException {
    HTableDescriptor desc = e.getEnvironment().getRegion().getTableDesc();

    if (!DotUtil.isDot(desc))
      return;

    final List<KeyValue> newResults = new ArrayList<KeyValue>();

    class KVList extends KVListBase {
      public KVList() {}

      @Override
      void addKVItem(KeyValue kv) {
        newResults.add(kv);
      }
    }


    KVList kvlist = new KVList();

    Filter flt = getFilterInstance(get.getFilter());
    try {
      Map<byte[], Map<byte[], Set<byte[]>>> docAndFields = requiredFields
          .get(get);
      Configuration conf = e.getEnvironment().getConfiguration();
      byte[] nullValue = this.nullFormatBytes;
      // LOG.debug("kv list size: " + results.size());
      Iterator<KeyValue> iterator = results.iterator();
      while (iterator.hasNext()) {
        KeyValue kv = (KeyValue) iterator.next();
        postGetOrScan(kv, docAndFields,null,kvlist,
            desc, flt, nullValue);
      }

      results.clear();
      results.addAll(newResults);
    } finally {
      requiredFields.remove(get);
      if (flt != null) {
        filterKVs.remove(flt);
        docObjectCache.remove(flt);
      }
    }
  }

  /**
   * Decode all document element and return back 'doc.field' and its value to Get or Scanner
   * @param kv
   * @param docAndFields
   * @param documents
   * @param kvlist
   * @param desc
   * @param flt
   * @param nullValue
   * @throws IOException
   */
  private void postGetOrScan(KeyValue kv,
      Map<byte[], Map<byte[], Set<byte[]>>> docAndFields,
      Map<byte[], Document> documents, KVListBase kvlist,
      HTableDescriptor desc, Filter flt, byte[] nullValue) throws IOException {

    byte[] columnfamily = DotUtil.getKVColumnfamily(kv);
    byte[] qualifier = DotUtil.getKVQualifier(kv);
    Set<KeyValue> filteredSets = null;
    
    byte[] docRow = null;
    if (flt != null) {
      docRow = kv.getKey();
      filteredSets = this.filterKVs.get(flt).get(docRow);
    }

    if (filteredSets != null) {
      // has filteredSet
      // TODO to refine this block
      Map<byte[], Set<byte[]>> doc2fields = null;
      boolean noRequiredFields = true;
      if (docAndFields != null) {
        // has required filed list
        doc2fields = docAndFields.get(columnfamily);
        if (doc2fields != null && !doc2fields.isEmpty()) {
          Set<byte[]> fld = doc2fields.get(qualifier);
          if ( fld != null && !fld.isEmpty()) {
            noRequiredFields = false;
            for (KeyValue newKV : filteredSets) {
              byte[][] splits = DotUtil.getDocAndField(newKV.getBuffer(),
                  newKV.getQualifierOffset(), newKV.getQualifierLength());
              if (fld.contains(splits[1])) {
                newKV.setMemstoreTS(kv.getMemstoreTS());
                kvlist.addKVItem(newKV);
              }
            }
          }
 
        }
      }
      if (noRequiredFields) {
        for (KeyValue newKV : filteredSets) {
          // LOG.info("filter list: " + newKV);
          newKV.setMemstoreTS(kv.getMemstoreTS());
          kvlist.addKVItem(newKV);
        }
      }

      this.filterKVs.get(flt).remove(docRow);
    } else {
      long timestamp = kv.getTimestamp();
      Type type = Type.codeToType(kv.getType());

      byte[] column = DotUtil.getKVColumn(kv);

      Document docObject = null;
      if(documents != null) docObject = documents.get(column);
      Document docObj = decodeDocValue(kv, docObject);
      if (docObject == null && documents != null) {
        documents.put(column, docObj);
      }
      docObject = docObj;

      // iterate all fields for certain document by default
      Iterator<byte[]> fitr = docObject.getFields().iterator();
      if (docAndFields != null ) {
        Map<byte[], Set<byte[]>> fieldMaps = docAndFields.get(columnfamily);
        if (fieldMaps != null) {
          Set<byte[]> fieldSet = fieldMaps.get(qualifier);
          if (fieldSet != null && !fieldSet.isEmpty()) {
            // has required filed list
            fitr = fieldSet.iterator();
          }
        }
      }

      while (fitr.hasNext()) {
        byte[] field = fitr.next();
        byte[] newValue = docObject.getValue(field);
        byte[] newQualifier = DotUtil.combineDocAndField(qualifier, field);

        if (Bytes.equals(newValue, nullValue) || newValue == null) {
          // if newValue is null, won't add this kv to the final result list
          continue;
        }
        KeyValue newKV = new KeyValue(kv.getBuffer(), kv.getRowOffset(),
            kv.getRowLength(), columnfamily, 0, columnfamily.length,
            newQualifier, 0, newQualifier.length, timestamp, type,
            newValue, 0, newValue.length);
        // newKV.setMemstoreTS(kv.getMemstoreTS());
        // LOG.info("add new KV item to kvlist: " + newKV);
        kvlist.addKVItem(newKV);
      }
    }

  }


  /**
   * Decode all document element, and return back "doc.field" and its value one
   * by one.
   *
   */
  @Override
  public boolean postScannerNext(
      final ObserverContext<RegionCoprocessorEnvironment> e,
      final InternalScanner s, final List<Result> results, final int limit,
      final boolean hasMore) throws IOException {
    HTableDescriptor desc = e.getEnvironment().getRegion().getTableDesc();
    if (!DotUtil.isDot(desc))
      return hasMore;

    // LOG.debug("get scan id: " + s.toString());
    Scan scan = scanMap.get(s);
    int batch = scan.getBatch();
    Filter flt = getFilterInstance(scan.getFilter());
    Map<byte[], Map<byte[], Set<byte[]>>> docAndFields = requiredFields
        .get(scan);
    Configuration conf = e.getEnvironment().getConfiguration();

    final List<Result> newResults = new ArrayList<Result>();

    /**
     * A new KVList wrapper. <br>
     * 1. add a new KV element into the list<br>
     * 2. check the list's limitation<br>
     * 3. create a new 'ROW' if it is over limits<br>
     * 4. flush all remaining content to the newResults<br>
     */
    class KVList extends KVListBase {
      List<KeyValue> kvList = new ArrayList<KeyValue>();
      int limits;

      public KVList( int batch ) {
        limits = batch;
      }

      @Override
      void addKVItem(KeyValue kv) {
        kvList.add(kv);
        if (limits > 0 && kvList.size() % limits == 0) {
          // LOG.debug("kvlist: " +kvList.size());
          newResults.add(new Result(kvList));
          kvList.clear();
        }
      }

      @Override
      void flush() {
        if (kvList.size() > 0) {
          // LOG.debug("kvlist: " +kvList.size());
          newResults.add(new Result(kvList));
          kvList.clear();
        }
      }
    }

    KVList kvlist = new KVList(batch);
    // columnfamily+qualifier
    Map<byte[], Document> documents = new TreeMap(Bytes.BYTES_COMPARATOR);
    // LOG.debug("old row number " + results.size() + " batch: " +
    // scan.getBatch());
    // 1. Get the doc object
    // 2. if field list specified, return those required fields only
    // Otherwise, return all doc fields one by one.
    Iterator<Result> iterator = results.iterator(); // single row result
    while (iterator.hasNext()) {
      Result r = iterator.next();
      KeyValue[] kvs = r.raw();
      for (KeyValue kv : kvs) {
        postGetOrScan(kv, docAndFields,documents,kvlist,
      desc, flt, this.nullFormatBytes);
      }

      // flush all remained part
      kvlist.flush();
    }

    // TODO we may delete this line
    // after entire row emitted, if all those left parts will be flushed out
    // as well.
    kvlist.flush();

    results.clear();
    // update the result list
    results.addAll(newResults);

    return hasMore;
  }

  @Override
  public RegionScanner preScannerOpen(
      final ObserverContext<RegionCoprocessorEnvironment> e, final Scan scan,
      final RegionScanner s) throws IOException {
    HTableDescriptor desc = e.getEnvironment().getRegion().getTableDesc();
    if (!DotUtil.isDot(desc)) {
      return s;
    }
    try {
      prepareGetOrScan(scan);
    } catch (IllegalAccessException e1) {
      throw new IOException("Failures with operator", e1);
    } catch (IllegalArgumentException e1) {
      throw new IOException("Failures with operator", e1);
    } catch (InvocationTargetException e1) {
      throw new IOException("Failures with operator", e1);
    } catch (NoSuchMethodException e1) {
      throw new IOException("Failures with operator", e1);
    } catch (SecurityException e1) {
      throw new IOException("Failures with operator", e1);
    }

    return s;
  }

  /**
   * To store the scanner => scan into region description, see {@link #scanMap}
   */
  @Override
  public RegionScanner postScannerOpen(
      final ObserverContext<RegionCoprocessorEnvironment> e, final Scan scan,
      final RegionScanner s) throws IOException {
    HTableDescriptor desc = e.getEnvironment().getRegion().getTableDesc();
    if (DotUtil.isDot(desc)) {
      if (s != null) {
        // LOG.debug("put scan id: " + s.toString());
        scanMap.put(s, scan);
      }
    }
    return s;
  }

  /**
   * Sweep all element related to a scan
   */
  @Override
  public void postScannerClose(
      final ObserverContext<RegionCoprocessorEnvironment> e,
      final InternalScanner s) throws IOException {
    HTableDescriptor desc = e.getEnvironment().getRegion().getTableDesc();
    if (!DotUtil.isDot(desc)) return;
    // To remove the corresponding item from scanmap.
    Scan scan = scanMap.get(s);
    Filter flt = getFilterInstance(scan.getFilter());
    if (flt != null) {
      filterKVs.remove(flt);
      docObjectCache.remove(flt);
    }
    scanMap.remove(s);
    requiredFields.remove(scan);
  }

  // ------------------------------------------------------------------------<br>
  // To hook those filter related stuffs<br>
  // DOT Only focuses on the Value or Qualifier related part<br>
  // 1. getNextKeyHint<br>
  // 2. filterKeyValue<br>
  // 3. transform<br>
  // 4. filterRow<br>
  // ------------------------------------------------------------------------<br>

  public void doFilterRow(List<KeyValue> kvs, Filter filter) throws IOException {
    // 1. to parse the doc to filed list
    // 2. to new some fielded based filters
    // 3. to bypass the actual filter

    // docRow -> (kv, index) , index is sorted in ASC
    Map<byte[], Pair<KeyValue, Integer>> returnList = 
        new TreeMap<byte[], Pair<KeyValue, Integer>>(Bytes.BYTES_COMPARATOR);

    List<KeyValue> toProcess = new ArrayList<KeyValue>();
    for (int i = 0; i < kvs.size(); i++) {
      KeyValue kv = kvs.get(i);
      // LOG.info("kv in preFilterRow:  " + kv);
      byte[] docRow = kv.getKey();

      if (!filterKVs.get(filter).containsKey(docRow)) {
        throw new IOException(DotException.MISSING_FILTER_CANDIDATE);
      }

      returnList.put(docRow, Pair.newPair(kv, i));
      Set<KeyValue> docKVSet = filterKVs.get(filter).get(docRow);
      toProcess.addAll(docKVSet);
      docKVSet.clear();
    }

    // LOG.debug("toprocess size: " + toProcess.size());
    // do the filterRow based on field-list
    
    // TODO Fixme! The extra call to filterRow() here will break some
    // Filters' feature. e.g. PageFilter.
    filter.filterRow(toProcess);
    boolean toDelete = toProcess.isEmpty() || filter.filterRow();
    if (!toDelete) {
      // update the results based on field-list
      for (KeyValue kv : toProcess) {
        byte[][] splits = DotUtil.getDocAndField(kv.getBuffer(),
            kv.getQualifierOffset(), kv.getQualifierLength());

        KeyValue kvCopy = new KeyValue(kv.getBuffer(), kv.getRowOffset(),
            kv.getRowLength(), kv.getBuffer(), kv.getFamilyOffset(),
            kv.getFamilyLength(), splits[0], 0, splits[0].length,
            kv.getTimestamp(), Type.codeToType(kv.getType()), null,
            0, 0);
        byte[] docRow = kvCopy.getKey();
        returnList.get(docRow).setSecond(null);
        filterKVs.get(filter).get(docRow).add(kv);
      }

      // delete not included kv in kvs.
      int correction = 0;
      for (Pair<KeyValue, Integer> pair : returnList.values()) {
        if (pair.getSecond() != null) {
          int index = pair.getSecond() - correction;
          kvs.remove(index);
          correction++;
        }
      }
    } else {
      kvs.clear();
    }

  }

  public KeyValue doFilterTransform( KeyValue raw, Filter filter) throws IOException {
    // transform all candidates
    byte[] docRow = raw.getKey();

    Iterator<KeyValue> iter = this.filterKVs.get(filter).get(docRow).iterator();
    Set<KeyValue> newSets = new TreeSet<KeyValue>(new KeyValue.KVComparator());
    // only transform qualified kvs.
    while (iter.hasNext()) {
      KeyValue newKV = filter.transform((KeyValue) iter.next());
      newSets.add(newKV);
      iter.remove();
    }

    this.filterKVs.get(filter).get(docRow).addAll(newSets);
    return raw;
  }

  public KeyValue doGetNextKeyHint(
      KeyValue raw, Filter filter) throws IOException {
    // return the doc.field to doc in returnedKV.

    KeyValue result = null;
    result = filter.getNextKeyHint(raw);
    
    // TODO this function need to be rewritten
    // current impl not working at all, will make it seek to the previous one thus loop forever.
/*
      // TODO should not ignore the value part.
      result = KeyValue.createFirstOnRow(raw.getBuffer(),
          raw.getRowOffset(),
          raw.getRowLength(),

          raw.getBuffer(), raw.getFamilyOffset(),
          raw.getFamilyLength(),

          raw.getBuffer(),raw.getQualifierOffset(),
          raw.getQualifierLength());
    }
*/
    return result;
  }

  public ReturnCode doFilterKeyValue(KeyValue kv, Filter filter) throws IOException {
    // translate doc to fields
    // merge the returnCode correctly
    // LOG.info("preFilterKeyValue: " + filter + kv);

      byte[] columnfamily = DotUtil.getKVColumnfamily(kv);
      Set<byte[]> fieldlist = null;

      byte[] docRow = kv.getKey();
      Map<byte[], Set<KeyValue>> rows = this.filterKVs.get(filter);
      Set<KeyValue> kvSets = rows.get(docRow);

      if (kvSets == null) {
        kvSets = new TreeSet<KeyValue>(new KeyValue.KVComparator());
        rows.put(docRow, kvSets);
      }

      byte[] column = DotUtil.getKVColumn(kv);
      Map<byte[], Document> documents = this.docObjectCache.get(filter);
      Document docObject = documents.get(column);
      Document docObj = decodeDocValue(kv, docObject);
      if (docObject == null) {
        documents.put(column, docObj);
        docObject = docObj;
      }
      // get all fields under one doc
      fieldlist = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
      fieldlist = docObject.getFields();

      return getFilterKeyValueDecision(fieldlist, kv, filter, docObject);

  }

  private Document decodeDocValue(KeyValue kv, Document docObject) throws IOException {
    if (docObject == null) {
      Pair<Object, String> schemaPair = this.getSchema(kv);
      docObject = Document.createDoc(schemaPair.getSecond());
      docObject.loadSchema(schemaPair.getFirst());
    }

    docObject.setDoc(kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
    // docObject.initialize(schema, value, (Object) null);
    return docObject;
  }

  /**
   * 5 bits mask<br>
   * include(16), skip(8), next_col(4), next_row(2), seek_next_using_hint(1) The
   * highest priority means the least risk.
   *
   * <----bit4----bit3----bit2--- -bit1--------bit0--------><br>
   * <--INCLUDE---SKIP---NEXT_C---NEXT_R---SEEK_N_U_HINT---><br>
   */
  private enum ReturnCodePriority {

    // define sequence is important!
    SEEK_NEXT_USING_HINT(ReturnCode.SEEK_NEXT_USING_HINT),
    NEXT_ROW(ReturnCode.NEXT_ROW),
    NEXT_COL(ReturnCode.NEXT_COL),
    SKIP(ReturnCode.SKIP),
    INCLUDE(ReturnCode.INCLUDE);

    private int value;
    private ReturnCode rc;

    private ReturnCodePriority( ReturnCode rc ) {
      this.rc = rc;
      switch (rc) {
        case INCLUDE:
          value = 1 << 4;
          break;
        case SKIP:
          value = 1 << 3;
          break;
        case NEXT_COL:
          value = 1 << 2;
          break;
        case NEXT_ROW:
          value = 1 << 1;
          break;
        case SEEK_NEXT_USING_HINT:
          value = 1 << 0;
          break;
      }

    }
  }

  /**
   * Decide the final ReturnCode for the entire document element.<br>
   * Since each document field has its own ReturnCode, we need some mechanism to
   * decide the final ReturnCode for the whole document element. Here we provide
   * a priority list by 5-bit mask. We only return back the highest priority
   * ReturnCode to keep semantics. However, it somehow can be less efficient.
   *
   * @param fieldList
   * @param kv
   * @param filter
   * @param docObject
   * @param result
   * @return The final returnCode for the entire document.
   * @throws IOException
   */
  private ReturnCode getFilterKeyValueDecision(Set<byte[]> fieldList,
      KeyValue kv, Filter filter, Document docObject) throws IOException {
    ReturnCode result = ReturnCode.INCLUDE;
    byte[] qualifier = DotUtil.getKVQualifier(kv);
    byte[] docRow = kv.getKey();
    Map<byte[], Set<KeyValue>> rows = this.filterKVs.get(filter);
    // LOG.info("getFilterkeyvalueDecision  filter: " + filter);
    Set<KeyValue> kvSets = rows.get(docRow);
    // 5 bits mask
    // include(16), skip(8), next_col(4), next_row(2), seek_next_using_hint(1)
    byte returnCodeMatch = 0;

    for (byte[] field : fieldList) {
      byte[] newQualifier = DotUtil.combineDocAndField(qualifier, field);
      byte[] newValue = docObject.getValue(field);
      if (Bytes.equals(newValue, this.nullFormatBytes)) {
        // if newValue is null, won't add it to the final result list
        continue;
      }
      KeyValue newKV = new KeyValue(kv.getBuffer(), kv.getRowOffset(),
          kv.getRowLength(), kv.getBuffer(), kv.getFamilyOffset(),
          kv.getFamilyLength(), newQualifier, 0, newQualifier.length,
          kv.getTimestamp(), Type.codeToType(kv.getType()), newValue, 0,
          newValue.length);
      ReturnCode newReturned = filter.filterKeyValue(newKV);
      if (newReturned == ReturnCode.INCLUDE) {
        // LOG.info("put: " + docRow + " " + newKV);
        kvSets.add(newKV);
        returnCodeMatch = (byte) ( returnCodeMatch | ReturnCodePriority.INCLUDE.value );
      } else if (newReturned == ReturnCode.SKIP) {
        returnCodeMatch = (byte) ( returnCodeMatch | ReturnCodePriority.SKIP.value );
      } else if (newReturned == ReturnCode.NEXT_COL) {
        returnCodeMatch = (byte) ( returnCodeMatch | ReturnCodePriority.NEXT_COL.value );
      } else if (newReturned == ReturnCode.NEXT_ROW) {
        returnCodeMatch = (byte) ( returnCodeMatch | ReturnCodePriority.NEXT_ROW.value );
      } else if (newReturned == ReturnCode.SEEK_NEXT_USING_HINT) {
        returnCodeMatch = (byte) ( returnCodeMatch | ReturnCodePriority.SEEK_NEXT_USING_HINT.value );
      }

    }

    // TODO Should not depends on internal bit sequence. 
    // Need to rewrite the logic to simplify these codes.
    for (int i = ReturnCodePriority.values().length; i >= 0; i--) {
      if ( ( returnCodeMatch >> i ) == 1) {
        // return back the highest priority returnCode
        result = ReturnCodePriority.values()[i].rc;
        break;
      }
    }

    return result;
  }

}
