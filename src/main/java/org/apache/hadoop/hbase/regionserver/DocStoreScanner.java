/**
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

package org.apache.hadoop.hbase.regionserver;

import com.intel.hadoop.hbase.dot.DotConstants;
import com.intel.hadoop.hbase.dot.DotUtil;
import com.intel.hadoop.hbase.dot.doc.Document.DocSchemaField;
import com.intel.hadoop.hbase.dot.doc.Document;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.HStore.ScanInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.StringUtils;


/**
 * Scanner scans both the memstore and the HStore. Coalesce KeyValue stream
 * into List<KeyValue> for a single row.
 */
@InterfaceAudience.Private
public class DocStoreScanner extends StoreScanner {
  static final Log LOG = LogFactory.getLog(DocStoreScanner.class);

  private Map<String, Pair<Object, String>> schemaMap = new HashMap<String, Pair<Object, String>>();
  private Map<byte[], Document> docObjCache  = new TreeMap<byte[], Document>(Bytes.BYTES_COMPARATOR);
  private byte[] nullFormatBytes;

  private DocKV remainKV = null;

  class DocKV {
    private long KVtimestamp;
    private Type KVtype;
    private Document docObject = null;
    private List<DocSchemaField> fieldList;
    private List<byte[]> values;
    private int rowOffset;
    private short rowLen;
    private int familyOffset;
    private byte familyLen;
    private int index = 0;
    private int kvSize = 1;
    private KeyValue rawKV = null;
    boolean isRaw = false;

    public DocKV(KeyValue kv) throws IOException {

      docObject = getDocForKV(kv);
      rawKV = kv;
      if ((null == docObject)) {
        isRaw = true;
        return;
      }

      KVtimestamp = kv.getTimestamp();
      KVtype = Type.codeToType(kv.getType());

      // get all fields under one doc
      fieldList = docObject.getFields();
      values = docObject.getValues();

      rowOffset = kv.getRowOffset();
      rowLen = kv.getRowLength();
      familyOffset = kv.getFamilyOffset();
      familyLen = kv.getFamilyLength();

      assert (fieldList.size() == values.size());
      kvSize = fieldList.size();

      return;
    }
    
    public KeyValue getNextKV() {
      
      if(isRaw) {
        // only return once the rawKV
        index++;
        return index > 1 ? null : rawKV;
      }
        
      byte[] newQualifier = null;
      byte[] newValue = null;

      while(index < kvSize) {
        newQualifier = fieldList.get(index).docWithField;
        newValue = values.get(index);
        if (!Bytes.equals(newValue, nullFormatBytes))
          break;

        // if newValue is null, continue search;
        index++;
        continue;
      }

      if (index >= kvSize)
        return null;
      
      index++;
      return new KeyValue(rawKV.getBuffer(), rowOffset,
          rowLen, rawKV.getBuffer(), familyOffset,
          familyLen, newQualifier, 0, newQualifier.length,
          KVtimestamp, KVtype, newValue, 0, newValue.length);
    }
    
    public boolean isDone() {
      return index >= kvSize;
    }
  }

  /**
   * Opens a scanner across memstore, snapshot, and all StoreFiles. Assumes we
   * are not in a compaction.
   *
   * @param store who we scan
   * @param scan the spec
   * @param columns which columns we are scanning
   * @throws IOException
   */
  public DocStoreScanner(HStore store, ScanInfo scanInfo, Scan scan, final NavigableSet<byte[]> columns)
                              throws IOException {
    super(store, scanInfo, scan, columns);
    prepareForDoc();
  }

  /**
   * init various data for use by doc. Need to be called before any other method.
   * @param nullFormatBytes
   */
  public void init(byte[] nullFormatBytes) {
    // TODO Auto-generated method stub
    this.nullFormatBytes = nullFormatBytes;
  }

  /**
   * Prepare staffs for DOC.
   */
  private void prepareForDoc() {

    HColumnDescriptor hcd = store.getFamily();
    String[] docs = StringUtils.getStrings(
        hcd.getValue(DotConstants.HBASE_DOT_COLUMNFAMILY_DOC_ELEMENT));

    for (String doc : docs) {
      String columnfamilyName = hcd.getNameAsString();
      String docNameWithColumnfamilyName = DotUtil
          .getDocNameWithColumnfamilyName(columnfamilyName, doc);
      String serializer = hcd
          .getValue(DotConstants.HBASE_DOT_COLUMNFAMILY_DOC_SERIALIZER_CLASS);

      // always put the schema object into the schemaMap
      String schema = hcd
          .getValue(DotConstants.HBASE_DOT_COLUMNFAMILY_DOC_SCHEMA_PREFIX
              + doc);
      schemaMap.put(docNameWithColumnfamilyName,
          Pair.newPair(Document.parseSchema(serializer, schema), serializer));
    }

  }

  private Document getDocForKV(KeyValue kv) throws IOException {

    byte[] qualifier = DotUtil.getKVQualifier(kv);
    Document docObject = docObjCache.get(qualifier);

    if (docObject == null) {
      String docNameWithColumnfamilyName = DotUtil.getDocNameWithColumnfamilyName(kv);
      Pair<Object, String> schemaObj = schemaMap.get(docNameWithColumnfamilyName);
      if (null == schemaObj) {
        //throw new DotInvalidIOException("Cannot find schemaObj for " + docNameWithColumnfamilyName);
        return null;
      }

      docObject = Document.createDoc(schemaObj.getSecond());
      docObject.loadSchema(schemaObj.getFirst());
      docObjCache.put(qualifier, docObject);
    }

    docObject.setDoc(kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
    return docObject;
  }

  /**
   * Get the next row of values from this Store.
   * @param outResult
   * @param limit
   * @return true if there are more rows, false if scanner is done
   */
  @Override
  public synchronized boolean next(List<KeyValue> outResult, int limit,
      String metric) throws IOException {

    if (checkReseek()) {
      return true;
    }

    // if the heap was left null, then the scanners had previously run out anyways, close and
    // return.
    if (this.heap == null) {
      close();
      return false;
    }

    KeyValue peeked = this.heap.peek();
    if (peeked == null) {
      close();
      return false;
    }

    // only call setRow if the row changes; avoids confusing the query matcher
    // if scanning intra-row
    byte[] row = peeked.getBuffer();
    int offset = peeked.getRowOffset();
    short length = peeked.getRowLength();
    if ((matcher.row == null) || !Bytes.equals(row, offset, length, matcher.row, matcher.rowOffset, matcher.rowLength)) {
      this.countPerRow = 0;
      matcher.setRow(row, offset, length);
    }

    KeyValue rawKV;
    KeyValue prevKV = null;
    DocKV dockv = null;

    // Only do a sanity-check if store and comparator are available.
    KeyValue.KVComparator comparator =
        store != null ? store.getComparator() : null;

    long cumulativeMetric = 0;
    int count = 0;
    boolean isRawKV = false;
    try {
      LOOP: while((rawKV = this.heap.peek()) != null) {
        if (prevKV != null && comparator != null) {
          // Check that the heap gives us KVs in an increasing order.
          assert comparator.compare(prevKV, rawKV) <= 0 :
            "Key " + prevKV + " followed by a " + "smaller key " + rawKV + " in cf " + store;
          if (comparator.matchingRowColumn(prevKV, rawKV)) {
            // TODO We don't support multiple version KV for doc at present. Ignore KV other than the latest one on the same row and family:column.
            this.heap.next();
            continue;
          }
        }

        prevKV = rawKV;
        
        if (remainKV == null) {
          dockv = new DocKV(rawKV);
        } else {
          // we are on the same KV we leave last time due to 'limit' is reached.
          // just complete the remain part of the KV
          dockv = remainKV;
          remainKV = null;
        }

        isRawKV = dockv.isRaw;
        boolean skipDoc = false;
        // TODO : need to handle batch operation (limit), i.e. need to handle various seek
        while (true) {
          KeyValue kv = dockv.getNextKV();
          if (kv == null)
            break;

          ScanQueryMatcher.MatchCode qcode = matcher.match(kv);
          switch (qcode) {
          case INCLUDE:
          case INCLUDE_AND_SEEK_NEXT_ROW:
          case INCLUDE_AND_SEEK_NEXT_COL:

            Filter f = matcher.getFilter();
            if (f != null) {
              kv = f.transform(kv);
            }

            this.countPerRow++;
            if (storeLimit > -1
                && this.countPerRow > (storeLimit + storeOffset)) {
              // do what SEEK_NEXT_ROW does.
              if (!matcher.moreRowsMayExistAfter(kv)) {
                return false;
              }
              reseek(matcher.getKeyForNextRow(kv));
              break LOOP;
            }

            // add to results only if we have skipped #storeOffset kvs
            // also update metric accordingly
            if (this.countPerRow > storeOffset) {
              if (metric != null) {
                cumulativeMetric += kv.getLength();
              }
              outResult.add(kv);
              count++;
            }

            if (qcode == ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_ROW) {
              if (!matcher.moreRowsMayExistAfter(kv)) {
                return false;
              }
              reseek(matcher.getKeyForNextRow(kv));
              skipDoc = true;
            } else if (qcode == ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_COL) {
              // TODO need to fix this correctly.
              // currently , just ignore it when it is not raw kv.
              if (isRawKV) {
                reseek(matcher.getKeyForNextColumn(kv));
              }
            } else {
            }

            if (limit > 0 && (count == limit)) {
              // TODO Double Check the boundary condition carefully.

              // check whether we are already done on this DOC
              if (dockv.isDone())
                break LOOP;

              remainKV = dockv;
              break LOOP;
            }

            if (skipDoc)
              continue LOOP;
            else
              continue;

          case DONE:
            return true;

          case DONE_SCAN:
            close();
            return false;

          case SEEK_NEXT_ROW:
            // This is just a relatively simple end of scan fix, to short-cut
            // end
            // us if there is an endKey in the scan.
            if (!matcher.moreRowsMayExistAfter(kv)) {
              return false;
            }

            // TODO need to fix this correctly.
            reseek(matcher.getKeyForNextRow(kv));
            continue LOOP;

          case SEEK_NEXT_COL:
            // TODO need to fix this correctly.
            // Just ignore this for now if it is not raw kv.
            if (isRawKV) {
              reseek(matcher.getKeyForNextColumn(kv));
            }
            continue;

          case SKIP:
            break;

          case SEEK_NEXT_USING_HINT:
            KeyValue nextKV = matcher.getNextKeyHint(kv);
            if (nextKV != null) {
              // TODO need to fix this correctly.
              // Ignore this for now.
              // reseek(nextKV);
              continue;
            } else {
            }
            break;

          default:
            throw new RuntimeException("UNEXPECTED");
          }
        }

        this.heap.next();
      }
    } finally {
      if (cumulativeMetric > 0 && metric != null) {

      }
    }

    if (count > 0) {
      return true;
    }

    // No more keys
    close();
    return false;
  }

}

