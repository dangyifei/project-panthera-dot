/**
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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList.Operator;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;


class StringRange {
  private String start = null;
  private String end = null;
  private boolean startInclusive = true;
  private boolean endInclusive = false;

  public StringRange(String start, boolean startInclusive, String end,
      boolean endInclusive) {
    this.start = start;
    this.startInclusive = startInclusive;
    this.end = end;
    this.endInclusive = endInclusive;
  }

  public String getStart() {
    return this.start;
  }

  public String getEnd() {
    return this.end;
  }

  public boolean isStartInclusive() {
    return this.startInclusive;
  }

  public boolean isEndInclusive() {
    return this.endInclusive;
  }

  @Override
  public int hashCode() {
    int hashCode = 0;
    if (this.start != null) {
      hashCode ^= this.start.hashCode();
    }

    if (this.end != null) {
      hashCode ^= this.end.hashCode();
    }
    return hashCode;
  }

  @Override
  public String toString() {
    String result = (this.startInclusive ? "[" : "(")
          + (this.start == null ? null : this.start) + ", "
          + (this.end == null ? null : this.end)
          + (this.endInclusive ? "]" : ")");
    return result;
  }

   public boolean inRange(String value) {
    boolean afterStart = true;
    if (this.start != null) {
      int startCmp = value.compareTo(this.start);
      afterStart = this.startInclusive ? startCmp >= 0 : startCmp > 0;
    }

    boolean beforeEnd = true;
    if (this.end != null) {
      int endCmp = value.compareTo(this.end);
      beforeEnd = this.endInclusive ? endCmp <= 0 : endCmp < 0;
    }

    return afterStart && beforeEnd;
  }
}


@Category(MediumTests.class)
public class TestFiltersOnDot {

  static final Log LOG = LogFactory.getLog(TestFiltersOnDot.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static HBaseAdmin admin = null;
  private static Configuration conf = null;

  private static final String FilterTableName = "TestTableForFilters";
  private static HTable FilterTable = null;

  private static boolean needToResetTable = false;

  private static final byte [][] ROWS_ONE = {
    Bytes.toBytes("testRowOne-0"), Bytes.toBytes("testRowOne-1"),
    Bytes.toBytes("testRowOne-2"), Bytes.toBytes("testRowOne-3")
  };

  private static final byte [][] ROWS_TWO = {
    Bytes.toBytes("testRowTwo-0"), Bytes.toBytes("testRowTwo-1"),
    Bytes.toBytes("testRowTwo-2"), Bytes.toBytes("testRowTwo-3")
  };

  private static final byte [][] ROWS_THREE = {
    Bytes.toBytes("testRowThree-0"), Bytes.toBytes("testRowThree-1"),
    Bytes.toBytes("testRowThree-2"), Bytes.toBytes("testRowThree-3")
  };

  private static final byte [][] ROWS_FOUR = {
    Bytes.toBytes("testRowFour-0"), Bytes.toBytes("testRowFour-1"),
    Bytes.toBytes("testRowFour-2"), Bytes.toBytes("testRowFour-3")
  };

  private static final byte [][] FAMILIES = {
    Bytes.toBytes("testFamilyOne"), Bytes.toBytes("testFamilyTwo")
  };

  private static final byte [][] FAMILIES_1 = {
    Bytes.toBytes("testFamilyThree"), Bytes.toBytes("testFamilyFour")
  };

  private static final byte [][] QUALIFIERS_ONE = {
    Bytes.toBytes("testQualifierOne.f0"),
    /*Bytes.toBytes("testQualifierOne.f1"),*/
    Bytes.toBytes("testQualifierOne.f2"), Bytes.toBytes("testQualifierOne.f3")
  };

  private static final byte [][] QUALIFIERS_TWO = {
    Bytes.toBytes("testQualifierTwo.f0"),
    /*Bytes.toBytes("testQualifierTwo.f1"),*/
    Bytes.toBytes("testQualifierTwo.f2"), Bytes.toBytes("testQualifierTwo.f3")
  };

  private static final byte [][] QUALIFIERS_THREE = {
    Bytes.toBytes("testQualifierThree.f0"), Bytes.toBytes("testQualifierThree.f1"),
    Bytes.toBytes("testQualifierThree.f2"), Bytes.toBytes("testQualifierThree.f3")
  };

  private static final byte [][] QUALIFIERS_FOUR = {
    Bytes.toBytes("testQualifierFour.f0"), Bytes.toBytes("testQualifierFour.f1"),
    Bytes.toBytes("testQualifierFour.f2"), Bytes.toBytes("testQualifierFour.f3")
  };

  private static final byte [][] VALUES = {
    Bytes.toBytes("testValueOne"), Bytes.toBytes("testValueTwo")
  };

  byte [][] NEW_FAMILIES = {
      Bytes.toBytes("f1"), Bytes.toBytes("f2")
    };

  private static long numRows = ROWS_ONE.length + ROWS_TWO.length;
  private static long colsPerRow = FAMILIES.length * QUALIFIERS_ONE.length;

  private static void createTestTableForFilters() throws IOException {

    String tableName = FilterTableName;

    Map<String, List<String>> layouts = new HashMap<String, List<String>>();

    // create table with families x qualifiers one/two
    for (byte[] family : FAMILIES) {
      List<String> columns = new ArrayList<String>();
      for (byte[] q : QUALIFIERS_ONE) {
        columns.add(Bytes.toString(q));
      }
      for (byte[] q : QUALIFIERS_TWO) {
        columns.add(Bytes.toString(q));
      }
      layouts.put(Bytes.toString(family), columns);
    }
    createDotTable(tableName, layouts);
    //createNormalTable(tableName, layouts);

    numRows = ROWS_ONE.length + ROWS_TWO.length;
    colsPerRow = FAMILIES.length * QUALIFIERS_ONE.length;

    // open table;
    HTable table = null;
    try {
      table = new HTable(conf, tableName);
      FilterTable = table;
    } catch (IOException e) {
      assertNull("Failed to create table", e);
    }

    try {
      // Insert first half
      for (byte[] ROW : ROWS_ONE) {
        Put p = new Put(ROW);
        p.setWriteToWAL(false);
        for (byte[] QUALIFIER : QUALIFIERS_ONE) {
          p.add(FAMILIES[0], QUALIFIER, VALUES[0]);
        }
        table.put(p);
      }
      for (byte[] ROW : ROWS_TWO) {
        Put p = new Put(ROW);
        p.setWriteToWAL(false);
        for (byte[] QUALIFIER : QUALIFIERS_TWO) {
          p.add(FAMILIES[1], QUALIFIER, VALUES[1]);
        }
        table.put(p);
      }

      // Insert second half (reverse families)
      for (byte[] ROW : ROWS_ONE) {
        Put p = new Put(ROW);
        p.setWriteToWAL(false);
        for (byte[] QUALIFIER : QUALIFIERS_ONE) {
          p.add(FAMILIES[1], QUALIFIER, VALUES[0]);
        }
        table.put(p);
      }
      for (byte[] ROW : ROWS_TWO) {
        Put p = new Put(ROW);
        p.setWriteToWAL(false);
        for (byte[] QUALIFIER : QUALIFIERS_TWO) {
          p.add(FAMILIES[0], QUALIFIER, VALUES[1]);
        }
        table.put(p);
      }

      // Flush
      TEST_UTIL.flush();

      // Delete the second qualifier from all rows and families
      // need to do this for the test?
      // If needed, we can only achieve by remove testQualifier*.1 from initial array

      // Delete the second rows from both groups
      Delete d = new Delete(ROWS_ONE[1]);
      d.deleteFamily(FAMILIES[0]);
      d.deleteFamily(FAMILIES[1]);
      table.delete(d);

      d = new Delete(ROWS_TWO[1]);
      d.deleteFamily(FAMILIES[0]);
      d.deleteFamily(FAMILIES[1]);
      table.delete(d);
      numRows -= 2;

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void initialize(Configuration conf) {
    conf.setInt("hbase.client.retries.number", 1);
    try {
      admin = TEST_UTIL.getHBaseAdmin();
    } catch (IOException e) {
      assertNull("Failed to connect to HBase master", e);
    }
    assertNotNull("No HBaseAdmin instance created", admin);

    try {
      createTestTableForFilters();
    } catch (IOException e) {
      assertNull("Failed to create Test Table for Filters", e);
    }
  }

  private static void createNormalTable(String tableName,
      Map<String, List<String>> layouts) {

    HTableDescriptor htd = new HTableDescriptor(tableName);
    for (Map.Entry<String, List<String>> cfLayout : layouts.entrySet()) {
      String family = cfLayout.getKey();
      HColumnDescriptor cfdesc = new HColumnDescriptor(family);
      htd.addFamily(cfdesc);
    }

    try {
      admin.createTable(htd);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void createDotTable(String tableName,
      Map<String, List<String>> layouts) {

    HTableDescriptor htd = new HTableDescriptor(tableName);

    for (Map.Entry<String, List<String>> cfLayout : layouts.entrySet()) {
      String family = cfLayout.getKey();
      List<String> columns = cfLayout.getValue();

      HColumnDescriptor cfdesc = new HColumnDescriptor(family);

      Map<String, List<String>> docsMap = new HashMap<String, List<String>>();

      for (String q : columns) {
        int idx = q.indexOf(".");
        String doc = q.substring(0, idx);
        String field = q.substring(idx + 1);

        List<String> fieldList = docsMap.get(doc);

        if (fieldList == null) {
          fieldList = new ArrayList<String>();
          docsMap.put(doc, fieldList);
        }

        fieldList.add(field);
      }

      String[] docs = new String[docsMap.entrySet().size()];
      int index = 0;

      for (Map.Entry<String, List<String>> m : docsMap.entrySet()) {
        String docName = m.getKey();
        List<String> fields = m.getValue();
        boolean firstField = true;

        docs[index++] = docName;

        String docSchemaId = "hbase.dot.columnfamily.doc.schema." + docName;
        String docSchemaValue = " {    \n" + " \"name\": \"" + docName
            + "\", \n" + " \"type\": \"record\",\n" + " \"fields\": [\n";
        for (String field : fields) {
          if (firstField) {
            firstField = false;
          } else {
            docSchemaValue += ", \n";
          }
          docSchemaValue += " {\"name\": \"" + field
              + "\", \"type\": \"bytes\"}";
        }

        docSchemaValue += " ]}";
        LOG.info("--- " + family + ":" + docName + " = " + docSchemaValue);
        cfdesc.setValue(docSchemaId, docSchemaValue);
      }
      String docElements = StringUtils.arrayToString(docs);
      cfdesc.setValue("hbase.dot.columnfamily.doc.element", docElements);
      htd.addFamily(cfdesc);
    }

    htd.setValue("hbase.dot.enable", "true");
    htd.setValue("hbase.dot.type", "ANALYTICAL");

    try {
      admin.createTable(htd);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void deleteTable(String tableName) {
    try {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    } catch (IOException e) {
      assertNull("Failed to delete table", e);
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    conf = TEST_UTIL.getConfiguration();

    conf.set("hbase.coprocessor.region.classes",
        "com.intel.hadoop.hbase.dot.access.DataManipulationOps");
    conf.set("hbase.coprocessor.master.classes",
        "com.intel.hadoop.hbase.dot.access.DataDefinitionOps");

    TEST_UTIL.startMiniCluster(1);
    initialize(conf);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void resetTable() throws Exception {
    if (needToResetTable == false)
      return;
    needToResetTable = true;
    if (FilterTable != null) {
      FilterTable.close();
      deleteTable(FilterTableName);
    }
    createTestTableForFilters();
  }

  private void verifyScan(Scan s, long expectedRows, long expectedKeys)
  throws IOException {

    ResultScanner scanner = FilterTable.getScanner(s);
    int row = 0;
    int kv_number = 0;

    for (Result result : scanner) {
      LOG.info("counter=" + row + ", " + result);
      row++;
      kv_number = result.list().size();

      assertTrue("Scanned too many rows! Only expected " + expectedRows +
          " total but already scanned " + row, expectedRows >= row);
      assertEquals("Expected " + expectedKeys + " keys per row but " +
          "returned " + kv_number, expectedKeys, kv_number);

    }
    assertEquals("Expected " + expectedRows + " rows but scanned " + row +
        " rows", expectedRows, row);
  }

  private void verifyScanNoEarlyOut(Scan s, long expectedRows,
      long expectedKeys)
  throws IOException {
    ResultScanner scanner = FilterTable.getScanner(s);
    int row = 0;
    int kv_number = 0;

    for (Result result : scanner) {
      LOG.info("counter=" + row + ", " + result);
      row++;
      kv_number = result.list().size();

      assertTrue("Scanned too many rows! Only expected " + expectedRows +
          " total but already scanned " + row, expectedRows >= row);
      assertEquals("Expected " + expectedKeys + " keys per row but " +
          "returned " + kv_number, expectedKeys, kv_number);
    }
    assertEquals("Expected " + expectedRows + " rows but scanned " + row +
        " rows", expectedRows, row);
  }

  private void verifyScanFull(Scan s, KeyValue [] kvs)
  throws IOException {
    ResultScanner scanner = FilterTable.getScanner(s);
    int row = 0;
    int kv_number = 0;
    int idx = 0;

    for (Result result : scanner) {
      row++;
      kv_number += result.list().size();

      assertTrue("Scanned too many keys! Only expected " + kvs.length +
          " total but already scanned " + kv_number,
          kvs.length >= kv_number);

      // result.list() need to be sort?
      for(KeyValue kv : result.list()) {
        LOG.info("row=" + row + ", result=" + kv.toString() +
            ", match=" + kvs[idx].toString());
        assertTrue("Row mismatch",
            Bytes.equals(kv.getRow(), kvs[idx].getRow()));
        assertTrue("Family mismatch",
            Bytes.equals(kv.getFamily(), kvs[idx].getFamily()));
        assertTrue("Qualifier mismatch",
            Bytes.equals(kv.getQualifier(), kvs[idx].getQualifier()));
        assertTrue("Value mismatch",
            Bytes.equals(kv.getValue(), kvs[idx].getValue()));
        idx++;
      }
    }
    LOG.info("Looked at " + row + " rows with " + idx + " keys");
    assertEquals("Expected " + kvs.length + " total keys but scanned " + idx,
        kvs.length, idx);
  }

  List<String> generateRandomWords(int numberOfWords, String suffix) {
    return generateRandomWords(numberOfWords, suffix, null);
  }

  List<String> generateRandomWords(int numberOfWords, String suffix,
      String prefix) {
    Set<String> wordSet = new HashSet<String>();
    for (int i = 0; i < numberOfWords; i++) {
      int lengthOfWords = (int) (Math.random() * 2) + 1;
      char[] wordChar = new char[lengthOfWords];
      for (int j = 0; j < wordChar.length; j++) {
        wordChar[j] = (char) (Math.random() * 26 + 97);
      }
      String word;
      if (suffix == null) {
        word = new String(wordChar);
      } else {
        word = new String(wordChar) + suffix;
      }
      if (prefix != null) {
        word = prefix + word;
      }
      wordSet.add(word);
    }
    List<String> wordList = new ArrayList<String>(wordSet);
    return wordList;
  }

  List<String> generateRandomWords(int numberOfWords, int maxLengthOfWords, String prefix) {
    Set<String> wordSet = new HashSet<String>();
    for (int i = 0; i < numberOfWords; i++) {
      int lengthOfWords = (int) (Math.random() * maxLengthOfWords) + 1;
      char[] wordChar = new char[lengthOfWords];
      for (int j = 0; j < wordChar.length; j++) {
        wordChar[j] = (char) (Math.random() * 26 + 97);
      }
      String word = new String(wordChar);
      if (prefix != null) {
        word = prefix + word;
      }
      wordSet.add(word);
    }
    List<String> wordList = new ArrayList<String>(wordSet);
    return wordList;
  }

  List<String> getFixedColumns() {
    List<String> wordList = new ArrayList<String>();
    wordList.add("d1.f1");
    wordList.add("d1.f2");
    return wordList;
  }

  private byte [][] makeN(byte [] base, int n) {
    if (n > 256) {
      return makeNBig(base, n);
    }
    byte [][] ret = new byte[n][];
    for(int i=0;i<n;i++) {
      ret[i] = Bytes.add(base, new byte[]{(byte)i});
    }
    return ret;
  }

  private byte [][] makeNBig(byte [] base, int n) {
    byte [][] ret = new byte[n][];
    for(int i=0;i<n;i++) {
      int byteA = (i % 256);
      int byteB = (i >> 8);
      ret[i] = Bytes.add(base, new byte[]{(byte)byteB,(byte)byteA});
    }
    return ret;
  }

  @Test
  public void testColumnPrefixFilter() throws IOException {

    List<String> rows = generateRandomWords(100, "row");
    List<String> columns = generateRandomWords(1000, null, "d1.");

    String tableName = "TestColumnPrefixFilter";
    String family = "cf1";
    Map<String, List<String>> layouts = new HashMap<String, List<String>>();
    layouts.put(family, columns);
    createDotTable(tableName, layouts);
    //createNormalTable(tableName, layouts);

    HTable table = null;
    try {
      table = new HTable(conf, tableName);
    } catch (IOException e) {
      assertNull("Failed to create table", e);
    }

    try {
      List<KeyValue> kvList = new ArrayList<KeyValue>();
      Map<String, List<KeyValue>> prefixMap = new HashMap<String, List<KeyValue>>();

      prefixMap.put("d1", new ArrayList<KeyValue>());
      prefixMap.put("d1.s", new ArrayList<KeyValue>());
      prefixMap.put("dx", new ArrayList<KeyValue>());

      String valueString = "ValueString";

      for (String row : rows) {
        Put p = new Put(Bytes.toBytes(row));
        p.setWriteToWAL(false);
        for (String column : columns) {
          KeyValue kv = KeyValueTestUtil.create(row, family, column,
              HConstants.LATEST_TIMESTAMP, valueString);
          p.add(kv);
          kvList.add(kv);
          for (String s : prefixMap.keySet()) {
            if (column.startsWith(s)) {
              prefixMap.get(s).add(kv);
            }
          }
        }
        table.put(p);
      }

      // Flush
      TEST_UTIL.flush();

      ColumnPrefixFilter filter;
      Scan scan = new Scan();
      scan.setMaxVersions();
      for (String s : prefixMap.keySet()) {
        filter = new ColumnPrefixFilter(Bytes.toBytes(s));

        scan.setFilter(filter);

        ResultScanner scanner = table.getScanner(scan);
        int kv_number = 0;
        for (Result result : scanner) {
          kv_number += result.list().size();
        }
        LOG.info("prefix " + s + " : Result/Actual num = "
            + prefixMap.get(s).size() + "/" + kv_number);
        assertEquals(prefixMap.get(s).size(), kv_number);
      }
    } catch (Exception e) {
      assertNull("Exceptions :" + e.getMessage(), e);
    } finally {
      table.close();
      deleteTable(tableName);
    }
  }

  @Test
  public void testColumnPrefixFilterWithFilterList() throws IOException {

    List<String> rows = generateRandomWords(100, "row");
    List<String> columns = generateRandomWords(1000, null, "d1.");

    String tableName = "TestColumnPrefixFilterWithFilterList";
    String family = "cf1";
    Map<String, List<String>> layouts = new HashMap<String, List<String>>();
    layouts.put(family, columns);
    createDotTable(tableName, layouts);
    //createNormalTable(tableName, layouts);

    HTable table = null;
    try {
      table = new HTable(conf, tableName);
    } catch (IOException e) {
      assertNull("Failed to create table", e);
    }

    try {
      List<KeyValue> kvList = new ArrayList<KeyValue>();
      Map<String, List<KeyValue>> prefixMap = new HashMap<String, List<KeyValue>>();

      prefixMap.put("d1", new ArrayList<KeyValue>());
      prefixMap.put("d1.s", new ArrayList<KeyValue>());
      prefixMap.put("dx", new ArrayList<KeyValue>());

      String valueString = "ValueString";

      for (String row : rows) {
        Put p = new Put(Bytes.toBytes(row));
        p.setWriteToWAL(false);
        for (String column : columns) {
          KeyValue kv = KeyValueTestUtil.create(row, family, column,
              HConstants.LATEST_TIMESTAMP, valueString);
          p.add(kv);
          kvList.add(kv);
          for (String s : prefixMap.keySet()) {
            if (column.startsWith(s)) {
              prefixMap.get(s).add(kv);
            }
          }
        }
        table.put(p);
      }

      // Flush
      TEST_UTIL.flush();

      ColumnPrefixFilter filter;
      Scan scan = new Scan();
      scan.setMaxVersions();
      for (String s : prefixMap.keySet()) {
        filter = new ColumnPrefixFilter(Bytes.toBytes(s));

        // this is how this test differs from the one above
        FilterList filterList = new FilterList(
            FilterList.Operator.MUST_PASS_ALL);
        filterList.addFilter(filter);
        scan.setFilter(filterList);

        ResultScanner scanner = table.getScanner(scan);
        int kv_number = 0;
        for (Result result : scanner) {
          kv_number += result.list().size();
        }
        // LOG.info("prefix " + s + " : Result/Actual num = " +
        // prefixMap.get(s).size() + "/" + kv_number);
        assertEquals(prefixMap.get(s).size(), kv_number);
      }
    } catch (Exception e) {
      assertNull("Exceptions :" + e.getMessage(), e);
    } finally {
      table.close();
      deleteTable(tableName);
    }

  }

  @Test
  public void testMultipleColumnPrefixFilter() throws IOException {

    List<String> rows = generateRandomWords(100, "row");
    List<String> columns = generateRandomWords(1000, null, "d1.");

    String tableName = "testMultipleColumnPrefixFilter";
    String family = "cf1";
    Map<String, List<String>> layouts = new HashMap<String, List<String>>();
    layouts.put(family, columns);
    createDotTable(tableName, layouts);
    //createNormalTable(tableName, layouts);

    HTable table = null;
    try {
      table = new HTable(conf, tableName);
    } catch (IOException e) {
      assertNull("Failed to create table", e);
    }

    try {
      List<KeyValue> kvList = new ArrayList<KeyValue>();
      Map<String, List<KeyValue>> prefixMap = new HashMap<String, List<KeyValue>>();

      prefixMap.put("d1.p", new ArrayList<KeyValue>());
      prefixMap.put("d1.q", new ArrayList<KeyValue>());
      prefixMap.put("d1.s", new ArrayList<KeyValue>());

      String valueString = "ValueString";

      for (String row : rows) {
        Put p = new Put(Bytes.toBytes(row));
        p.setWriteToWAL(false);
        for (String column : columns) {
          KeyValue kv = KeyValueTestUtil.create(row, family, column,
              HConstants.LATEST_TIMESTAMP, valueString);
          p.add(kv);
          kvList.add(kv);
          for (String s : prefixMap.keySet()) {
            if (column.startsWith(s)) {
              prefixMap.get(s).add(kv);
            }
          }
        }
        table.put(p);
      }

      // Flush
      TEST_UTIL.flush();

      MultipleColumnPrefixFilter filter;
      Scan scan = new Scan();
      scan.setMaxVersions();
      byte [][] filter_prefix = new byte [2][];
      filter_prefix[0] = Bytes.toBytes("d1.p");
      filter_prefix[1] = Bytes.toBytes("d1.q");

      filter = new MultipleColumnPrefixFilter(filter_prefix);
      scan.setFilter(filter);

      ResultScanner scanner = table.getScanner(scan);
      int kv_number = 0;
      for (Result result : scanner) {
        kv_number += result.list().size();
      }
      assertEquals(prefixMap.get("d1.p").size() + prefixMap.get("d1.q").size(), kv_number);

    } catch (Exception e) {
      assertNull("Exceptions :" + e.getMessage(), e);
    } finally {
      table.close();
      deleteTable(tableName);
    }
  }

  @Test
  public void testRandomRowFilter() throws Exception {
    int max = 100000;
    int wordlength = 8;

    List<String> rows = generateRandomWords(max, wordlength, null);
    List<String> columns = generateRandomWords(1, null, "d1.");

    String tableName = "testRandomRowFilter";
    String family = "cf1";
    Map<String, List<String>> layouts = new HashMap<String, List<String>>();
    layouts.put(family, columns);
    createDotTable(tableName, layouts);
    //createNormalTable(tableName, layouts);

    HTable table = null;
    try {
      table = new HTable(conf, tableName);
    } catch (IOException e) {
      assertNull("Failed to create table", e);
    }

    try {
      String valueString = "ValueString";
      List<Put> batch = new ArrayList<Put>();
      for (String row : rows) {
        Put p = new Put(Bytes.toBytes(row));
        p.setWriteToWAL(false);
        for (String column : columns) {
          KeyValue kv = KeyValueTestUtil.create(row, family, column,
              HConstants.LATEST_TIMESTAMP, valueString);
          p.add(kv);
        }
        batch.add(p);
      }
      table.batch(batch);

      // Flush
      TEST_UTIL.flush();

      RandomRowFilter filter;
      Scan scan = new Scan();

      filter = new RandomRowFilter(0.25f);
      scan.setFilter(filter);

      ResultScanner scanner = table.getScanner(scan);
      int kv_number = 0;
      for (Result result : scanner) {
        kv_number++;
      }

      int actualRowNumber = rows.size();
      int epsilon = actualRowNumber / 100;
      double passRate = kv_number * 100.0 / actualRowNumber;
      LOG.info("Roughly 25% should pass the filter, actual " + passRate + "%");
      assertTrue("Roughly 25% should pass the filter",
          Math.abs(kv_number - actualRowNumber / 4) < epsilon);

    } catch (Exception e) {
      assertNull("Exceptions :" + e.getMessage(), e);
    } finally {
      table.close();
      deleteTable(tableName);
    }
  }

  @Test
  public void testColumnPaginationFilter() throws Exception {

    // Set of KVs (page: 1; pageSize: 1) - the first set of 1 column per row
    KeyValue [] expectedKVs = {
        // testRowOne-0
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        // testRowOne-2
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        // testRowOne-3
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        // testRowTwo-0
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        // testRowTwo-2
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        // testRowTwo-3
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1])
    };


    // Set of KVs (page: 3; pageSize: 1)  - the third set of 1 column per row
    KeyValue [] expectedKVs2 = {
        // testRowOne-0
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
        // testRowOne-2
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
        // testRowOne-3
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
        // testRowTwo-0
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        // testRowTwo-2
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        // testRowTwo-3
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
    };

    // Set of KVs (page: 2; pageSize 2)  - the 2nd set of 2 columns per row
    KeyValue [] expectedKVs3 = {
        // testRowOne-0
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
        // testRowOne-2
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
        // testRowOne-3
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
        // testRowTwo-0
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        // testRowTwo-2
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        // testRowTwo-3
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
    };


    // Set of KVs (page: 2; pageSize 2)  - the 2nd set of 2 columns per row
    KeyValue [] expectedKVs4 = {

    };

    long expectedRows = numRows;
    long expectedKeys = 1;
    Scan s = new Scan();


    // Page 1; 1 Column per page  (Limit 1, Offset 0)
    s.setFilter(new ColumnPaginationFilter(1,0));
    verifyScan(s, expectedRows, expectedKeys);
    this.verifyScanFull(s, expectedKVs);

    // Page 3; 1 Result per page  (Limit 1, Offset 2)
    s.setFilter(new ColumnPaginationFilter(1,2));
    verifyScan(s, expectedRows, expectedKeys);
    this.verifyScanFull(s, expectedKVs2);

    // Page 2; 2 Results per page (Limit 2, Offset 2)
    s.setFilter(new ColumnPaginationFilter(2,2));
    expectedKeys = 2;
    verifyScan(s, expectedRows, expectedKeys);
    this.verifyScanFull(s, expectedKVs3);

    // Page 8; 20 Results per page (no results) (Limit 20, Offset 140)
    s.setFilter(new ColumnPaginationFilter(20,140));
    expectedKeys = 0;
    expectedRows = 0;
    verifyScan(s, expectedRows, 0);
    this.verifyScanFull(s, expectedKVs4);
  }

  @Test
  public void testNoFilter() throws Exception {
    // No filter
    long expectedRows = numRows;
    long expectedKeys = colsPerRow;

    // Both families
    Scan s = new Scan();
    verifyScan(s, expectedRows, expectedKeys);

    // One family
    s = new Scan();
    s.addFamily(FAMILIES[0]);
    verifyScan(s, expectedRows, expectedKeys/2);
  }

  @Test
  public void testPrefixFilter() throws Exception {
    // Grab rows from group one (half of total)
    long expectedRows = numRows / 2;
    long expectedKeys = colsPerRow;
    Scan s = new Scan();
    s.setFilter(new PrefixFilter(Bytes.toBytes("testRowOne")));
    verifyScan(s, expectedRows, expectedKeys);
  }

  @Test
  public void testColumnCountGetFilter() throws IOException {
      // Test using a filter on a Get
      Get g = new Get(Bytes.toBytes("testRowOne-0"));
      final int count = 2;
      g.setFilter(new ColumnCountGetFilter(count));
      Result res = FilterTable.get(g);
      assertTrue("Result not available", res.list() != null);
      assertEquals(count, res.list().size());
  }

  @Test
  public void testPageFilter() throws Exception {

    // KVs in first 6 rows
    KeyValue [] expectedKVs = {
      // testRowOne-0
      new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
      new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[1], VALUES[0]),
      new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
      new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
      new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[1], VALUES[0]),
      new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[2], VALUES[0]),
      // testRowOne-2
      new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
      new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[1], VALUES[0]),
      new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
      new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
      new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[1], VALUES[0]),
      new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[2], VALUES[0]),
      // testRowOne-3
      new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
      new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[1], VALUES[0]),
      new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
      new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
      new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[1], VALUES[0]),
      new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[2], VALUES[0]),
      // testRowTwo-0
      new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
      new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[1], VALUES[1]),
      new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
      new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
      new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[1], VALUES[1]),
      new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
      // testRowTwo-2
      new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
      new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[1], VALUES[1]),
      new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
      new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
      new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[1], VALUES[1]),
      new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
      // testRowTwo-3
      new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
      new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[1], VALUES[1]),
      new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
      new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
      new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[1], VALUES[1]),
      new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1])
    };

    // Grab all 6 rows
    long expectedRows = 6;
    long expectedKeys = colsPerRow;
    Scan s = new Scan();
    s.setFilter(new PageFilter(expectedRows));
    verifyScan(s, expectedRows, expectedKeys);
    s.setFilter(new PageFilter(expectedRows));
    verifyScanFull(s, expectedKVs);

    // Grab first 4 rows (6 cols per row)
    expectedRows = 4;
    expectedKeys = colsPerRow;
    s = new Scan();
    s.setFilter(new PageFilter(expectedRows));
    verifyScan(s, expectedRows, expectedKeys);
    s.setFilter(new PageFilter(expectedRows));
    verifyScanFull(s, Arrays.copyOf(expectedKVs, 24));

    // Grab first 2 rows
    expectedRows = 2;
    expectedKeys = colsPerRow;
    s = new Scan();
    s.setFilter(new PageFilter(expectedRows));
    verifyScan(s, expectedRows, expectedKeys);
    s.setFilter(new PageFilter(expectedRows));
    verifyScanFull(s, Arrays.copyOf(expectedKVs, 12));

    // Grab first row
    expectedRows = 1;
    expectedKeys = colsPerRow;
    s = new Scan();
    s.setFilter(new PageFilter(expectedRows));
    verifyScan(s, expectedRows, expectedKeys);
    s.setFilter(new PageFilter(expectedRows));
    verifyScanFull(s, Arrays.copyOf(expectedKVs, 6));
  }

  //@Ignore("Will fail w/o dot, need to come back to check again")
  @Test
  public void testWhileMatchFilterWithFilterRow() throws Exception {
    final int pageSize = 4;

    Scan s = new Scan();
    WhileMatchFilter filter = new WhileMatchFilter(new PageFilter(pageSize));
    s.setFilter(filter);
    ResultScanner scanner = FilterTable.getScanner(s);

    int row = 0;
    for (Result result : scanner) {
      row++;
    }
    assertEquals("The page filter returned more rows than expected", pageSize, row);
  }

  @Test
  public void testWhileMatchFilterWithFilterRowKey() throws Exception {
    Scan s = new Scan();
    String prefix = "testRowOne";
    WhileMatchFilter filter = new WhileMatchFilter(new PrefixFilter(Bytes.toBytes(prefix)));
    s.setFilter(filter);

    ResultScanner scanner = FilterTable.getScanner(s);
    int row = 0;
    for (Result result : scanner) {
      row++;
      if (!Bytes.toString(result.list().get(0).getRow()).startsWith(prefix)) {
        assertTrue("The WhileMatchFilter should now filter all remaining", filter.filterAllRemaining());
      }
    }
  }

  @Test
  public void testWhileMatchFilterWithFilterKeyValue() throws Exception {
    Scan s = new Scan();
    WhileMatchFilter filter = new WhileMatchFilter(
        new SingleColumnValueFilter(FAMILIES[0], QUALIFIERS_ONE[0], CompareOp.EQUAL, Bytes.toBytes("foo"))
    );
    s.setFilter(filter);

    ResultScanner scanner = FilterTable.getScanner(s);
    for (Result result : scanner) {
      assertTrue("The WhileMatchFilter should now filter all remaining", filter.filterAllRemaining());
    }
  }

  @Test
  public void testInclusiveStopFilter() throws IOException {

    // Grab rows from group one
    // If we just use start/stop row, we get total/2 - 1 rows
    long expectedRows = (numRows / 2) - 1;
    long expectedKeys = colsPerRow;
    Scan s = new Scan(Bytes.toBytes("testRowOne-0"),
        Bytes.toBytes("testRowOne-3"));
    verifyScan(s, expectedRows, expectedKeys);

    // Now use start row with inclusive stop filter
    expectedRows = numRows / 2;
    s = new Scan(Bytes.toBytes("testRowOne-0"));
    s.setFilter(new InclusiveStopFilter(Bytes.toBytes("testRowOne-3")));
    verifyScan(s, expectedRows, expectedKeys);

    // Grab rows from group two

    // If we just use start/stop row, we get total/2 - 1 rows
    expectedRows = (numRows / 2) - 1;
    expectedKeys = colsPerRow;
    s = new Scan(Bytes.toBytes("testRowTwo-0"), Bytes.toBytes("testRowTwo-3"));
    verifyScan(s, expectedRows, expectedKeys);

    // Now use start row with inclusive stop filter
    expectedRows = numRows / 2;
    s = new Scan(Bytes.toBytes("testRowTwo-0"));
    s.setFilter(new InclusiveStopFilter(Bytes.toBytes("testRowTwo-3")));
    verifyScan(s, expectedRows, expectedKeys);
  }

  @Test
  public void testQualifierFilter() throws IOException {

    // Match two keys (one from each family) in half the rows
    long expectedRows = numRows / 2;
    long expectedKeys = 2;
    Filter f = new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(
        Bytes.toBytes("testQualifierOne.f2")));
    Scan s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match keys less than same qualifier
    // Expect only two keys (one from each family) in half the rows
    expectedRows = numRows / 2;
    expectedKeys = 2;
    f = new QualifierFilter(CompareOp.LESS, new BinaryComparator(
        Bytes.toBytes("testQualifierOne.f2")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match keys less than or equal
    // Expect four keys (two from each family) in half the rows
    expectedRows = numRows / 2;
    expectedKeys = 4;
    f = new QualifierFilter(CompareOp.LESS_OR_EQUAL, new BinaryComparator(
        Bytes.toBytes("testQualifierOne.f2")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match keys not equal
    // Expect four keys (two from each family)
    // Only look in first group of rows
    expectedRows = numRows / 2;
    expectedKeys = 4;
    f = new QualifierFilter(CompareOp.NOT_EQUAL, new BinaryComparator(
        Bytes.toBytes("testQualifierOne.f2")));
    s = new Scan(HConstants.EMPTY_START_ROW, Bytes.toBytes("testRowTwo"));
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match keys greater or equal
    // Expect four keys (two from each family)
    // Only look in first group of rows
    expectedRows = numRows / 2;
    expectedKeys = 4;
    f = new QualifierFilter(CompareOp.GREATER_OR_EQUAL, new BinaryComparator(
        Bytes.toBytes("testQualifierOne.f2")));
    s = new Scan(HConstants.EMPTY_START_ROW, Bytes.toBytes("testRowTwo"));
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match keys greater
    // Expect two keys (one from each family)
    // Only look in first group of rows
    expectedRows = numRows / 2;
    expectedKeys = 2;
    f = new QualifierFilter(CompareOp.GREATER, new BinaryComparator(
        Bytes.toBytes("testQualifierOne.f2")));
    s = new Scan(HConstants.EMPTY_START_ROW, Bytes.toBytes("testRowTwo"));
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match keys not equal to
    // Look across rows and fully validate the keys and ordering
    // Expect varied numbers of keys, 4 per row in group one, 6 per row in group
    // two
    f = new QualifierFilter(CompareOp.NOT_EQUAL, new BinaryComparator(
        QUALIFIERS_ONE[1]));
    s = new Scan();
    s.setFilter(f);

    KeyValue[] kvs = {
        // testRowOne-0
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[2], VALUES[0]),
        // testRowOne-2
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[2], VALUES[0]),
        // testRowOne-3
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[2], VALUES[0]),
        // testRowTwo-0
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[1], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[1], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        // testRowTwo-2
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[1], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[1], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        // testRowTwo-3
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[1], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[1], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]), };
    verifyScanFull(s, kvs);

    // Test across rows and groups with a regex
    // Filter out "test*.2"
    // Expect 4 keys per row across both groups
    f = new QualifierFilter(CompareOp.NOT_EQUAL, new RegexStringComparator(
        "test.+\\.f2"));
    s = new Scan();
    s.setFilter(f);

    kvs = new KeyValue[] {
        // testRowOne-0
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[2], VALUES[0]),
        // testRowOne-2
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[2], VALUES[0]),
        // testRowOne-3
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[2], VALUES[0]),
        // testRowTwo-0
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        // testRowTwo-2
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        // testRowTwo-3
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]), };
    verifyScanFull(s, kvs);

  }

  @Test
  public void testFamilyFilter() throws IOException {

    // Match family, only half of columns returned.
    long expectedRows = numRows;
    long expectedKeys = colsPerRow / 2;
    Filter f = new FamilyFilter(CompareOp.EQUAL,
        new BinaryComparator(Bytes.toBytes("testFamilyOne")));
    Scan s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match keys less than given family, should return nothing
    expectedRows = 0;
    expectedKeys = 0;
    f = new FamilyFilter(CompareOp.LESS,
        new BinaryComparator(Bytes.toBytes("testFamily")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match keys less than or equal, should return half of columns
    expectedRows = numRows;
    expectedKeys = colsPerRow / 2;
    f = new FamilyFilter(CompareOp.LESS_OR_EQUAL,
        new BinaryComparator(Bytes.toBytes("testFamilyOne")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match keys from second family
    // look only in second group of rows
    expectedRows = numRows / 2;
    expectedKeys = colsPerRow / 2;
    f = new FamilyFilter(CompareOp.NOT_EQUAL,
        new BinaryComparator(Bytes.toBytes("testFamilyOne")));
    s = new Scan(HConstants.EMPTY_START_ROW, Bytes.toBytes("testRowTwo"));
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match all columns
    // look only in second group of rows
    expectedRows = numRows / 2;
    expectedKeys = colsPerRow;
    f = new FamilyFilter(CompareOp.GREATER_OR_EQUAL,
        new BinaryComparator(Bytes.toBytes("testFamilyOne")));
    s = new Scan(HConstants.EMPTY_START_ROW, Bytes.toBytes("testRowTwo"));
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match all columns in second family
    // look only in second group of rows
    expectedRows = numRows / 2;
    expectedKeys = colsPerRow / 2;
    f = new FamilyFilter(CompareOp.GREATER,
        new BinaryComparator(Bytes.toBytes("testFamilyOne")));
    s = new Scan(HConstants.EMPTY_START_ROW, Bytes.toBytes("testRowTwo"));
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match keys not equal to given family
    // Look across rows and fully validate the keys and ordering
    f = new FamilyFilter(CompareOp.NOT_EQUAL,
        new BinaryComparator(FAMILIES[1]));
    s = new Scan();
    s.setFilter(f);

    KeyValue [] kvs = {
        // testRowOne-0
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[1], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
        // testRowOne-2
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[1], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
        // testRowOne-3
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[1], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
        // testRowTwo-0
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[1], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        // testRowTwo-2
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[1], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        // testRowTwo-3
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[1], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
    };
    verifyScanFull(s, kvs);

    // Test across rows and groups with a regex
    // Filter out "test*One"
    // Expect 4 keys per row across both groups
    f = new FamilyFilter(CompareOp.NOT_EQUAL,
        new RegexStringComparator("test.*One"));
    s = new Scan();
    s.setFilter(f);

    kvs = new KeyValue [] {
        // testRowOne-0
        new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[1], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[2], VALUES[0]),
        // testRowOne-2
        new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[1], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[2], VALUES[0]),
        // testRowOne-3
        new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[1], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[2], VALUES[0]),
        // testRowTwo-0
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[1], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        // testRowTwo-2
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[1], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        // testRowTwo-3
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[1], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
    };
    verifyScanFull(s, kvs);
  }

  @Test
  public void testRowFilter() throws IOException {

    // Match a single row, all keys
    long expectedRows = 1;
    long expectedKeys = colsPerRow;
    Filter f = new RowFilter(CompareOp.EQUAL,
        new BinaryComparator(Bytes.toBytes("testRowOne-2")));
    Scan s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match a two rows, one from each group, using regex
    expectedRows = 2;
    expectedKeys = colsPerRow;
    f = new RowFilter(CompareOp.EQUAL,
        new RegexStringComparator("testRow.+-2"));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match rows less than
    // Expect all keys in one row
    expectedRows = 1;
    expectedKeys = colsPerRow;
    f = new RowFilter(CompareOp.LESS,
        new BinaryComparator(Bytes.toBytes("testRowOne-2")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match rows less than or equal
    // Expect all keys in two rows
    expectedRows = 2;
    expectedKeys = colsPerRow;
    f = new RowFilter(CompareOp.LESS_OR_EQUAL,
        new BinaryComparator(Bytes.toBytes("testRowOne-2")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match rows not equal
    // Expect all keys in all but one row
    expectedRows = numRows - 1;
    expectedKeys = colsPerRow;
    f = new RowFilter(CompareOp.NOT_EQUAL,
        new BinaryComparator(Bytes.toBytes("testRowOne-2")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match keys greater or equal
    // Expect all keys in all but one row
    expectedRows = numRows - 1;
    expectedKeys = colsPerRow;
    f = new RowFilter(CompareOp.GREATER_OR_EQUAL,
        new BinaryComparator(Bytes.toBytes("testRowOne-2")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match keys greater
    // Expect all keys in all but two rows
    expectedRows = numRows - 2;
    expectedKeys = colsPerRow;
    f = new RowFilter(CompareOp.GREATER,
        new BinaryComparator(Bytes.toBytes("testRowOne-2")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match rows not equal to testRowTwo-2
    // Look across rows and fully validate the keys and ordering
    // Should see all keys in all rows but testRowTwo-2
    f = new RowFilter(CompareOp.NOT_EQUAL,
        new BinaryComparator(Bytes.toBytes("testRowOne-2")));
    s = new Scan();
    s.setFilter(f);

    KeyValue [] kvs = {
        // testRowOne-0
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[1], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[1], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[1], QUALIFIERS_ONE[2], VALUES[0]),
        // testRowOne-3
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[1], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[1], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[1], QUALIFIERS_ONE[2], VALUES[0]),
        // testRowTwo-0
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[1], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[1], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        // testRowTwo-2
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[1], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[1], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        // testRowTwo-3
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[1], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[1], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
    };
    verifyScanFull(s, kvs);


    // Test across rows and groups with a regex
    // Filter out everything that doesn't match "*-2"
    // Expect all keys in two rows
    f = new RowFilter(CompareOp.EQUAL,
        new RegexStringComparator(".+-2"));
    s = new Scan();
    s.setFilter(f);

    kvs = new KeyValue [] {
        // testRowOne-2
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[1], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[1], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[1], QUALIFIERS_ONE[2], VALUES[0]),
        // testRowTwo-2
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[1], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[1], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1])
    };
    verifyScanFull(s, kvs);

  }

  @Test
  public void testValueFilter() throws IOException {

    // Match group one rows
    long expectedRows = numRows / 2;
    long expectedKeys = colsPerRow;
    Filter f = new ValueFilter(CompareOp.EQUAL,
        new BinaryComparator(Bytes.toBytes("testValueOne")));
    Scan s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match group two rows
    expectedRows = numRows / 2;
    expectedKeys = colsPerRow;
    f = new ValueFilter(CompareOp.EQUAL,
        new BinaryComparator(Bytes.toBytes("testValueTwo")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match all values using regex
    expectedRows = numRows;
    expectedKeys = colsPerRow;
    f = new ValueFilter(CompareOp.EQUAL,
        new RegexStringComparator("testValue((One)|(Two))"));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match values less than
    // Expect group one rows
    expectedRows = numRows / 2;
    expectedKeys = colsPerRow;
    f = new ValueFilter(CompareOp.LESS,
        new BinaryComparator(Bytes.toBytes("testValueTwo")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match values less than or equal
    // Expect all rows
    expectedRows = numRows;
    expectedKeys = colsPerRow;
    f = new ValueFilter(CompareOp.LESS_OR_EQUAL,
        new BinaryComparator(Bytes.toBytes("testValueTwo")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match values less than or equal
    // Expect group one rows
    expectedRows = numRows / 2;
    expectedKeys = colsPerRow;
    f = new ValueFilter(CompareOp.LESS_OR_EQUAL,
        new BinaryComparator(Bytes.toBytes("testValueOne")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match values not equal
    // Expect half the rows
    expectedRows = numRows / 2;
    expectedKeys = colsPerRow;
    f = new ValueFilter(CompareOp.NOT_EQUAL,
        new BinaryComparator(Bytes.toBytes("testValueOne")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match values greater or equal
    // Expect all rows
    expectedRows = numRows;
    expectedKeys = colsPerRow;
    f = new ValueFilter(CompareOp.GREATER_OR_EQUAL,
        new BinaryComparator(Bytes.toBytes("testValueOne")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match values greater
    // Expect half rows
    expectedRows = numRows / 2;
    expectedKeys = colsPerRow;
    f = new ValueFilter(CompareOp.GREATER,
        new BinaryComparator(Bytes.toBytes("testValueOne")));
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, expectedRows, expectedKeys);

    // Match values not equal to testValueOne
    // Look across rows and fully validate the keys and ordering
    // Should see all keys in all group two rows
    f = new ValueFilter(CompareOp.NOT_EQUAL,
        new BinaryComparator(Bytes.toBytes("testValueOne")));
    s = new Scan();
    s.setFilter(f);

    KeyValue [] kvs = {
        // testRowTwo-0
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[1], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[1], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        // testRowTwo-2
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[1], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[1], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        // testRowTwo-3
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[1], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[1], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
    };
    verifyScanFull(s, kvs);
  }

  @Test
  public void testSkipFilter() throws IOException {

    // Test for qualifier regex: "testQualifierOne.f2"
    // Should only get rows from second group, and all keys
    Filter f = new SkipFilter(new QualifierFilter(CompareOp.NOT_EQUAL,
        new BinaryComparator(Bytes.toBytes("testQualifierOne.f2"))));
    Scan s = new Scan();
    s.setFilter(f);

    KeyValue [] kvs = {
        // testRowTwo-0
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[1], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[1], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        // testRowTwo-2
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[1], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[1], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
        // testRowTwo-3
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[1], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[1], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[1], QUALIFIERS_TWO[2], VALUES[1]),
    };
    verifyScanFull(s, kvs);
  }

  @Test
  public void testFilterList() throws IOException {

    // Test getting a single row, single key using Row, Qualifier, and Value
    // regular expression and substring filters
    // Use must pass all
    List<Filter> filters = new ArrayList<Filter>();
    filters.add(new RowFilter(CompareOp.EQUAL, new RegexStringComparator(".+-2")));
    filters.add(new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator(".+\\.f2")));
    filters.add(new ValueFilter(CompareOp.EQUAL, new SubstringComparator("One")));
    Filter f = new FilterList(Operator.MUST_PASS_ALL, filters);
    Scan s = new Scan();
    s.addFamily(FAMILIES[0]);
    s.setFilter(f);
    KeyValue [] kvs = {
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[1], VALUES[0])
    };
    verifyScanFull(s, kvs);

    // Test getting everything with a MUST_PASS_ONE filter including row, qf, val
    // regular expression and substring filters
    filters.clear();
    filters.add(new RowFilter(CompareOp.EQUAL, new RegexStringComparator(".+Two.+")));
    filters.add(new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator(".+\\.f2")));
    filters.add(new ValueFilter(CompareOp.EQUAL, new SubstringComparator("One")));
    f = new FilterList(Operator.MUST_PASS_ONE, filters);
    s = new Scan();
    s.setFilter(f);
    verifyScanNoEarlyOut(s, numRows, colsPerRow);

  }

  @Test
  public void testFirstKeyOnlyFilter() throws IOException {
    Scan s = new Scan();
    s.setFilter(new FirstKeyOnlyFilter());
    // Expected KVs, the first KV from each of the remaining 6 rows
    KeyValue [] kvs = {
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1])
    };
    verifyScanFull(s, kvs);
  }

  @Test
  public void testFilterListWithSingleColumnValueFilter() throws IOException {
    // Test for HBASE-3191

    // Scan using SingleColumnValueFilter
    SingleColumnValueFilter f1 = new SingleColumnValueFilter(FAMILIES[0], QUALIFIERS_ONE[0],
          CompareOp.EQUAL, VALUES[0]);
    f1.setFilterIfMissing( true );
    Scan s1 = new Scan();
    s1.addFamily(FAMILIES[0]);
    s1.setFilter(f1);
    KeyValue [] kvs1 = {
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[1], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[1], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[1], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
    };
    verifyScanNoEarlyOut(s1, 3, 3);
    verifyScanFull(s1, kvs1);

    // Scan using another SingleColumnValueFilter, expect disjoint result
    SingleColumnValueFilter f2 = new SingleColumnValueFilter(FAMILIES[0], QUALIFIERS_TWO[0],
        CompareOp.EQUAL, VALUES[1]);
    f2.setFilterIfMissing( true );
    Scan s2 = new Scan();
    s2.addFamily(FAMILIES[0]);
    s2.setFilter(f2);
    KeyValue [] kvs2 = {
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[1], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[1], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[1], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
    };
    verifyScanNoEarlyOut(s2, 3, 3);
    verifyScanFull(s2, kvs2);

    // Scan, ORing the two previous filters, expect unified result
    FilterList f = new FilterList(Operator.MUST_PASS_ONE);
    f.addFilter(f1);
    f.addFilter(f2);
    Scan s = new Scan();
    s.addFamily(FAMILIES[0]);
    s.setFilter(f);
    KeyValue [] kvs = {
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[1], VALUES[0]),
        new KeyValue(ROWS_ONE[0], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[1], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[1], VALUES[0]),
        new KeyValue(ROWS_ONE[3], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[1], VALUES[1]),
        new KeyValue(ROWS_TWO[0], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[1], VALUES[1]),
        new KeyValue(ROWS_TWO[2], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[0], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[1], VALUES[1]),
        new KeyValue(ROWS_TWO[3], FAMILIES[0], QUALIFIERS_TWO[2], VALUES[1]),
    };
    verifyScanNoEarlyOut(s, 6, 3);
    verifyScanFull(s, kvs);
  }

  @Test
  public void testSingleColumnValueFilter() throws IOException {

    // we will modify table in this case, need to reset after case is done.
    needToResetTable = true;

    // From HBASE-1821
    // Desired action is to combine two SCVF in a FilterList
    // Want to return only rows that match both conditions

    // Need to change one of the group one columns to use group two value
    Put p = new Put(ROWS_ONE[2]);
    p.add(FAMILIES[0], QUALIFIERS_ONE[1], VALUES[1]);
    p.add(FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]);
    p.add(FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0]);
    FilterTable.put(p);

    // Now let's grab rows that have Q_ONE[0](VALUES[0]) and Q_ONE[2](VALUES[1])
    // Since group two rows don't have these qualifiers, they will pass
    // so limiting scan to group one
    List<Filter> filters = new ArrayList<Filter>();
    filters.add(new SingleColumnValueFilter(FAMILIES[0], QUALIFIERS_ONE[0],
        CompareOp.EQUAL, VALUES[0]));
    filters.add(new SingleColumnValueFilter(FAMILIES[0], QUALIFIERS_ONE[1],
        CompareOp.EQUAL, VALUES[1]));
    Filter f = new FilterList(Operator.MUST_PASS_ALL, filters);
    Scan s = new Scan(ROWS_ONE[0], ROWS_TWO[0]);
    s.addFamily(FAMILIES[0]);
    s.setFilter(f);
    // Expect only one row, all qualifiers
    KeyValue [] kvs = {
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[1], VALUES[1]),
        new KeyValue(ROWS_ONE[2], FAMILIES[0], QUALIFIERS_ONE[2], VALUES[0])
    };
    verifyScanNoEarlyOut(s, 1, 3);
    verifyScanFull(s, kvs);

    // In order to get expected behavior without limiting to group one
    // need to wrap SCVFs in SkipFilters
    filters = new ArrayList<Filter>();
    filters.add(new SkipFilter(new SingleColumnValueFilter(FAMILIES[0], QUALIFIERS_ONE[0],
        CompareOp.EQUAL, VALUES[0])));
    filters.add(new SkipFilter(new SingleColumnValueFilter(FAMILIES[0], QUALIFIERS_ONE[1],
        CompareOp.EQUAL, VALUES[1])));
    f = new FilterList(Operator.MUST_PASS_ALL, filters);
    s = new Scan(ROWS_ONE[0], ROWS_TWO[0]);
    s.addFamily(FAMILIES[0]);
    s.setFilter(f);
    // Expect same KVs
    verifyScanNoEarlyOut(s, 1, 3);
    verifyScanFull(s, kvs);

    // More tests from HBASE-1821 for Clint and filterIfMissing flag

    byte [][] ROWS_THREE = {
        Bytes.toBytes("rowThree-0"), Bytes.toBytes("rowThree-1"),
        Bytes.toBytes("rowThree-2"), Bytes.toBytes("rowThree-3")
    };

    // Give row 0 and 2 QUALIFIERS_ONE[0] (VALUE[0] VALUE[1])
    // Give row 1 and 3 QUALIFIERS_ONE[1] (VALUE[0] VALUE[1])

    KeyValue [] srcKVs = new KeyValue [] {
        new KeyValue(ROWS_THREE[0], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[0]),
        new KeyValue(ROWS_THREE[1], FAMILIES[0], QUALIFIERS_ONE[0], VALUES[1]),
        new KeyValue(ROWS_THREE[2], FAMILIES[0], QUALIFIERS_ONE[1], VALUES[0]),
        new KeyValue(ROWS_THREE[3], FAMILIES[0], QUALIFIERS_ONE[1], VALUES[1])
    };

    for(KeyValue kv : srcKVs) {
      Put put = new Put(kv.getRow()).add(kv);
      put.setWriteToWAL(false);
      FilterTable.put(put);
    }

    // Match VALUES[0] against QUALIFIERS_ONE[0] with filterIfMissing = false
    // Expect 3 rows (0, 2, 3)
    SingleColumnValueFilter scvf = new SingleColumnValueFilter(FAMILIES[0],
        QUALIFIERS_ONE[0], CompareOp.EQUAL, VALUES[0]);
    s = new Scan(ROWS_THREE[0], Bytes.toBytes("rowThree-4"));
    s.addFamily(FAMILIES[0]);
    s.setFilter(scvf);
    kvs = new KeyValue [] { srcKVs[0], srcKVs[2], srcKVs[3] };
    verifyScanFull(s, kvs);

    // Match VALUES[0] against QUALIFIERS_ONE[0] with filterIfMissing = true
    // Expect 1 row (0)
    scvf = new SingleColumnValueFilter(FAMILIES[0], QUALIFIERS_ONE[0],
        CompareOp.EQUAL, VALUES[0]);
    scvf.setFilterIfMissing(true);
    s = new Scan(ROWS_THREE[0], Bytes.toBytes("rowThree-4"));
    s.addFamily(FAMILIES[0]);
    s.setFilter(scvf);
    kvs = new KeyValue [] { srcKVs[0] };
    verifyScanFull(s, kvs);

    // Match VALUES[1] against QUALIFIERS_ONE[1] with filterIfMissing = true
    // Expect 1 row (3)
    scvf = new SingleColumnValueFilter(FAMILIES[0],
        QUALIFIERS_ONE[1], CompareOp.EQUAL, VALUES[1]);
    scvf.setFilterIfMissing(true);
    s = new Scan(ROWS_THREE[0], Bytes.toBytes("rowThree-4"));
    s.addFamily(FAMILIES[0]);
    s.setFilter(scvf);
    kvs = new KeyValue [] { srcKVs[3] };
    verifyScanFull(s, kvs);

    // Add QUALIFIERS_ONE[1] to ROWS_THREE[0] with VALUES[0]
    KeyValue kvA = new KeyValue(ROWS_THREE[0], FAMILIES[0], QUALIFIERS_ONE[1], VALUES[0]);
    FilterTable.put(new Put(kvA.getRow()).add(kvA));

    // Match VALUES[1] against QUALIFIERS_ONE[1] with filterIfMissing = true
    // Expect 1 row (3)
    scvf = new SingleColumnValueFilter(FAMILIES[0],
        QUALIFIERS_ONE[1], CompareOp.EQUAL, VALUES[1]);
    scvf.setFilterIfMissing(true);
    s = new Scan(ROWS_THREE[0], Bytes.toBytes("rowThree-4"));
    s.addFamily(FAMILIES[0]);
    s.setFilter(scvf);
    kvs = new KeyValue [] { srcKVs[3] };
    verifyScanFull(s, kvs);

  }

  @Test
  public void testKeyOnlyFilter() throws Exception {

    byte [] ROW = Bytes.toBytes("testRow");
    byte [] VALUE = Bytes.toBytes("testValue");

    String tableName = "testKeyOnlyFilter";
    String family = "testFamily";
    byte [][] QUALIFIERS = {
        Bytes.toBytes("d1.col0-<d2v1>-<d3v2>"), Bytes.toBytes("d2.col1-<d2v1>-<d3v2>"),
        Bytes.toBytes("d1.col2-<d2v1>-<d3v2>"), Bytes.toBytes("d2.col3-<d2v1>-<d3v2>"),
        Bytes.toBytes("d1.col4-<d2v1>-<d3v2>"), Bytes.toBytes("d2.col5-<d2v1>-<d3v2>"),
        Bytes.toBytes("d1.col6-<d2v1>-<d3v2>"), Bytes.toBytes("d2.col7-<d2v1>-<d3v2>"),
        Bytes.toBytes("d1.col8-<d2v1>-<d3v2>"), Bytes.toBytes("d2.col9-<d2v1>-<d3v2>")
    };

    Map<String, List<String>> layouts = new HashMap<String, List<String>>();
    List<String> columns = new ArrayList<String>();

    for (byte[] q : QUALIFIERS) {
      columns.add(Bytes.toString(q));
    }

    layouts.put(family, columns);
    createDotTable(tableName, layouts);
    //createNormalTable(tableName, layouts);

    HTable table = null;
    try {
      table = new HTable(conf, tableName);
    } catch (IOException e) {
      assertNull("Failed to create table", e);
    }

    try {
      byte [][] ROWS = makeN(ROW, 10);

      for(int i=0;i<10;i++) {
        Put put = new Put(ROWS[i]);
        put.setWriteToWAL(false);
        put.add(Bytes.toBytes(family), QUALIFIERS[i], VALUE);
        table.put(put);
      }
      Scan scan = new Scan();
      scan.addFamily(Bytes.toBytes(family));
      Filter filter = new KeyOnlyFilter(true);
      scan.setFilter(filter);
      ResultScanner scanner = table.getScanner(scan);
      int count = 0;
      for(Result result : table.getScanner(scan)) {
        assertEquals(result.size(), 1);
        assertEquals(result.raw()[0].getValueLength(), Bytes.SIZEOF_INT);
        assertEquals(Bytes.toInt(result.raw()[0].getValue()), VALUE.length);
        count++;
      }
      assertEquals(count, 10);
      scanner.close();
    } catch (Exception e) {
      assertNull("Exceptions :" + e.getMessage(), e);
    } finally {
      table.close();
      deleteTable(tableName);
    }
  }

  @Test
  public void TestColumnRangeFilterClient() throws Exception {

    String family = "Family";
    String tableName = "TestColumnRangeFilterClient";

    List<String> rows = generateRandomWords(10, 8, null);
    List<String> columns = generateRandomWords(2000, 8, "d.");

    Map<String, List<String>> layouts = new HashMap<String, List<String>>();
    layouts.put(family, columns);
    createDotTable(tableName, layouts);
    //createNormalTable(tableName, layouts);

    HTable ht = null;
    try {
      ht = new HTable(conf, tableName);
    } catch (IOException e) {
      assertNull("Failed to create table", e);
    }

    List<KeyValue> kvList = new ArrayList<KeyValue>();

    Map<StringRange, List<KeyValue>> rangeMap = new HashMap<StringRange, List<KeyValue>>();

    rangeMap.put(new StringRange(null, true, "b", false),
        new ArrayList<KeyValue>());
    rangeMap.put(new StringRange("p", true, "q", false),
        new ArrayList<KeyValue>());
    rangeMap.put(new StringRange("r", false, "s", true),
        new ArrayList<KeyValue>());
    rangeMap.put(new StringRange("z", false, null, false),
        new ArrayList<KeyValue>());
    String valueString = "ValueString";

    for (String row : rows) {
      Put p = new Put(Bytes.toBytes(row));
      p.setWriteToWAL(false);
      for (String column : columns) {
        KeyValue kv = KeyValueTestUtil.create(row, family, column,
            HConstants.LATEST_TIMESTAMP, valueString);
        p.add(kv);
        kvList.add(kv);
        for (StringRange s : rangeMap.keySet()) {
          if (s.inRange(column)) {
            rangeMap.get(s).add(kv);
          }
        }
      }
      ht.put(p);
    }

    TEST_UTIL.flush();

    ColumnRangeFilter filter;
    Scan scan = new Scan();
    scan.setMaxVersions();
    for (StringRange s : rangeMap.keySet()) {
      filter = new ColumnRangeFilter(s.getStart() == null ? null
          : Bytes.toBytes(s.getStart()), s.isStartInclusive(),
          s.getEnd() == null ? null : Bytes.toBytes(s.getEnd()),
          s.isEndInclusive());
      scan.setFilter(filter);
      ResultScanner scanner = ht.getScanner(scan);
      List<KeyValue> results = new ArrayList<KeyValue>();
      LOG.info("scan column range: " + s.toString());
      long timeBeforeScan = System.currentTimeMillis();

      Result result;
      while ((result = scanner.next()) != null) {
        for (KeyValue kv : result.list()) {
          results.add(kv);
        }
      }
      long scanTime = System.currentTimeMillis() - timeBeforeScan;
      scanner.close();
      LOG.info("scan time = " + scanTime + "ms");
      LOG.info("found " + results.size() + " results");
      LOG.info("Expecting " + rangeMap.get(s).size() + " results");

      assertEquals(rangeMap.get(s).size(), results.size());
    }
    ht.close();
  }

}
