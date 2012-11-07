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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;


public class GenerateTestTable {

  static final Log LOG = LogFactory.getLog(GenerateTestTable.class);

  private HBaseAdmin admin = null;
  private final int BATCHNUM = 10000;

  private final int MAX_COLUMN_NUM = 100;
  private final int DEFAULT_COLUMN_NUM = 5;
  private final int MAX_FAMILY_NUM = 10;
  private final int DEFAULT_FAMILY_NUM = 1;
  private final long MAX_ROW_NUM = 10000000000L;
  private final long DEFAULT_ROW_NUM = 10000;
  private final int MAX_REGION_NUM = 2048;
  private final int DEFAULT_REGION_NUMBER = 96;

  private final String[] rowPrefix = {
      "a","b","c","d","e","f","g","h","i","j","k","l","m",
      "n","o","p","q","r","s","t","u","v","w","x","y","z"
  };
  
  private Configuration conf = null;
  private byte[][] tableSplits = null;
  
  private int colNum = this.DEFAULT_COLUMN_NUM;
  private long rowNum = this.DEFAULT_ROW_NUM;
  private int cfNum = this.DEFAULT_FAMILY_NUM;
  private int regionNumber = this.DEFAULT_REGION_NUMBER;
  private long baseRowNumber = 1L;
  private String tableName = null;

  
  /**
   * Constructor
   * 
   * @param c
   *          Configuration object
   */
  public GenerateTestTable() {

  }

  private void init(){
    this.conf = HBaseConfiguration.create();
    try {
      this.admin = new HBaseAdmin(this.conf);
    } catch (MasterNotRunningException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (ZooKeeperConnectionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  private void createNormalTable(String tableName,
      Map<String, List<String>> layouts, byte[][] splits) {

    HTableDescriptor htd = new HTableDescriptor(tableName);
    for (Map.Entry<String, List<String>> cfLayout : layouts.entrySet()) {
      String family = cfLayout.getKey();
      HColumnDescriptor cfdesc = new HColumnDescriptor(family);
      htd.addFamily(cfdesc);
    }

    try {
      if (splits == null) {
        admin.createTable(htd);
      } else {
        admin.createTable(htd, splits);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void createDotTable(String tableName,
      Map<String, List<String>> layouts, byte[][] splits) {

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
      if (splits == null) {
        admin.createTable(htd);
      } else {
        admin.createTable(htd, splits);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void deleteTable(String tableName) {
    try {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    } catch (IOException e) {
      assertNull("Failed to delete table", e);
    }
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

  public void createTable() throws Exception {

    List<String> familys = new ArrayList<String>();
    List<String> columns = new ArrayList<String>();

    for (int i = 0; i < this.cfNum; i++) {
      familys.add("cf" + Integer.toString(i));
    }
    
    for (int i = 0; i < this.colNum; i++) {
      columns.add("d.f" + Integer.toString(i));
    }

    Map<String, List<String>> layouts = new HashMap<String, List<String>>();
    
    for (String family : familys) {
      layouts.put(family, columns);
    }
    
    tableSplits = getFourLetterSplits(this.regionNumber);

    String DotTableName  =this.tableName + "Dot";
    createDotTable(DotTableName, layouts, tableSplits);
    createNormalTable(tableName, layouts, tableSplits);

    HTable htDot = null;
    HTable ht = null;
    try {
      htDot = new HTable(conf, DotTableName);
      ht = new HTable(conf, tableName);
    } catch (IOException e) {
      assertNull("Failed to create table", e);
    }

    long remainRows = this.rowNum;
    long index = 0;
    int toProcess;
    String row = null;

    while (remainRows > 0) {
      toProcess = this.BATCHNUM;
      if (toProcess > remainRows)
        toProcess = (int)remainRows;

      List<Put> putList = new ArrayList<Put>(toProcess);

      for (long i = 0; i < toProcess; i++){
        row = rowPrefix[(int)((index / (26*26*26)) % 26)]
            + rowPrefix[(int)((index / (26*26)) % 26)]
            + rowPrefix[(int)((index / 26) % 26)] 
            + rowPrefix[(int)(index % 26)]
            + Long.toString(this.baseRowNumber + index);
        
        Put p = new Put(Bytes.toBytes(row));
        p.setWriteToWAL(false);
        for (String family : familys) {
          for (String column : columns) {
            KeyValue kv = KeyValueTestUtil.create(row, family, column,
                HConstants.LATEST_TIMESTAMP, "v" + "-" + column + "-" + row);
            p.add(kv);
          }
        }
        putList.add(p);
        index++;
      }

      ht.put(putList);
      htDot.put(putList);
      remainRows -= toProcess;
    }
    ht.close();
    htDot.close();
  }

  protected void printUsage() {
    printUsage(null);
  }

  protected void printUsage(final String message) {
    if (message != null && message.length() > 0) {
      System.err.println(message);
    }
    System.err.println("Usage: java " + this.getClass().getName());
    System.err.println("--table=tablename [--rownum=] [--colnum=] [--cfnum=] [--regions=]");
    System.err.println();
  }
  
  
  private byte[][] getLetterSplits(int n) {
    assert(n > 0 && n < 26);
    byte[][] splits = new byte[n-1][];

    double step = 26.0 / n;
    double offset = 0.0;
    long index;
    char[] letter = new char[1];
    for (int i = 0; i < (n-1); i++) {
      offset += step;
      index = Math.round(offset);
      letter[0] = (char) (index + 97);
      splits[i] = Bytes.toBytes(new String(letter));
    }
    return splits;
  }

  private byte[][] getTwoLetterSplits(int n) {
    double range = 26.0 * 26.0;
    assert(n > 0 && n < range);
    byte[][] splits = new byte[n-1][];

    double step = range / n;
    double offset = 0.0;
    long index;
    char[] letter = new char[2];
    for (int i = 0; i < (n-1); i++) {
      offset += step;
      index = Math.round(offset);
      letter[0] = (char) ((index / 26) + 97);
      letter[1] = (char) ((index % 26) + 97);
      splits[i] = Bytes.toBytes(new String(letter));
    }
    return splits;
  }

  private byte[][] getFourLetterSplits(int n) {
    double range = 26.0 * 26.0 * 26.0 * 26.0;
    assert(n > 0 && n < MAX_REGION_NUM);
    byte[][] splits = new byte[n-1][];

    double step = range / n;
    double offset = 0.0;
    long index;
    char[] letter = new char[4];
    for (int i = 0; i < (n-1); i++) {
      offset += step;
      index = Math.round(offset);
      letter[0] = (char) ((index / (26*26*26)) + 97);
      letter[1] = (char) ((index / (26*26) % 26) + 97);
      letter[2] = (char) ((index / (26) % 26) + 97);
      letter[3] = (char) ((index % 26) + 97);
      splits[i] = Bytes.toBytes(new String(letter));
    }
    return splits;
  }

  public int parseCommandLine(final String[] args) {
    // (but hopefully something not as painful as cli options).
    int errCode = 0;
    if (args.length < 1) {
      printUsage();
      return -1;
    }

    for (int i = 0; i < args.length; i++) {
      String cmd = args[i];
      if (cmd.equals("-h") || cmd.startsWith("--h")) {
        printUsage();
        break;
      }

      final String colnum = "--colnum=";
      if (cmd.startsWith(colnum)) {
        int val = Integer.parseInt(cmd.substring(colnum.length()));
        if (val <= 0 || val > this.MAX_COLUMN_NUM)
          val = this.DEFAULT_COLUMN_NUM;
        this.colNum = val;
        continue;
      }

      final String cf = "--cfnum=";
      if (cmd.startsWith(cf)) {
        int val = Integer.parseInt(cmd.substring(cf.length()));
        if (val <= 0 || val > this.MAX_FAMILY_NUM)
          val = this.DEFAULT_FAMILY_NUM;
        this.cfNum = val;
        continue;
      }
      
      final String rows = "--rownum=";
      if (cmd.startsWith(rows)) {
        long val = Long.decode(cmd.substring(rows.length()));
        if (val <= 0 || val > this.MAX_ROW_NUM)
          val = this.DEFAULT_ROW_NUM;
        this.rowNum = val;        
        continue;
      }

      final String regions = "--regions=";
      if (cmd.startsWith(regions)) {
        int val = Integer.parseInt(cmd.substring(regions.length()));
        if (val <= 0 || val > this.MAX_REGION_NUM)
          val = this.DEFAULT_REGION_NUMBER;
        this.regionNumber = val;    
        continue;
      }

      final String table = "--table=";
      if (cmd.startsWith(table)) {
        this.tableName = cmd.substring(table.length());
        continue;
      }
    }

    if (this.tableName == null) {
        printUsage("Please specify the table name");
        errCode = -2;
    }

    this.baseRowNumber = 1L;
    
    while(this.baseRowNumber < this.rowNum) {
      this.baseRowNumber *= 10L;
    }
   
    System.out.println("cfnum = " + this.cfNum);
    System.out.println("colnum = " + this.colNum);
    System.out.println("rownum = " + this.rowNum);
    System.out.println("baseRowNumber = " + this.baseRowNumber);
    System.out.println("tablename = " + this.tableName);
    System.out.println("Presplit Region number = " + this.regionNumber);
    return errCode;
  }

  /**
   * do tmp test here.
   * @throws IOException
   */
  @Test
  public void test() throws IOException {
    String[] fakeArgs = {
        "--cfnum=5",
        "--colnum=10",
        "--rownum=10000",
        "--table=testDataTable",
        "--regions=32"
    };

    parseCommandLine(fakeArgs);
    byte[][] splits = getLetterSplits(12);
    byte[][] splits2 = getTwoLetterSplits(64);
  }

  /**
   * @param args
   * @throws Exception 
   */
  public static void main(final String[] args) throws Exception {
    GenerateTestTable gt = new GenerateTestTable();
    if (gt.parseCommandLine(args) != 0) {
      System.err.println("fail to parse cmdline");
      return;
    }
    gt.init();
    gt.createTable();
  }
}
