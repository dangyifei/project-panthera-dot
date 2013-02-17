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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.StringUtils;

class KEY {
  public static final String INPUT_TABLE = TableInputFormat.INPUT_TABLE;
  public static final String OPTION_CFNUM = "test_key_cfnum";
  public static final String OPTION_COLNUM = "test_key_colnum";
  public static String OPTION_ROWNUM = "test_key_rownum";
  public static String OPTION_BASEROWNUM = "test_key_baserownum";
  public static String OPTION_REGIONROWNUM = "test_key_regionrownum";
  public static long BATCHNUM = 1000;
}

class RegionWriteInputFormat extends
    InputFormat<String, Long> {

  public static class RegionWriteRecordReader extends
      RecordReader<String, Long> {
    private TableSplit value = null;
    private Configuration conf;
    private long rowNum;
    private long index = 0L;
    private long baseRowNumber = 1L;
    private String regionPrefix = null;
    private boolean currentValueRead = false;

    public RegionWriteRecordReader(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
      initialize(split, context);
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
      this.value = (TableSplit) split;
      conf = context.getConfiguration();
      this.rowNum = conf.getLong(KEY.OPTION_REGIONROWNUM, 100000L);
      this.baseRowNumber = conf.getLong(KEY.OPTION_BASEROWNUM , 1L);
      this.index = 0L;
      
      byte[] srow = value.getStartRow();
      if (srow.length == 0) {
        // this is the first region, we use "aaaa" for prefix.
        regionPrefix = "aaaa";        
      } else {
        regionPrefix = Bytes.toString(srow);
      }
      
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (index >= rowNum) {
        return false;
      }
      if (currentValueRead == true) {
        index += KEY.BATCHNUM;
        currentValueRead = false;
      }
      return true;
    }

    @Override
    public String getCurrentKey() throws IOException,
        InterruptedException {
      String s = regionPrefix + Long.toString((this.baseRowNumber + index) / KEY.BATCHNUM);
      return s;
    }

    @Override
    public Long getCurrentValue() throws IOException,
        InterruptedException {
      currentValueRead = true;
      if ((rowNum - index) > KEY.BATCHNUM)
        return KEY.BATCHNUM;
      return rowNum - index;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return index * 1.0f / rowNum;
    }

    @Override
    public void close() throws IOException {
    }
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException,
      InterruptedException {
    TableInputFormat tif = new TableInputFormat();
    tif.setConf(context.getConfiguration());
    return tif.getSplits(context);
  }

  @Override
  public RecordReader<String, Long> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {
    return new RegionWriteRecordReader(split, context);
  }
}

class GenerateRegionDataTask extends
    Mapper<String, Long, LongWritable, LongWritable> {

  private int colNum;
  private int cfNum = 1;
  private String tableName = null;
  private HTable ht = null;
  
  private Configuration conf;

  @Override
  protected void setup(Context context) throws IOException,
      InterruptedException {

    conf = context.getConfiguration();

    this.cfNum = conf.getInt(KEY.OPTION_CFNUM, 1);
    this.colNum = conf.getInt(KEY.OPTION_COLNUM, 18);
    this.tableName = conf.get(KEY.INPUT_TABLE);
    
    try {
      ht = new HTable(conf, tableName);
    } catch (IOException e) {
      assertNull("Failed to create table", e);
    }
    
  }

  @Override
  protected void cleanup(Context context) throws IOException,
      InterruptedException {
//    context.getCounter(CounterType.ROWS).increment(this.cmd.rows.get());
//    context.getCounter(CounterType.KVS).increment(this.cmd.kvs.get());
    if (ht != null)
      ht.close();
  }

  @Override
  protected void map(String prefix, Long rows, final Context context)
      throws IOException, InterruptedException {

    //long startTime = System.currentTimeMillis();
    // region write task
    try {
        doWrite(prefix, rows, context);        
    } catch (Exception e) {
      e.printStackTrace();
      throw new IOException(e);
    }

    //long elapsedTime = System.currentTimeMillis() - startTime;

    context.progress();
  }
  
  private void doWrite(String rowPrefix, Long rows, final Context context) throws IOException {

    List<String> familys = new ArrayList<String>();
    List<String> columns = new ArrayList<String>();

    for (int i = 0; i < this.cfNum; i++) {
      familys.add("cf" + Integer.toString(i));
    }
    
    for (int i = 0; i < this.colNum; i++) {
      columns.add("d.f" + Integer.toString(i));
    }

    long remainRows = rows;
    long index = 0;
    int toProcess;
    String row = null;

    while (remainRows > 0) {
      toProcess = (int) KEY.BATCHNUM;
      if (toProcess > remainRows)
        toProcess = (int) remainRows;

      List<Put> putList = new ArrayList<Put>(toProcess);

      for (long i = 0; i < toProcess; i++) {
        row = rowPrefix + Long.toString(index);

        Put p = new Put(Bytes.toBytes(row));
        p.setWriteToWAL(false);
        for (String family : familys) {
          for (String column : columns) {
            
            KeyValue kv = new KeyValue(
                Bytes.toBytes(row),
                Bytes.toBytes(family),
                Bytes.toBytes(column),
                HConstants.LATEST_TIMESTAMP,
                KeyValue.Type.Put,
                Bytes.toBytes("v" + "-" + column + "-" + row));
                
            //KeyValue kv = KeyValueTestUtil.create(row, family, column,
            //    HConstants.LATEST_TIMESTAMP, "v" + "-" + column + "-" + row);
            p.add(kv);
          }
        }
        putList.add(p);
        index++;
      }

      ht.put(putList);
      remainRows -= toProcess;
    }
  }
}


public class GenerateTestTable {

  static final Log LOG = LogFactory.getLog(GenerateTestTable.class);

  private HBaseAdmin admin = null;

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
  private boolean createDotTable = false;

  private String dotTableName;

  
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

  private void doMapReduce(
      Class<? extends InputFormat> inputFormatClass,
      Class<? extends Mapper> mapperClass,
      String mrTableName) throws IOException,
      ClassNotFoundException, InterruptedException {

    this.conf.set(KEY.INPUT_TABLE, mrTableName);
    Job job = new Job(this.conf);
    job.setJobName("Generate Data for [" + mrTableName + "]");
    job.setJarByClass(GenerateTestTable.class);

    job.setInputFormatClass(inputFormatClass);

    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(LongWritable.class);
    
    FileSystem fs = FileSystem.get(conf);
    Path path = new Path("/tmp", "tempout");
    fs.delete(path, true);

    FileOutputFormat.setOutputPath(job, path);

    job.setMapperClass(mapperClass);
    job.setNumReduceTasks(0);

    TableMapReduceUtil.addDependencyJars(job);
    // Add a Class from the hbase.jar so it gets registered too.
    TableMapReduceUtil.addDependencyJars(job.getConfiguration(),
        org.apache.hadoop.hbase.util.Bytes.class);

    TableMapReduceUtil.initCredentials(job);

    job.waitForCompletion(true);

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

    this.dotTableName  =this.tableName + "Dot";
    if (this.createDotTable)
      createDotTable(this.dotTableName, layouts, tableSplits);
    createNormalTable(tableName, layouts, tableSplits);

//    HTable htDot = null;
//    HTable ht = null;
//    try {
//      if (this.createDotTable)
//        htDot = new HTable(conf, DotTableName);
//      ht = new HTable(conf, tableName);
//    } catch (IOException e) {
//      assertNull("Failed to create table", e);
//    }
//
//    long remainRows = this.rowNum;
//    long index = 0;
//    int toProcess;
//    String row = null;
//
//    while (remainRows > 0) {
//      toProcess = this.BATCHNUM;
//      if (toProcess > remainRows)
//        toProcess = (int)remainRows;
//
//      List<Put> putList = new ArrayList<Put>(toProcess);

//      for (long i = 0; i < toProcess; i++){
//        row = rowPrefix[(int)((index / (26*26*26)) % 26)]
//            + rowPrefix[(int)((index / (26*26)) % 26)]
//            + rowPrefix[(int)((index / 26) % 26)] 
//            + rowPrefix[(int)(index % 26)]
//            + Long.toString(this.baseRowNumber + index);
//        
//        Put p = new Put(Bytes.toBytes(row));
//        p.setWriteToWAL(false);
//        for (String family : familys) {
//          for (String column : columns) {
//            KeyValue kv = KeyValueTestUtil.create(row, family, column,
//                HConstants.LATEST_TIMESTAMP, "v" + "-" + column + "-" + row);
//            p.add(kv);
//          }
//        }
//        putList.add(p);
//        index++;
//      }
//
//      ht.put(putList);
//      if (this.createDotTable)
//        htDot.put(putList);
//      remainRows -= toProcess;
//    }
//    ht.close();
//    if (this.createDotTable)
//      htDot.close();
  }

  protected void printUsage() {
    printUsage(null);
  }

  protected void printUsage(final String message) {
    if (message != null && message.length() > 0) {
      System.err.println(message);
    }
    System.err.println("Usage: java " + this.getClass().getName());
    System.err.println("--table=tablename [--rownum=] [--colnum=] [--cfnum=] [--regions=] [--enabledot]");
    System.err.println();
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
        this.conf.setLong(KEY.OPTION_COLNUM, this.colNum);
        continue;
      }

      final String cf = "--cfnum=";
      if (cmd.startsWith(cf)) {
        int val = Integer.parseInt(cmd.substring(cf.length()));
        if (val <= 0 || val > this.MAX_FAMILY_NUM)
          val = this.DEFAULT_FAMILY_NUM;
        this.cfNum = val;
        this.conf.setInt(KEY.OPTION_CFNUM, this.cfNum);
        continue;
      }
      
      final String rows = "--rownum=";
      if (cmd.startsWith(rows)) {
        long val = Long.decode(cmd.substring(rows.length()));
        if (val < 0 || val > this.MAX_ROW_NUM)
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
      
      final String enabledot = "--enabledot";
      if (cmd.startsWith(enabledot)) {
        this.createDotTable  = true;    
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

    this.conf.setLong(KEY.OPTION_BASEROWNUM, this.baseRowNumber);
    this.conf.setLong(KEY.OPTION_REGIONROWNUM , this.rowNum / this.regionNumber);

    System.out.println("cfnum = " + this.cfNum);
    System.out.println("colnum = " + this.colNum);
    System.out.println("rownum = " + this.rowNum);
    System.out.println("baseRowNumber = " + this.baseRowNumber);
    System.out.println("tablename = " + this.tableName);
    System.out.println("Presplit Region number = " + this.regionNumber);
    System.out.println("row per region = " + this.rowNum / this.regionNumber);
    System.out.println("Also create dot table = " + this.createDotTable);
    return errCode;
  }

  /**
   * @param args
   * @throws Exception 
   */
  public static void main(final String[] args) throws Exception {
    GenerateTestTable gt = new GenerateTestTable();
    gt.init();
    if (gt.parseCommandLine(args) != 0) {
      System.err.println("fail to parse cmdline");
      return;
    }
    gt.createTable();
    
    if (gt.rowNum == 0) {
      System.out.println("rowNum=0, only create table");
      return;
    }

    gt.doMapReduce(RegionWriteInputFormat.class,
          GenerateRegionDataTask.class,
          gt.tableName);
    
    if (gt.createDotTable) {
      gt.doMapReduce(RegionWriteInputFormat.class,
          GenerateRegionDataTask.class,
          gt.dotTableName);
    }
  }
  
  /**
   * for test usage.
   * @param args
   * @throws Exception
   */
  public static void testmain(final String[] args,Configuration conf) throws Exception {
    GenerateTestTable gt = new GenerateTestTable();
    gt.conf = conf;
    gt.admin = new HBaseAdmin(gt.conf);
    if (gt.parseCommandLine(args) != 0) {
      System.err.println("fail to parse cmdline");
      return;
    }
    gt.createTable();
    
    if (gt.rowNum == 0) {
      System.out.println("rowNum=0, only create table");
      return;
    }

    gt.doMapReduce(RegionWriteInputFormat.class,
          GenerateRegionDataTask.class,
          gt.tableName);
    
    if (gt.createDotTable) {
      gt.doMapReduce(RegionWriteInputFormat.class,
          GenerateRegionDataTask.class,
          gt.dotTableName);
    }
  }
}
