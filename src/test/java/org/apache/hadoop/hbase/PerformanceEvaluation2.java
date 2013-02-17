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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.cliffc.high_scale_lib.Counter;

public class PerformanceEvaluation2 {
  protected static final Log LOG = LogFactory
      .getLog(PerformanceEvaluation2.class.getName());

  /**
   * Enum for map metrics. Keep it out here rather than inside in the Map
   * inner-class so we can find associated properties.
   */
  protected static enum CounterType {
    /** elapsed time */
    ELAPSED_TIME,
    /** number of rows */
    ROWS,
    /** number of keyvalues */
    KVS,
  }

  /**
   * Implementations can have their status set.
   */
  static interface Status {
    /**
     * Sets status
     * 
     * @param msg
     *          status message
     * @throws IOException
     */
    void setStatus(final String msg) throws IOException;
  }

  public static enum RunType {
    run_native, run_mapred, run_mapred2,
  }

  private Configuration conf;
  private int threads = 1;
  private int maps = 0;
  private RunType runType = RunType.run_native;

  private Counter totalRowCount = new Counter();
  private Counter totalKVCount = new Counter();

  /**
   * Constructor
   * 
   * @param c
   *          Configuration object
   */
  public PerformanceEvaluation2(final Configuration c) {
    this.conf = c;
  }

  private static long getResultColsCount(Result result) {
    long count = 0;
    for (KeyValue kv : result.list()) {
      if (kv.getValueLength() > 0) {
        ++count;
      }
    }
    return count;
  }

  public static class E1 {
    /**
     * MapReduce job that runs a performance evaluation client in each map task.
     */
    public static class EvaluationMapTask extends
        Mapper<ImmutableBytesWritable, Result, String, LongWritable> {
      private Test cmd;

      @Override
      protected void setup(Context context) throws IOException,
          InterruptedException {
        try {
          this.cmd = new Test(context.getConfiguration());
        } catch (Exception e) {
          throw new IllegalStateException(
              "Could not instantiate Test instance", e);
        }
      }

      @Override
      protected void map(ImmutableBytesWritable key, Result value,
          final Context context) throws IOException, InterruptedException {
        this.cmd.columnsQuery(value);
        context.getCounter(CounterType.ROWS).increment(1);
        context.getCounter(CounterType.KVS).increment(
            getResultColsCount(value));
        context.progress();
      }
    }

    public static class RegionScanInputFormat extends
        InputFormat<NullWritable, TableSplit> {

      public static class RegionScanRecordReader extends
          RecordReader<NullWritable, TableSplit> {
        private TableSplit value = null;

        public RegionScanRecordReader(InputSplit split,
            TaskAttemptContext context) throws IOException,
            InterruptedException {
          initialize(split, context);
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
          this.value = (TableSplit) split;
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
          if (null == this.value) {
            return false;
          }

          return true;
        }

        @Override
        public NullWritable getCurrentKey() throws IOException,
            InterruptedException {
          return NullWritable.get();
        }

        @Override
        public TableSplit getCurrentValue() throws IOException,
            InterruptedException {
          TableSplit s = this.value;

          this.value = null;
          return s;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
          return 0.5f;
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
      public RecordReader<NullWritable, TableSplit> createRecordReader(
          InputSplit split, TaskAttemptContext context) throws IOException,
          InterruptedException {
        return new RegionScanRecordReader(split, context);
      }
    }

    public static class EvaluationMapTask2 extends
        Mapper<NullWritable, TableSplit, LongWritable, LongWritable> {

      private Test cmd;

      @Override
      protected void setup(Context context) throws IOException,
          InterruptedException {
        try {
          this.cmd = new Test(context.getConfiguration());
        } catch (Exception e) {
          throw new IllegalStateException("Could not instantiate PE instance",
              e);
        }
      }

      @Override
      protected void cleanup(Context context) throws IOException,
          InterruptedException {
        context.getCounter(CounterType.ROWS).increment(this.cmd.rows.get());
        context.getCounter(CounterType.KVS).increment(this.cmd.kvs.get());
      }

      @Override
      protected void map(NullWritable key, TableSplit value,
          final Context context) throws IOException, InterruptedException {

        long startTime = System.currentTimeMillis();
        // Evaluation task
        try {
          this.cmd.test(value);
        } catch (Exception e) {
          e.printStackTrace();
          throw new IOException(e);
        }

        long elapsedTime = System.currentTimeMillis() - startTime;

        // Collect how much time the thing took. Report as map output
        // and to the ELAPSED_TIME counter.
        context.getCounter(CounterType.ELAPSED_TIME).increment(elapsedTime);
        context.progress();
      }
    }
  }

  private void runTest(final Test cmd, List<TableSplit> splits)
      throws IOException, InterruptedException, ClassNotFoundException {
    if (this.runType.equals(RunType.run_native)) {
      doMultipleClients(cmd, splits, this.threads);
    } else {
      // must be the map reduce, do some clean up
      FileSystem fs = FileSystem.get(conf);
      Path path = new Path("/tmp", "tempout");
      fs.delete(path, true);

      try {
        if (this.runType.equals(RunType.run_mapred)) {
          doMapReduce(cmd, TableInputFormat.class, E1.EvaluationMapTask.class,
              path);
        }

        if (this.runType.equals(RunType.run_mapred2)) {
          doMapReduce(cmd, E1.RegionScanInputFormat.class,
              E1.EvaluationMapTask2.class, path);
        }
      } finally {
        fs.deleteOnExit(path);
      }
    }
  }

  private void doMultipleClients(final Test cmd, final List<TableSplit> splits,
      final int nthread) throws IOException {

    BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>(nthread);
    final ThreadPoolExecutor services = new ThreadPoolExecutor(nthread,
        nthread, 10, TimeUnit.SECONDS, queue,
        new ThreadPoolExecutor.CallerRunsPolicy());
    for (final TableSplit ts : splits) {
      services.submit(new Runnable() {

        @Override
        public void run() {
          try {
            long startTime = System.currentTimeMillis();
            runOneClient(cmd, ts);
            long elapsedTime = System.currentTimeMillis() - startTime;

            LOG.info("Finished " + Thread.currentThread().getName() + " in "
                + elapsedTime + "ms for " + cmd.rows.get() + " rows and "
                + cmd.kvs.get() + " cols");

            totalRowCount.add(cmd.rows.get());
            totalKVCount.add(cmd.kvs.get());
          } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          } 
        }
      });
    }

    services.shutdown();
    try {
      services.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private void doMapReduce(final Test cmd,
      Class<? extends InputFormat> inputFormatClass,
      Class<? extends Mapper> mapperClass, Path path) throws IOException,
      ClassNotFoundException, InterruptedException {
    Job job = new Job(this.conf);
    job.setJarByClass(PerformanceEvaluation2.class);
    job.setJobName("HBase Performance Evaluation");

    job.setInputFormatClass(inputFormatClass);

    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(LongWritable.class);

    job.setMapperClass(mapperClass);
    job.setReducerClass(LongSumReducer.class);

    job.setNumReduceTasks(1);

    job.setOutputFormatClass(TextOutputFormat.class);
    job.setJobName("Scanning [" + this.conf.get(Test.KEY_INPUT_TABLE) + "]");
    TextOutputFormat.setOutputPath(job, path);

    TableMapReduceUtil.addDependencyJars(job);
    // Add a Class from the hbase.jar so it gets registered too.
    TableMapReduceUtil.addDependencyJars(job.getConfiguration(),
        org.apache.hadoop.hbase.util.Bytes.class);

    TableMapReduceUtil.initCredentials(job);

    job.waitForCompletion(true);
    this.totalRowCount.add(job.getCounters().findCounter(CounterType.ROWS)
        .getValue());
    this.totalKVCount.add(job.getCounters().findCounter(CounterType.KVS)
        .getValue());
  }

  /*
   * A test. Subclass to particularize what happens per row.
   */
  static class Test {
    public static final String KEY_INPUT_TABLE = TableInputFormat.INPUT_TABLE;
    public static final String KEY_OPTION_CACHE_SIZE = "key_cache_size";
    public static final String OPTION_CACHE_BLOCK = "enable_cache_block";
    public static final String KEY_OPTION_SCAN_COLS = TableInputFormat.SCAN_COLUMNS;
    public static final String KEY_OPTION_ROWS = "key_rows";
    public static final String KEY_OPTION_FILTER = "key_filter";
    public static final String KEY_OPTION_PERIOD = "key_report_period";
    public static final String KEY_OPTION_IGNORERESULT = "key_ignoreresult";
    public static final String KEY_OPTION_REPEAT_TIMES = "key_repeat_times";

    protected Status status = new Status() {
      @Override
      public void setStatus(String msg) throws IOException {
        LOG.info(msg);
      }
    };

    protected int reportPeriod = 100000;
    protected int cacheSize = 1000;
    protected long maxRows = 0;
    protected int repeatTimes = 1;
    protected String cols;
    protected Configuration conf = null;

    protected Counter rows = new Counter();
    protected Counter kvs = new Counter();

    // protected HTable table;

    /**
     * Note that all subclasses of this class must provide a public contructor
     * that has the exact same list of arguments.
     * 
     * @throws IOException
     */
    Test(Configuration conf) throws IOException {
      this.reportPeriod = Integer.parseInt(conf.get(KEY_OPTION_PERIOD, "100000"));
      this.cols = conf.get(KEY_OPTION_SCAN_COLS);
      this.cacheSize = conf.getInt(KEY_OPTION_CACHE_SIZE, 1000);
      this.maxRows = conf.getLong(KEY_OPTION_ROWS, Long.MAX_VALUE);
      this.repeatTimes = conf.getInt(KEY_OPTION_REPEAT_TIMES, 1);

      this.conf = conf;
    }

    private String generateStatus(final String sr, final long i) {
      return sr + "/" + i;
    }

    protected int getReportingPeriod() {
      return this.reportPeriod;
    }

    void testTakedown() throws IOException {
    }

    /*
     * Run test
     * 
     * @return record count scanned.
     * 
     * @throws IOException
     */
    void test(TableSplit split) throws IOException, InstantiationException,
        IllegalAccessException, ClassNotFoundException {
      try {
        testTimed(split.getStartRow(), split.getEndRow(), this.maxRows);
      } finally {
        testTakedown();
      }
    }

    protected void addColumns(Scan scan, String columns) {
      byte[][][] splitted = getCFs(columns);
      for (byte[][] cf : splitted) {
        if (cf[1] == null) {
          scan.addFamily(cf[0]);
          LOG.debug("Add Family:" + new String(cf[0]));
        } else {
          scan.addColumn(cf[0], cf[1]);
          LOG.debug("Add ColumnFamily:" + new String(cf[0]) + "." + new String(cf[1]));
        }
      }
    }
    
    protected byte[][][] getCFs (String columns) {
      String[] cols = columns.split(" ");
      byte[][][] cfs = new byte[cols.length][2][];
      for (int i = 0; i < cols.length; ++i) {
        byte[][] fq = KeyValue.parseColumn(cols[i].getBytes());
        cfs[i][0] = fq[0];
        if (fq.length > 1 && fq[1] != null && fq[1].length > 0) {
          cfs[i][1] = fq[1];
        } else {
          cfs[i][1] = null;
        }
      }
      
      return cfs;
    }

    private Filter getFilter(String filterName) throws InstantiationException,
        IllegalAccessException, ClassNotFoundException {
      if (null == filterName || filterName.isEmpty()) {
        return null;
      }

      Filter f = Class.forName(filterName).asSubclass(Filter.class).newInstance();
      System.out.println("-------- filter created : " + f.toString());
      return f;
    }

    public void columnsQuery(Result result) {
      result.getRow();
    }

    /**
     * Provides the scan range per key.
     * 
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    void testTimed(byte[] startKey, byte[] endKey, long limit)
        throws IOException, InstantiationException, IllegalAccessException,
        ClassNotFoundException {
      HTable table = new HTable(conf, conf.get(KEY_INPUT_TABLE));

      try {
        for (int i = 0; i < repeatTimes; i++) {
          Scan scan = new Scan(startKey, endKey);
          String filterName = conf.get(Test.KEY_OPTION_FILTER);
          if (null != filterName && !filterName.isEmpty()) {
            if (filterName.equalsIgnoreCase("Stop"))
              return;
            if (filterName.equalsIgnoreCase("KeyOnly"))
              scan.setFilter(new KeyOnlyFilter());
            if (filterName.equalsIgnoreCase("Prefix"))
              scan.setFilter(new PrefixFilter(null));
          }

          scan.setCaching(this.cacheSize);

          String cacheblock = conf.get(Test.OPTION_CACHE_BLOCK);

          scan.setCacheBlocks(false);
          if (null != cacheblock && !cacheblock.isEmpty()) {
            if (cacheblock.equalsIgnoreCase("true"))
              scan.setCacheBlocks(true);
          }

          this.addColumns(scan, this.cols);

          String ignoreResult = conf.get(Test.KEY_OPTION_IGNORERESULT);
          boolean skipResult = false;
          if (null != ignoreResult && !ignoreResult.isEmpty()) {
            if (ignoreResult.equalsIgnoreCase("true")) {
              skipResult = true;
            }
          }

          ResultScanner rs = table.getScanner(scan);

          while (true) {
            if (rows.get() >= limit) {
              break;
            }
            Result result = rs.next();

            if (null == result) {
              break;
            }

            if (skipResult)
              continue;

            kvs.add(getResultColsCount(result));
            byte[] key = result.getRow();

            if (status != null && rows.get() > 0
                && (rows.get() % getReportingPeriod()) == 0) {
              status.setStatus(generateStatus(Bytes.toStringBinary(key),
                  rows.get()));
            }

            rows.increment();
          }
          rs.close();
        }

      } finally {
        table.close();
      }
    }
  }

  void runOneClient(final Test cmd, final TableSplit split) throws IOException,
      InstantiationException, IllegalAccessException, ClassNotFoundException {

    LOG.info("Start " + cmd + " from key: "
        + Bytes.toString(split.getStartRow()) + " to "
        + Bytes.toString(split.getEndRow()));
    long startTime = System.currentTimeMillis();
    cmd.test(split);
    long totalElapsedTime = System.currentTimeMillis() - startTime;

    LOG.info("Finished " + cmd + " in " + totalElapsedTime + "ms for "
        + cmd.rows.get() + " rows " + cmd.kvs.get() + " cols");
  }

  private List<TableSplit> getTableSplits(HTable table) throws IOException {
    Pair<byte[][], byte[][]> keys = table.getStartEndKeys();
    if (keys == null || keys.getFirst() == null || keys.getFirst().length == 0) {
      throw new IOException("Expecting at least one region.");
    }
    List<TableSplit> splits = new ArrayList<TableSplit>(100);

    for (int i = 0; i < keys.getFirst().length; i++) {
      // determine if the given start an stop key fall into the region
      TableSplit split = new TableSplit(table.getTableName(),
          keys.getFirst()[i], keys.getSecond()[i], null);

      splits.add(split);
      LOG.debug("Split -> " + i + " -> " + split);
    }

    return splits;
  }

  private void runTest(final Test cmd) throws IOException,
      InterruptedException, ClassNotFoundException {
    try {
      HBaseAdmin admin = null;
      try {
        String tableName = this.conf.get(TableInputFormat.INPUT_TABLE);
        admin = new HBaseAdmin(this.conf);
        if (!admin.tableExists(tableName)) {
          LOG.error("Table :" + tableName + " doesn't exists, will exit.");
        }
        List<TableSplit> splits = getTableSplits(new HTable(new Configuration(
            conf), Bytes.toBytes(tableName)));
        this.maps = splits.size();

        runTest(cmd, splits);
      } catch (Exception e) {
        LOG.error("Failed", e);
      }
    } finally {
    }
  }

  protected void printUsage() {
    printUsage(null);
  }

  protected void printUsage(final String message) {
    if (message != null && message.length() > 0) {
      System.err.println(message);
    }
    System.err.println("Usage: java " + this.getClass().getName() + " \\");
    System.err
        .println(" <--type=native|mapred|mapred2> [--enablecacheblock] [--ignoreresult] [--rows=] [--repeat=]" +
        		" --table=abc --cols=\"f:col1 f:col2\" --filter=org.apache.hadoop.hbase.filter.PrefixFilter [nThreads]");
    System.err.println();
  }

  public int doCommandLine(final String[] args) {
    // (but hopefully something not as painful as cli options).
    int errCode = -1;
    if (args.length < 1) {
      printUsage();
      return errCode;
    }

    String optionCols = null;
    String optionRow = String.valueOf(Long.MAX_VALUE);
    String tableName = null;

    for (int i = 0; i < args.length; i++) {
      String cmd = args[i];
      if (cmd.equals("-h") || cmd.startsWith("--h")) {
        printUsage();
        errCode = 0;
        break;
      }

      final String type = "--type=";
      if (cmd.startsWith(type)) {
        this.runType = RunType.valueOf("run_" + cmd.substring(type.length()));
        continue;
      }

      final String rows = "--rows=";
      if (cmd.startsWith(rows)) {
        optionRow = cmd.substring(rows.length());
        this.conf.set(Test.KEY_OPTION_ROWS, optionRow);

        continue;
      }

      final String table = "--table=";
      if (cmd.startsWith(table)) {
        tableName = cmd.substring(table.length());
        this.conf.set(Test.KEY_INPUT_TABLE, tableName);

        continue;
      }

      final String ignoreresult = "--ignoreresult";
      if (cmd.startsWith(ignoreresult)) {
        this.conf.set(Test.KEY_OPTION_IGNORERESULT, "true");

        continue;
      }

      final String cacheblock = "--enablecacheblock";
      if (cmd.startsWith(cacheblock)) {
        this.conf.set(Test.OPTION_CACHE_BLOCK, "true");

        continue;
      }

      final String family = "--cols=";
      if (cmd.startsWith(family)) {
        optionCols = cmd.substring(family.length());
        if (null != optionCols && !"".equals(optionCols)) {
          optionCols = optionCols.replaceAll("\"", "");
        }

        this.conf.set(Test.KEY_OPTION_SCAN_COLS, optionCols == null ? ""
            : optionCols);
        continue;
      }

      final String filter = "--filter=";
      if (cmd.startsWith(filter)) {
        String filterName = cmd.substring(filter.length());
        this.conf.set(Test.KEY_OPTION_FILTER, filterName == null ? ""
            : filterName);
        System.out.println("---- filter = " + filterName);
        continue;
      }

      final String repeats = "--repeat=";
      if (cmd.startsWith(repeats)) {
        String repeatTimes = cmd.substring(repeats.length());
        this.conf.set(Test.KEY_OPTION_REPEAT_TIMES, repeatTimes);
        continue;
      }

      if (this.runType == RunType.run_native) {
        try {
          this.threads = Integer.parseInt(cmd);
        } catch (Exception ignore) {
          this.threads = 0;
        }
      }
    }

    try {
      if (tableName == null || this.threads < 1) {
        printUsage("Please specify the table name or nthread");
        errCode = 1;
      } else {
        Test cmd = new Test(this.conf);
        long startTime = System.currentTimeMillis();
        runTest(cmd);
        long elapse = System.currentTimeMillis() - startTime;
        LOG.info("Time Elapsed: " + (elapse) + " milliseconds for scanning "
            + this.totalRowCount + " rows " + this.totalKVCount + " cols");
      }
    } catch (Exception e) {
      LOG.error("Error in running test", e);
      errCode = 2;
    } finally {
      LOG.info("Table Name:" + tableName);
      LOG.info("Type: " + this.runType);
      if (this.runType == RunType.run_native) {
        LOG.info("Threads: " + this.threads);
      } else {
        LOG.info("Maps: " + this.maps);
      }
    }

    return errCode;
  }

  /**
   * @param args
   * @throws IOException 
   */
  public static void main(final String[] args) throws IOException {
    Configuration c = HBaseConfiguration.create();
    System.exit(new PerformanceEvaluation2(c).doCommandLine(args));
  }

//   public static void main(final String[] args) {
//   Configuration configuration = HBaseConfiguration.create();
//   configuration.clear();
//  
//   configuration.set("hbase.zookeeper.quorum", "10.239.10.21"); // - Our
//   configuration.set("hbase.zookeeper.property.clientPort", "2181"); // - Port
//  
//   System.exit(new PerformanceEvaluation2(configuration).doCommandLine(args));
//   }
}
