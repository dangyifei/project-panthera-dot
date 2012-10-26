package com.intel.hadoop.hbase.dot.access.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.ImportTsv;
import org.apache.hadoop.hbase.mapreduce.TsvImporterMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.codehaus.jettison.json.JSONObject;

import com.intel.hadoop.hbase.dot.DotConstants;
import com.intel.hadoop.hbase.dot.DotUtil;
import com.intel.hadoop.hbase.dot.access.DataManipulationOps;
import com.intel.hadoop.hbase.dot.doc.Document;

/**
 * Importer mapper to read tsv file into dot row.
 *
 */
public class DotTsvImporterMapper extends TsvImporterMapper {
  private static final Log LOG = LogFactory.getLog(DotTsvImporterMapper.class);
  Map<byte[], Map<byte[], JSONObject>> schemas = null;
  Configuration conf = null;
  // docName_with_colfamily -> SchemaObject
  private ConcurrentMap<String, Object> schemaMap = new ConcurrentHashMap<String, Object>();

  @Override
  protected void setup(Context context) {
    super.setup(context);
  }

  protected void doSetup(Context context) {
    super.doSetup(context);
    init(context);
  }

  private void init(Context context) {
    // System.out.println("intial schemas object");
    Configuration conf = context.getConfiguration();
    this.conf = conf;
    try {
      schemas = DotUtil.genSchema(conf.getStrings(ImportTsv.COLUMNS_CONF_KEY));
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to generate schema automatically from column map", e);
    }
  }

  private Object loadSchema(String columnfamily, String docName, String schema,
      String serializer) {
    String docNameWithColumnfamilyName = DotUtil
        .getDocNameWithColumnfamilyName(columnfamily, docName);

    if (!schemaMap.containsKey(docNameWithColumnfamilyName)) {
      schemaMap.put(docNameWithColumnfamilyName,
          Document.parseSchema(serializer, schema));
    }

    return schemaMap.get(docNameWithColumnfamilyName);
  }

  @Override
  public void map(LongWritable offset, Text value, Context context)
      throws IOException {

    byte[] lineBytes = value.getBytes();
    // column family: { doc : {field : value }}
    Map<byte[], Map<byte[], Map<byte[], byte[]>>> familyMap = new TreeMap<byte[], Map<byte[], Map<byte[], byte[]>>>(
        Bytes.BYTES_COMPARATOR);
    ImportTsv.TsvParser.ParsedLine parsed = null;
    Put put = null;
    ImmutableBytesWritable rowKey = null;
    try {
      parsed = parser.parse(lineBytes, value.getLength());
      // prepare the row-key
      rowKey = new ImmutableBytesWritable(lineBytes, parsed.getRowKeyOffset(),
          parsed.getRowKeyLength());

      // new put instance with that row key
      put = new Put(rowKey.copyBytes());
      for (int i = 0; i < parsed.getColumnCount(); i++) {
        // ignore that row-key column
        if (i == parser.getRowKeyColumnIndex())
          continue;

        byte[] columnfamily = parser.getFamily(i);
        Map<byte[], Map<byte[], byte[]>> docMap = familyMap.get(columnfamily);
        if (docMap == null) {
          docMap = new TreeMap<byte[], Map<byte[], byte[]>>(
              Bytes.BYTES_COMPARATOR);
          familyMap.put(columnfamily, docMap);
        }
        byte[] qualifier = parser.getQualifier(i);
        byte[][] df = DotUtil.getDocAndField(qualifier, 0, qualifier.length);

        Map<byte[], byte[]> fieldMap = docMap.get(df[0]);
        if (fieldMap == null) {
          fieldMap = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
          docMap.put(df[0], fieldMap);
        }

        // lineBytes,
        byte[] val = new byte[parsed.getColumnLength(i)];
        System.arraycopy(lineBytes, parsed.getColumnOffset(i), val, 0,
            val.length);
        fieldMap.put(df[1], val);
      }

    } catch (ImportTsv.TsvParser.BadTsvLineException badLine) {
      if (skipBadLines) {
        System.err.println("Bad line at offset: " + offset.get() + ":\n"
            + badLine.getMessage());
        incrementBadLineCount(1);
        return;
      } else {
        throw new IOException(badLine);
      }
    } catch (IllegalArgumentException e) {
      if (skipBadLines) {
        System.err.println("Bad line at offset: " + offset.get() + ":\n"
            + e.getMessage());
        incrementBadLineCount(1);
        return;
      } else {
        throw new IOException(e);
      }
    }

    // encode all fields under the same doc into single value
    String serializer = this.conf.get(
        DotConstants.HBASE_DOT_COLUMNFAMILY_DOC_SERIALIZER_CLASS,
        DotConstants.HBASE_DOT_COLUMNFAMILY_DOC_SERIALIZER_CLASS_DEFAULT);
    // put doc and its corresponding value
    Iterator fmIterator = familyMap.entrySet().iterator();
    DataManipulationOps dataManipulationOps = new DataManipulationOps();
    while (fmIterator.hasNext()) {
      Map.Entry<byte[], Map<byte[], Map<byte[], byte[]>>> fmEntry = (Entry<byte[], Map<byte[], Map<byte[], byte[]>>>) fmIterator
          .next();

      byte[] columnfamily = fmEntry.getKey();
      Iterator docIterator = fmEntry.getValue().entrySet().iterator();
      while (docIterator.hasNext()) {
        Map.Entry<byte[], Map<byte[], byte[]>> docEntry = (Entry<byte[], Map<byte[], byte[]>>) docIterator
            .next();
        byte[] doc = docEntry.getKey();

        Object schema = loadSchema(new String(columnfamily), new String(doc),
            schemas.get(columnfamily).get(doc).toString(), serializer);
        byte[] data = dataManipulationOps.getDocValue(new String(doc),
            docEntry.getValue(), Pair.newPair(schema, serializer), conf);
        KeyValue kv = new KeyValue(lineBytes, parsed.getRowKeyOffset(),
            parsed.getRowKeyLength(), columnfamily, 0, columnfamily.length,
            doc, 0, doc.length, ts, KeyValue.Type.Put, data, 0, data.length);
        // System.out.println("doc: "+new String(data));
        put.add(kv);

      }
    }

    try {
      context.write(rowKey, put);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

}