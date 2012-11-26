# A Document Store for Better Query Processing on HBase #

####Why query processing on HBase####

In Hadoop based data warehousing systems (e.g., Hive) , the users can analyze their massive data using a high level query language based on the relational model; the queries are then automatically converted into a series of MapReduce jobs for data processing. By bringing the traditional database techniques to the Hadoop ecosystem, these systems make MapReduce much more accessible to mainstream users. 

Initially, these systems typically store their data in HDFS, a high throughput but batch-processing oriented distributed file system (e.g., see [this link](http://hadoopblog.blogspot.com/2011/04/data-warehousing-at-facebook.html) for an example of one of the largest Hive deployment in the world). On the other hand, in the last couple of years, increasingly more systems have transitioned to a (semi) realtime analytics system built around HBase (i.e., storing data in HBase and running MapReduce jobs directly on HBase tables for query processing), which make several new use cases possible:

* *Stream* new data into HBase in near realtime for processing (versus loading new data into the HDFS cluster in large batch, e.g., every few hours).

* Support *high-update rate* workloads (by directly updating existing data in HBase) to keep the warehouse always up to date.

* Allow every low latency, *online data serving* (e.g., to power an online web service).

#### Overheads of query processing on HBase ####

HBase is an open source implementation of BigTable, which provides very flexible schema support, and very low latency get/put of individual cells in the table. While HBase already has very effective MapReduce integration with its good scanning performance, query processing using MapReduce on HBase still has significant gaps compared to query processing on HDFS.

* *Space overheads*. To provide flexible schema support, physically HBase stores its table as a multi-dimensional map, where each cell (except the row key) is stored on disk as a key-value pair: *(row_id, family:column, timestamp) -> cell*. On the other hand, a Hive table has a fixed relational model, and consequently HBase can introduce large space overheads (sometimes as large as 3x) compared to storing the same table in HDFS.

* *Query performance*. Query processing on HBase can be much (sometimes 3~5x) slower than that on HDFS due to various reasons. One of the reason is related to how HBase handles data accesses – HBase provides very good support for highly concurrent read/write accesses; consequently, one needs to pay some amount of overheads (e.g., concurrency control) for each column read. On the other hand, data accesses in analytical query processing are predominantly read (with some append), and should preferably avoid the column read overheads.

