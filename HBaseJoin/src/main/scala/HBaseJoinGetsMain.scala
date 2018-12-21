import java.util

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.log4j.LogManager
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object HBaseJoinGetsMain {
  val logger = LogManager.getRootLogger
  // Table, column family and column name
  val hTableName = "dblp"
  val cfName = "meta"
  val cName = "references"

  val hTable = stringToByteArray(hTableName)
  val cf = stringToByteArray(cfName)
  val column = stringToByteArray(cName)

  def main(args: Array[String]): Unit = {
    Args.set(args)

    val spark = SparkSession.
      builder().
      appName("HBaseJoinMain").
      config("spark.eventLog.enabled", "true").
      //      master("local[4]").
      getOrCreate()

    val cf_bc = spark.sparkContext.broadcast(cf)
    val column_bc = spark.sparkContext.broadcast(column)
    val hbaseCacheLimit_bc = spark.sparkContext.broadcast(Args.hbaseCacheLimit)

    // Load metaData from hbase
    val metaData = loadMetaData(spark)

    // RDD of (b, a) where a -> b
    val b_a = createB_A(metaData).persist()
    val total = b_a.map(_ => 1).reduce(_+_)
    val b_as_unsorted: RDD[(String, List[String])] = b_a.aggregateByKey(List[String]())((l, a) => a :: l, (l1, l2) => l1 ++ l2).persist()
    val b_as = b_as_unsorted.repartition(200)


    val abc_unsorted = createABC(b_as, cf_bc, column_bc, hbaseCacheLimit_bc).persist()
    val abc = abc_unsorted
    val triplets = createTriplets(abc, cf_bc, column_bc, hbaseCacheLimit_bc)

    val count = triplets.map(_ => 1).reduce(_+_)
//    val total = b_a.count

    println(f"Count: $count")
    println(f"Total: $total")
    spark.sparkContext.parallelize(List(f"Count: $count, Total: $total")).saveAsTextFile(Args.outputPath)
  }

  def loadMetaData(spark: SparkSession): RDD[(String, Array[String])] = {
    val conf = HBaseConfiguration.create()

    conf.set(TableInputFormat.INPUT_TABLE, hTableName)

    // Load data from hbase using spark-hadoop api

    val connection = ConnectionFactory.createConnection(conf)
    val admin = connection.getAdmin
    val hbaseData = spark.sparkContext.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

    // rdd of metaData
    val metaData = hbaseData.map { case (k, v) =>
      (byteArrayToString(k.get()),
        byteArrayToReferences(v.getColumnLatestCell(cf, column).getValue))
    }

    metaData
  }

  def createB_A(metaData: RDD[(String, Array[String])]): RDD[(String, String)] = {
    metaData.flatMap { case (a, bs) => bs.map(b => (b, a)) }
  }

  def multipleGet(rows: Array[String], hTable: Table, cf: Array[Byte], column: Array[Byte]): Array[Array[String]] = {
    val gets = new util.ArrayList[Get]()
    rows.foreach(row => {
      val get = new Get(stringToByteArray(row))
      get.addColumn(cf, column)
      gets.add(get)
    })

    hTable.get(gets).
      map(result => result.getValue(cf, column)).
      map(ba => if (ba == null) Array[String]() else byteArrayToReferences(ba))
  }

  def createABC(b_a: RDD[(String, List[String])],
                cf_bc: Broadcast[Array[Byte]],
                column_bc: Broadcast[Array[Byte]],
                hbaseCacheLimit_bc: Broadcast[Int]): RDD[(String, List[String])] = {
    val a_c_s = b_a.mapPartitions(partitionIterator => {
      val conf = HBaseConfiguration.create()
      val connection = ConnectionFactory.createConnection(conf)
      val hTable = connection.getTable(TableName.valueOf(hTableName))
      val cf = cf_bc.value
      val column = column_bc.value
      val hbaseCacheLimit = hbaseCacheLimit_bc.value

      partitionIterator.grouped(hbaseCacheLimit).flatMap { cumIterator =>
        val b_as_s = cumIterator.toArray

        val b_as = b_as_s.map { case (b, as) => as }
        val bs = b_as_s.map { case (b, as) => b }
        val b_cs = multipleGet(bs, hTable, cf, column)

        (b_as zip b_cs).toIterator.flatMap { case (as, cs) => cs.flatMap(c => as.map(a => (a, c))) }
      }
    })

//    a_c_s.distinct.aggregateByKey(List[String]())((l, c) => c :: l, (l1, l2) => l1 ++ l2)
    a_c_s.aggregateByKey(Set[String]())((s, c) => s + c, (s1, s2) => s1 ++ s2).mapValues(_.toList)
  }

  def createTriplets(abc: RDD[(String, List[String])],
                     cf_bc: Broadcast[Array[Byte]],
                     column_bc: Broadcast[Array[Byte]],
                     hbaseCacheLimit_bc: Broadcast[Int]) = {
    abc.mapPartitions(partitionIterator => {
      val conf = HBaseConfiguration.create()
      val connection = ConnectionFactory.createConnection(conf)
      val hTable = connection.getTable(TableName.valueOf(hTableName))
      val cf = cf_bc.value
      val column = column_bc.value
      val hbaseCacheLimit = hbaseCacheLimit_bc.value

      partitionIterator.grouped(hbaseCacheLimit).flatMap { cumIterator =>
        val a_cs_s = cumIterator.toArray

        val a_cs = a_cs_s.map { case (a, cs) => cs }
        val as = a_cs_s.map { case (a, cs) => a }
        val a_cSet = multipleGet(as, hTable, cf, column).map(realCs => realCs.toSet)

        (a_cSet zip a_cs).flatMap { case (cSet, cs) => cs.filter(c => cSet.contains(c)) }
      }
    })
  }

  def stringToReferences(s: String): Array[String] = {
    s.split(",").filter(_ != "")
  }

  def stringToByteArray(s: String): Array[Byte] = {
    s.toCharArray.map(_.toByte)
  }

  def byteArrayToString(ba: Array[Byte]): String = {
    ba.map(_.toChar).mkString
  }

  def byteArrayToReferences(ba: Array[Byte]): Array[String] = {
    stringToReferences(byteArrayToString(ba))
  }

  object Args {
    var outputPath: String = _
    //    var outputBAPath: String = _
    //    var outputABCPath: String = _
    var hbaseCacheLimit = 50

    def set(args: Array[String]): Unit = {
      outputPath = args(0)
      //      outputBAPath = args(1)
      //      outputABCPath = args(2)
      hbaseCacheLimit = args(1).toInt
    }
  }

}
