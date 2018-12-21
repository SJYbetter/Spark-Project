import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.log4j.LogManager
import org.apache.spark.RangePartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object HBaseJoinScanMain {
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

    // Load metaData from hbase
    val metaData = loadMetaData(spark)

    // RDD of (b, a) where a -> b
    val b_a_unsorted = createB_A(metaData).repartition(Args.partitionCnt)
    val total = b_a_unsorted.count
    val b_aRangePartitioner = new RangePartitioner(Args.partitionCnt, b_a_unsorted)
    val b_a = b_a_unsorted.repartitionAndSortWithinPartitions(b_aRangePartitioner).persist()
    //    b_a.saveAsTextFile(Args.outputBAPath)
    // RDD of (a, c) where there exist at least a b where a -> b -> c, (a, c) are distinct
    val abc_unsorted = createABC(b_a).persist()
    val abcRangePartitioner = new RangePartitioner(Args.partitionCnt, abc_unsorted)
    val abc = abc_unsorted.repartitionAndSortWithinPartitions(abcRangePartitioner).persist()
    //    abc.saveAsTextFile(Args.outputABCPath)
    // count of a -> c where there exist at least a b that satisfies a -> b -> c
    val triplets = createTriplets(abc).persist()
    val count = triplets.count
    //    val total = b_a.count()
    //    val prop = count.toDouble / total

    println(f"Count: $count")
    println(f"Total: $total")
    //    println(f"Prop: $prop")
    //
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
    metaData.flatMap { case (a, references) =>
      references.map(b => (b, a))
    }
  }

  def getScanDict(startRow: Array[Byte], hTable: Table): List[(String, Array[String])] = {
    val scan = new Scan().withStartRow(startRow).setLimit(Args.hbaseCacheLimit)
    scan.addColumn(cf, column)
    val scanner = hTable.getScanner(scan)

    val rowRefsMap = ListBuffer[(String, Array[String])]()

    val scannerIterator = scanner.iterator()
    while (scannerIterator.hasNext) {
      val result = scannerIterator.next()
      if (result.isEmpty) {}
      else {
        val cs = byteArrayToReferences(result.getValue(cf, column))
        rowRefsMap.append((byteArrayToString(result.getRow), cs))
      }
    }

    rowRefsMap.toList
  }

  def getScanDictSet(startRow: Array[Byte], hTable: Table): List[(String, Set[String])] = {
    val scanDict = getScanDict(startRow, hTable)
    scanDict.map { case (a, cArray) => (a, cArray.toSet) }
  }

  def createABC(b_a: RDD[(String, String)]): RDD[(String, String)] = {
    val abc = b_a.mapPartitions(bIterator => {
      val conf = HBaseConfiguration.create()
      val connection = ConnectionFactory.createConnection(conf)
      val hTable = connection.getTable(TableName.valueOf(hTableName))

      var scanDict: List[(String, Array[String])] = List(("", null))

      bIterator.flatMap { case (b, a) =>
        // move scanDict to the position of b
        while (scanDict.nonEmpty && b > scanDict.head._1) {
          scanDict = scanDict.tail
        }

        // B does not exist in current scanDict
        if (scanDict.isEmpty) {
          scanDict = getScanDict(stringToByteArray(b), hTable)
        }

        // If b does not exist in data
        if (scanDict.isEmpty || scanDict.head._1 != b)
          Nil
        else {
          val cs = scanDict.head._2
          cs.map(c => (a, c))
        }
      }
    })

    logger.info("Before distinct")
    val abc_distinct = abc.distinct()
    logger.info("After distinct")
    abc_distinct
  }

  def createTriplets(abc: RDD[(String, String)]) = {
    abc.mapPartitions(aIterator => {
      val conf = HBaseConfiguration.create()
      val connection = ConnectionFactory.createConnection(conf)
      val hTable = connection.getTable(TableName.valueOf(hTableName))

      var scanDictSet: List[(String, Set[String])] = List(("", null))

      aIterator.filter { case (a, c) =>
        while (scanDictSet.nonEmpty && a > scanDictSet.head._1) {
          scanDictSet = scanDictSet.tail
        }

        // A does not exist in current scanDict
        if (scanDictSet.isEmpty) {
          scanDictSet = getScanDictSet(stringToByteArray(a), hTable)
        }

        val cSet = {
          if (scanDictSet.isEmpty || scanDictSet.head._1 != a)
            Set[String]()
          else
            scanDictSet.head._2
        }

        cSet.contains(c)
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
    var partitionCnt = 200

    def set(args: Array[String]): Unit = {
      outputPath = args(0)
      //      outputBAPath = args(1)
      //      outputABCPath = args(2)
      hbaseCacheLimit = args(1).toInt
      partitionCnt = args(2).toInt
    }
  }

}
