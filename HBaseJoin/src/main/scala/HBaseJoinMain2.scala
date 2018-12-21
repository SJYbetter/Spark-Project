import java.util

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.log4j.LogManager
import org.apache.spark.{HashPartitioner, RangePartitioner}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object HBaseJoinMain2 {
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
    val b_a_unsorted: RDD[(String, List[String])] = b_a.aggregateByKey(List[String]())((l, a) => a :: l, (l1, l2) => l1 ++ l2).persist()
    val b_aRangePartitioner = new RangePartitioner(20, b_a_unsorted)
    val hashPartitioner = new HashPartitioner(20)
    //    b_a_unsorted.repartitionAndSortWithinPartitions()
    val b_as = b_a_unsorted.repartitionAndSortWithinPartitions(b_aRangePartitioner).persist()


    val abc_unsorted = createABC(b_as, cf_bc, column_bc, hbaseCacheLimit_bc).persist()
    //    val abcRangePartitioner = new RangePartitioner(20, abc_unsorted)
    //    val abc = abc_unsorted.repartitionAndSortWithinPartitions(abcRangePartitioner).persist()
    //    val abc = abc_unsorted.repartitionAndSortWithinPartitions(hashPartitioner)
    val abc = abc_unsorted
    val triplets = createTriplets(abc, cf_bc, column_bc, hbaseCacheLimit_bc)

    val count = triplets.count
    val total = b_a.count

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

      var cumIterator: Vector[(String, List[String])] = Vector()
      var cumCollected = 0

      var scanDict: List[(String, Array[String])] = List(("", null))

      val tmpIterator = partitionIterator.flatMap { case (b, as) =>
        while (scanDict.nonEmpty && b > scanDict.head._1) {
          scanDict = scanDict.tail
        }

        // A does not exist in current scanDict
        if (scanDict.isEmpty) {
          scanDict = getScanDict(stringToByteArray(b), hTable)
        }

        val cs = {
          if (scanDict.isEmpty || scanDict.head._1 != b)
            Array[String]()
          else
            scanDict.head._2
        }

        as.flatMap(a => cs.map(c => (a, c)))
      }

      val a_cSet: mutable.Map[String, mutable.Set[String]] = mutable.Map()
      tmpIterator.foreach { case (a, c) =>
        if (!a_cSet.contains(a))
          a_cSet.put(a, mutable.Set())
        a_cSet.getOrElse(a, mutable.Set()).add(c)
      }

      a_cSet.toIterator
    })

    a_c_s.reduceByKey((cSet1, cSet2) => cSet1 ++ cSet2).map { case (a, cSet) => (a, cSet.toList) }
    //    a_c_s.map{case (a,c) => ((a,c), 0)}.reduceByKey(_+_).map{case ((a,c), _) => (a, c)}
    //    a_c_s.distinct.aggregateByKey(List[String]())((l, c) => c :: l, (l1, l2) => l1 ++ l2)
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
      val tmpCacheLimit = Args.tmpCacheLimit

      var tmpPartitionIterator: List[(String, List[String])] = List()
      var cumTmp = 0

      partitionIterator.flatMap { case (a, cs) =>
        cumTmp += 1
        if (cumTmp < tmpCacheLimit && partitionIterator.hasNext) {
          tmpPartitionIterator = (a, cs) :: tmpPartitionIterator
          None
        } else {
          cumTmp = 0
          var scanDictSet: List[(String, Set[String])] = List(("", null))

          val tmpRes = tmpPartitionIterator.sortBy{case (a, cs) => a }.flatMap { case (a, cs) =>
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

            cs.filter(c => cSet.contains(c))
          }
          tmpPartitionIterator = Nil
          tmpRes
        }
      }

//      val partitionIteratorSorted = partitionIterator.toList.sortBy { case (a, cs) => a }.toIterator
//
//      partitionIteratorSorted.flatMap { case (a, cs) =>
//        while (scanDictSet.nonEmpty && a > scanDictSet.head._1) {
//          scanDictSet = scanDictSet.tail
//        }
//
//        // A does not exist in current scanDict
//        if (scanDictSet.isEmpty) {
//          scanDictSet = getScanDictSet(stringToByteArray(a), hTable)
//        }
//
//        val cSet = {
//          if (scanDictSet.isEmpty || scanDictSet.head._1 != a)
//            Set[String]()
//          else
//            scanDictSet.head._2
//        }
//
//        cs.filter(c => cSet.contains(c))
//      }
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
    var tmpCacheLimit = 50000

    def set(args: Array[String]): Unit = {
      outputPath = args(0)
      //      outputBAPath = args(1)
      //      outputABCPath = args(2)
      hbaseCacheLimit = args(1).toInt
    }
  }

}
