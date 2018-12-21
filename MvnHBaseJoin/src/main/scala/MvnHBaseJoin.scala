import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object MvnHBaseJoin {
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
            master("local[2]").
      getOrCreate()

    // Load metaData from hbase
    val metaData = loadMetaData(spark)

    // RDD of (b, a) where a -> b
    val b_a = createB_A(metaData).persist()
    // RDD of (a, c) where there exist at least a b where a -> b -> c, (a, c) are distinct
    val abc = createABC(b_a)
    // count of a -> c where there exist at least a b that satisfies a -> b -> c
    val count = createTripletsCount(abc)
    val total = b_a.count()
    val prop = count.toDouble / total

    println(f"Count: $count")
    println(f"Total: $total")
    println(f"Prop: ${count.toDouble / total}")

//    spark.sparkContext.parallelize(List(f"Count: $count, Total: $total")).saveAsTextFile(Args.outputPath)
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
    logger.info("Reading metaData from hbase")
    val metaData = hbaseData.map { case (k, v) =>
//      println(f"Get key: $k")
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

  def createABC(b_a: RDD[(String, String)]): RDD[(String, String)] = {
    val abc = b_a.groupByKey().mapPartitions(bIterator => {
      val conf = HBaseConfiguration.create()
      val connection = ConnectionFactory.createConnection(conf)
      val hTable = connection.getTable(TableName.valueOf(hTableName))

      bIterator.flatMap{ case(b, aIterator) =>
        val get = new Get(stringToByteArray(b))
        get.addColumn(cf, column)
        val result = hTable.get(get)

        if(result.isEmpty) None
        else {
          val cs = byteArrayToReferences(result.getValue(cf, column))
          aIterator.flatMap (a => {
            cs.map(c => (a, c))
          })
        }
      }
    })

    abc.distinct()
  }

  def createTripletsCount(abc: RDD[(String, String)]): Long = {
    abc.groupByKey.mapPartitions(aIterator => {
      val conf = HBaseConfiguration.create()
      val connection = ConnectionFactory.createConnection(conf)
      val hTable = connection.getTable(TableName.valueOf(hTableName))

      aIterator.flatMap{ case (a, cIterator) =>
        val get = new Get(stringToByteArray(a))
        get.addColumn(cf, column)
        val result = hTable.get(get)
        if (result.isEmpty) None
        else {
          val cSet = byteArrayToReferences(result.getValue(cf, column)).toSet[String]
          cIterator.filter(c => cSet.contains(c))
        }
      }
    }).count
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

    def set(args: Array[String]): Unit = {
      outputPath = args(0)
    }
  }
}

