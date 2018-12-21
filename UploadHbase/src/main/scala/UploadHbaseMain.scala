import com.google.gson.{JsonArray, JsonParser}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor}
import org.apache.hadoop.hbase.client.{HBaseAdmin, HTable, Put}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

object UploadHbaseMain {
  val logger = LogManager.getRootLogger

  def main(args: Array[String]): Unit = {
    Args.set(args)

    val spark = SparkSession.
      builder().
      appName("UploadHbase").
      config("spark.eventLog.enabled", "true").
//      master("local[4]").
      getOrCreate()

    // Load raw data from input file to RDD
    // use \n\n to split each metadata block
    val delim = if (Args.inputType == "json") "\n" else "\n\n"
    val originalData = spark.sparkContext.newAPIHadoopFile(
      Args.inputPath,
      classOf[TextInputFormat],
      classOf[LongWritable],
      classOf[Text],
      {
        val conf = new Configuration()
        conf.set("textinputformat.record.delimiter", delim)
        conf
      }
    ).map(_._2).map(_.toString)

    // Parse MetaData from each block
    val metaData = {
      if (Args.inputType == "json") {
        originalData.mapPartitions(iterator => {
          val jsonParser = new JsonParser()
          iterator.map(parseMeta10(_, jsonParser))
        })
      } else
        originalData.map(parseMeta)
    }.filter { case Meta(id, _) => id != ""}

    // Upload to hbase
    // Table and column setting
    val hTableName = "dblp"
    val cfName = "meta"
    val cRfsName = "references"
    val cYrName = "year"

    val hTable = stringToByteArray(hTableName)
    val cf = stringToByteArray(cfName)
    val cRfs = stringToByteArray(cRfsName)
    val cYr = stringToByteArray(cYrName)

    // create table on hbase
    val conf = HBaseConfiguration.create()
    val admin = new HBaseAdmin(conf)

    if (admin.isTableAvailable(hTableName)) {
      logger.info(f"droping original table $hTableName")
      admin.disableTable(hTableName)
      admin.deleteTable(hTableName)
    }

    logger.info(f"creating table $hTableName on hbase")
    val tableDescriptor = new HTableDescriptor(hTableName)
    tableDescriptor.addFamily(new HColumnDescriptor(cfName))
    admin.createTable(tableDescriptor)

    // upload metaData to hbase
    metaData.foreachPartition(iterator => {
      val conf = HBaseConfiguration.create()
      val hTable = new HTable(conf, hTableName)

      iterator.foreach { case Meta(id, references) =>
        val put = new Put(stringToByteArray(id))
        put.add(cf, cRfs, stringToByteArray(references.mkString(",")))
//        put.add(cf, cYr, stringToByteArray(year))
        hTable.put(put)
      }

      hTable.close()
    })
  }

  case class Meta(id: String, references: List[String])

  def jsonArrayToList(jsonArray: JsonArray): List[String] = {
    var l = List[String]()
    for(i <- 0 until jsonArray.size())
      l = jsonArray.get(i).getAsString :: l
    l
  }

  def parseMeta10(block: String, jsonParser: JsonParser): Meta = {
    val jsonObject = jsonParser.parse(block).getAsJsonObject
    val id = jsonObject.get("id").getAsString
    val references = {
      if (jsonObject.has("references"))
        jsonArrayToList(jsonObject.get("references").getAsJsonArray)
      else Nil
    }
    Meta(id, references)
  }

  def parseMeta(block: String): Meta = {
    // meta identifier
    val indexStart = "#index"
    val referenceStart = "#%"
    val yearStart = "#t"

    // meta variables
    var id = ""
    var references: List[String] = Nil
    var year = ""

    for (line <- block.split("\n")) {
      if (line.startsWith(indexStart))
        id = line.drop(indexStart.length)
      else if (line.startsWith(referenceStart))
        references = line.drop(referenceStart.length) :: references
    }

    Meta(id, references)
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
    var inputType: String = _
    var inputPath: String = _

    def set(args: Array[String]): Unit = {
      inputPath = args(0)
      inputType = args(1)
    }
  }
}
