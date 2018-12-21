import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.google.gson.JsonParser


object dataset {

  case class Meta(id: Int, references: List[Int])

  case class MetaUUID(id: UUID, references: List[UUID])

  case class Paper[U](id: U, references:List[U])

  private def parseAsString(block: String): Paper[String] = {
    val indexStart = "#index"
    val referenceStart = "#%"
    var id: String = ""
    var references: List[String] = Nil

    for (line <- block.split("\n")) {
      if (line.startsWith(indexStart))
        id = line.drop("#index".length)
      else if (line.startsWith(referenceStart))
        references = line.drop(2) :: references
    }

    Paper[String](id, references)
  }


  private def parseMeta(block: String): Paper[Int] = {
    val indexStart = "#index"
    val referenceStart = "#%"
    var id:Int = -1
    var references: List[Int] = Nil

    for (line <- block.split("\n")) {
      if (line.startsWith(indexStart))
        id =  line.drop(indexStart.length).toInt
      else if (line.startsWith(referenceStart))
        references = line.drop(referenceStart.length).toInt :: references
    }

    Paper[Int](id, references)
  }

  def load[U](spark: SparkContext, inputPath: String): RDD[Paper[U]] = {
    if (inputPath.endsWith(".text")) {
      load_txt(spark, inputPath).asInstanceOf[RDD[Paper[U]]]
    } else {
      load_json(spark, inputPath).asInstanceOf[RDD[Paper[U]]]
    }
  }
  def load_string(spark: SparkContext, inputPath: String): RDD[Paper[String]] = {
    val originalData = spark.newAPIHadoopFile(
      inputPath,
      classOf[TextInputFormat],
      classOf[LongWritable],
      classOf[Text], {
        val conf = new Configuration()
        conf.set("textinputformat.record.delimiter", "\n\n")
        conf
      }
    ).map(_._2).map(_.toString)

    originalData.map(parseAsString).filter { case Paper(id, _) => id != -1 }
  }

  def load_txt(spark: SparkContext, inputPath: String): RDD[Paper[Int]] = {
    val originalData = spark.newAPIHadoopFile(
      inputPath,
      classOf[TextInputFormat],
      classOf[LongWritable],
      classOf[Text], {
        val conf = new Configuration()
        conf.set("textinputformat.record.delimiter", "\n\n")
        conf
      }
    ).map(_._2).map(_.toString)


    originalData.map(parseMeta).filter { case Paper(id, _) => id != -1 }
  }

  def load_json(spark: SparkContext, inputPath: String): RDD[Paper[UUID]] = {
    val references = spark.textFile(inputPath).mapPartitionsWithIndex { case (id, iter) => {
      val parser = new JsonParser()
      iter.map(text => {
        val article = parser.parse(text).getAsJsonObject
        val uuid = UUID.fromString(article.get("id").getAsString)

        var references: List[UUID] = Nil
        if (article.has("references")) {
          val refs = article.get("references").getAsJsonArray
          for (i <- 0 until refs.size())
            references = UUID.fromString(refs.get(i).getAsString) :: references
        }
        Paper[UUID](uuid, references)
      })
    }
    }

    references
  }

}

