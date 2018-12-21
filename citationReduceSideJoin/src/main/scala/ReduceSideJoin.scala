import java.util.UUID

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

//import org.apache.hadoop.fs.shell.CopyCommands.Get

import org.apache.spark.SparkContext
object ReduceSideJoin {
  //private val log = LogManager.getLogManager

  def main(args: Array[String]): Unit = {

    Utils.run(ReduceSideJoin.getClass.getName, (spark) => {
      val id_type = args(0)
      val inputPath = args(1)
      val outputPath = if (args.length > 2) args(2) else null
      // different datasets

      val result = id_type match {
        case "int" => {
          val metadata = dataset.load_txt(spark, inputPath)
          v2[Int](spark, metadata)
        }
        case "uuid" => {
          val metadata = dataset.load_json(spark, inputPath)
          v2[UUID](spark, metadata)
        }
        case "string" => {
          val metadata = dataset.load_string(spark, inputPath)
          v2[String](spark, metadata)
        }
      }
      // output result
      if (outputPath != null) {
        spark.parallelize(Seq(result)).saveAsTextFile(outputPath)
      }

      println("count(a->c): " + result._1)
      println("count(total): " + result._2)
    })
  }


  def v3[U: ClassTag](spark: SparkContext, metaData: RDD[dataset.Paper[U]]): (Long, Long) = {
    // reduce side map (a,[b,c]) -> [(a,b),(a,c)]
    val outgoing = metaData.mapPartitions(iter => {
      iter.flatMap(x => {
        x.references.map(y => (x.id, y))
      })
    }).distinct().persist()

    println("count(total): " + outgoing.count())
    println("count(total): " + outgoing.count())

    (outgoing.count(), outgoing.count())
  }


  def v2[U: ClassTag](spark: SparkContext, metaData: RDD[dataset.Paper[U]]): (Long, Long) = {
    // reduce side map (a,[b,c]) -> [(a,b),(a,c)]
    //val metaData = dataset.load_json(spark, inputPath)

    // reduce side map (a,[b,c]) -> [(a,b),(a,c)]
    val outgoing = metaData.mapPartitions(iter => {
      iter.flatMap(x => {
        x.references.map(y => (x.id, y))
      })
    }).distinct().persist()


    // (a, b) -> (b, a) then join (b, c)
    val first = outgoing.map { case (a, b) => (b, a) }.join(outgoing).map { case (b, (a, c)) => (a, (b, c)) }
    //val first = outgoing.map(case (a, b) => (b, a)).union(outgoing).reduceByKey( case case (b, (a, c)) => (a, (b, c)))

    val second = first.join(outgoing).filter { case (a, ((b, c0), c1)) => c0 == c1 }
      .map { case (a, ((b, c0), c1)) => (a, c0) }.distinct()

    //second.foreach(println)
    // second.saveAsTextFile("r1")
    //val c0 = second.count()
      //val c1 = outgoing.count()
    (second.count(), outgoing.count())

  }


  /*def v0(spark: SparkContext, inputPath: String): Unit = {
    // load data from hdfs
    val metaData = dataset.load[UUID](spark, inputPath).persist()

    metaData.foreach(println)

    // map join (a,[b,c]) -> [(c, a, b), (b, a, c)]
    val triangle = metaData.mapPartitions(iter => {
      iter.flatMap(a => {
        a.references.flatMap(b => {
          a.references.map(c => {
            if (b != c)
              (b, a.id, c)
            else
              None
          }).filter(result => result != None)
        })
      })
    })

    triangle.foreach(println)


    // reduce side map (a,[b,c]) -> [(a,b),(a,c)]
    val outgoing = metaData.mapPartitions(iter => {
      iter.flatMap(x => {
        x.references.map(y => (x.id, y))
      })
    }).persist()

    // reduce side join (a,b),(a,c) -> [(b,(a,c)),(c,(a,b))]
    val triangle2 = outgoing.join(outgoing).filter(o => o._2._1 != o._2._2)
      .map(o => (o._2._2, (o._1, o._2._1)))

    triangle2.foreach(println)

    val matches = triangle2.map { case (b, (a, c)) => ((b, c), -1) }
      .reduceByKey(_ + _)

    println("----------------------")
    matches.foreach(println)

    val checked = outgoing.map { case (b, c) => ((b, c), -1) }
      .union(matches).reduceByKey(_ * _)
      .filter { case (_, sum) => sum > 0 }


    println("----------------------")
    checked.foreach(println)

  }*/

}
