import org.apache.spark.{SparkConf, SparkContext}

object Utils {

  def run(name: String, callback: (SparkContext) => Unit) {
    val conf = new SparkConf().setAppName(name)
      .set("spark.eventLog.enabled", "true")

    val sc = new SparkContext(conf)
    callback(sc)

    //read_quit_key()
    sc.stop()
  }

  private def read_quit_key(): Unit = {

    while (System.in.read() != 'q') {
      System.out.println("press `q` to exit....")
    }
  }

}
