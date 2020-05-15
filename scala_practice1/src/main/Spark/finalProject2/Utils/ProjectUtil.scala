package finalProject2.Utils

import org.apache.spark.{SparkConf, SparkContext}

object ProjectUtil {
  private val locTh = new ThreadLocal[SparkContext]

  def getSparkContext(appName:String="wordCount",master:String="local[*]")={
    var sc = locTh.get()
    if(locTh.get() == null){
      val conf: SparkConf = new SparkConf().setAppName(appName).setMaster(master)
      sc = new SparkContext(conf)
      locTh.set(sc)
    }
    sc
  }

  def stop(): Unit ={
    val sc: SparkContext = locTh.get()
    if(sc !=null) sc.stop()
    locTh.remove()
  }
}
