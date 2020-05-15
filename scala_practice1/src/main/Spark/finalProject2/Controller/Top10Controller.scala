package finalProject2.Controller

import finalProject2.Bean
import finalProject2.Service.Top10Service
import org.apache.spark.rdd.RDD

class Top10Controller {
  def exec() = {
    val service = new Top10Service
//     val result: Array[(String, (Int, Int, Int))] = service.analysis()
    val result: Array[Bean.HotCategory] = service.analysis()
    result.foreach(println)
  }
}
