package finalProject2.Controller

import com.atguigu.bigdata.spark.core.project.bean
import finalProject2.Bean
import finalProject2.Service.{Top10Service, Top10SessionService}
import org.apache.spark.rdd.RDD

class Top10SessionController {
  def exec() = {
    val service = new Top10Service
    val service1 = new Top10SessionService
//     val result: Array[(String, (Int, Int, Int))] = service.analysis()
    val category: Array[Bean.HotCategory] = service.analysis()
    val value: Array[String] = service1.analysis(category)
    value.toList.foreach(println)
  }
}
