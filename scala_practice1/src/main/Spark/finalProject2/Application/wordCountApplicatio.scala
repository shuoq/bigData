package finalProject2.Application

import finalProject.Controller.WordCountController
import finalProject2.Common.TApplication
import finalProject2.Controller.wordCountController
import org.apache.spark.{SparkConf, SparkContext}

object wordCountApplicatio extends App with TApplication{
  start(){
    val controller = new wordCountController
    val result: collection.Map[String, Long] = controller.exec()
    println(result)
  }
}
