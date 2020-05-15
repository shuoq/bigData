package finalProject2.Controller

import finalProject2.Service.wordCountService

class wordCountController {
  private val service = new wordCountService

  def exec()={
    val finalRDD: collection.Map[String, Long] = service.analysis()
    finalRDD
  }

}
