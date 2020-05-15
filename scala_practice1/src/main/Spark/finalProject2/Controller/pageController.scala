package finalProject2.Controller

import finalProject2.Bean
import finalProject2.Service.{Top10Service, pageService}

class pageController {
  def exec() = {
    val service = new pageService
//     val result: Array[(String, (Int, Int, Int))] = service.analysis()
 service.analysis()

  }
}
