package finalProject2.Application

import finalProject2.Common.TApplication
import finalProject2.Controller.Top10Controller

object Top10Appication extends App with TApplication{

  start("top10"){
    val controller = new Top10Controller
    controller.exec()
  }
}
