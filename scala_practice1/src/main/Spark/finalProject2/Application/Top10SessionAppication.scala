package finalProject2.Application

import finalProject2.Common.TApplication
import finalProject2.Controller.{Top10Controller, Top10SessionController}

object Top10SessionAppication extends App with TApplication{

  start("top10"){
    val controller = new Top10SessionController
    controller.exec()
  }
}
