package finalProject2.Application

import finalProject2.Common.TApplication
import finalProject2.Controller.{Top10Controller, pageController}

object pageApplication extends App with TApplication{

  start(){
    val controller = new pageController
    controller.exec()
  }
}
