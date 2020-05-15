package finalProject2.Common

import finalProject2.Utils.ProjectUtil
import org.apache.spark.{SparkConf, SparkContext}

trait TApplication {
  def start(appName:String="wordCount")(op: =>Unit)={
  ProjectUtil.getSparkContext(appName)

    try{
      op
    }catch {
      case e:Exception=>{e.printStackTrace()}
    }

  }

  def stop()={
    ProjectUtil.stop()
  }
}
