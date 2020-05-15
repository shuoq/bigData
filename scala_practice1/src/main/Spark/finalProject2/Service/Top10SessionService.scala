package finalProject2.Service

import Helper.HotCategoryAccumulator
import com.atguigu.bigdata.spark.core.project.bean.UserVisitAction
import finalProject2.Bean
import finalProject2.Dao.{Top10Dao, Top10SessionDao}
import finalProject2.Utils.ProjectUtil
import org.apache.spark.rdd.RDD

class Top10SessionService {
  def analysis(c:Array[Bean.HotCategory])={
    val dao = new Top10SessionDao
    val rdd: RDD[String] = dao.readData
    val pid: Array[String] = c.map(s => {
      s.id
    })

    pid
  }
}
