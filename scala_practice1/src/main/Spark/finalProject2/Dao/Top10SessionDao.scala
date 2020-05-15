package finalProject2.Dao

import finalProject2.Common.TDao
import org.apache.spark.rdd.RDD

class Top10SessionDao extends TDao{
  def readData ={
    val rdd: RDD[String] = source("input/user_visit_action.txt")
    rdd
  }
}
