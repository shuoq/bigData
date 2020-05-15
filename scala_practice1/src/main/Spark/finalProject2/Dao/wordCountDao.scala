package finalProject2.Dao

import finalProject2.Common.TDao
import finalProject2.Utils.ProjectUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class wordCountDao extends TDao{
  def readData() ={
    val rdd: RDD[String] = source()
    rdd
  }

}
