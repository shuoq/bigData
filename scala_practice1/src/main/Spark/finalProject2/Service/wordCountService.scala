package finalProject2.Service

import finalProject2.Dao.wordCountDao
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class wordCountService  {
  private val dao = new wordCountDao

 def analysis() ={
    val rdd: RDD[String] = dao.readData()
   val rddWord: RDD[String] = rdd.flatMap(s=>{s.split(" ")})
   val rddTuple: RDD[(String, Int)] = rddWord.map(s=>{(s,1)})
   val finalR: collection.Map[String, Long] = rddTuple.countByKey()
   finalR
 }
}
