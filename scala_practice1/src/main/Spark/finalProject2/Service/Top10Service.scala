package finalProject2.Service

import Helper.HotCategoryAccumulator
import finalProject2.Bean.HotCategory
import finalProject2.Dao.Top10Dao
import finalProject2.Utils.ProjectUtil
import org.apache.spark.rdd.RDD

import scala.collection.mutable

class Top10Service {
  def analysis()={
    val dao = new Top10Dao
    val rdd: RDD[String] = dao.readData

//    //累加器
//
//    val acc = new HotCategoryAccumulator
//
//    ProjectUtil.getSparkContext().register(acc,"HotCat")
//
//    rdd.foreach(data=>{
//      val datas = data.split("_")
//      if(datas(6) != "-1")
//      {acc.add((datas(6),"click"))}
//      else if (datas(8) != "null"){
//        val id: Array[String] = datas(8).split(",")
//        id
//      }
//    })






    // TODO: 包装类
    val line: RDD[HotCategory] = rdd.flatMap(data => {
      val datas: mutable.ArrayOps[String] = data.split("_")
      if (datas(6) != "-1") {
        List(HotCategory(datas(6), 1, 0, 0))
      }
      else if (datas(8) != "null") {
        val ids: Array[String] = datas(8).split(",")
        ids.map(id => {
          HotCategory(id, 0, 1, 0)
        })
      }
      else if (datas(10) != "null") {
        val ids: Array[String] = datas(10).split(",")
        ids.map(id => {
          HotCategory(id, 0, 0, 1)
        })
      }
      else {
        Nil
      }
    })

    val groupRDD: RDD[(String, Iterable[HotCategory])] = line.groupBy(_.id)
    val categoryRDD: RDD[(String, HotCategory)] = groupRDD.mapValues(iter => {
      iter.reduce(
        (c1, c2) => {
          c1.clickCt = c1.clickCt + c2.clickCt
          c1.orderCt = c1.orderCt + c2.orderCt
          c1.payCt = c1.payCt + c2.payCt
          c1
        }
      )
    })
    val categoryMapRDD: RDD[HotCategory] = categoryRDD.map(s=>{s._2})
    val sortRDD: Array[HotCategory] = categoryMapRDD.collect().sortWith(
      (left, right) => {
        if (left.clickCt > right.clickCt) {
          true
        }
        else if (left.clickCt == right.clickCt) {
          if (left.orderCt > right.orderCt) {
            true
          } else if (left.orderCt == right.orderCt) {
            if (left.payCt > right.payCt) {
              true
            } else {
              false
            }
          }
          else {
            false
          }
        }
        else {
          false
        }
      }
    )
    sortRDD.take(10)


   //  TODO: (string,(i,i,i))格式 -> reduceByKey -> sortBy(_._2,false)
//    val line: RDD[(String, (Int, Int, Int))] = rdd.flatMap(data => {
//      val datas: mutable.ArrayOps[String] = data.split("_")
//      if (datas(6) != "-1") {
//        List((datas(6), (1, 0, 0)))
//      }
//      else if (datas(8) != "null") {
//        val ids: Array[String] = datas(8).split(",")
//        ids.map((_, (0, 1, 0)))
//      }
//      else if (datas(10) != "null") {
//        val ids: Array[String] = datas(10).split(",")
//        ids.map((_, (0, 0, 1)))
//      }
//      else {
//        Nil
//      }
//
//    })
//    val categoryRDD: RDD[(String, (Int, Int, Int))] = line.reduceByKey(
//      (c1, c2) => {
//        (c1._1 + c2._1, c1._2 + c2._2, c1._3 + c2._3)
//      })
//    categoryRDD.sortBy(_._2,false).take(10)


  }
}
