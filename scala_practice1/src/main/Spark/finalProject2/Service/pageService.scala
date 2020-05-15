package finalProject2.Service

import com.atguigu.bigdata.spark.core.project.bean.UserVisitAction
import finalProject2.Dao.Top10SessionDao
import org.apache.spark.rdd.RDD

import scala.collection.GenTraversableOnce

case class pageService(){
  val dao = new Top10SessionDao

  def analysis() ={

    val rdd: RDD[String] = dao.readData

    val actionRDD: RDD[UserVisitAction] = rdd.map(fr => {
      val datas: Array[String] = fr.split("_")
      UserVisitAction(
        datas(0),
        datas(1).toLong,
        datas(2),
        datas(3).toLong,
        datas(4),
        datas(5),
        datas(6).toLong,
        datas(7).toLong,
        datas(8),
        datas(9),
        datas(10),
        datas(11),
        datas(12).toLong
      )
    })

    val actionCache: RDD[UserVisitAction] = actionRDD.cache()


    //计算分母
    val pageToOne: RDD[(Long, Int)] = actionCache.map(action => {
      (action.page_id, 1)
    })
    val pageSum: RDD[(Long, Int)] = pageToOne.reduceByKey(_+_)
    val FenMua: Array[(Long, Int)] = pageSum.collect()
    val FenMu: Map[Long, Int] = FenMua.toMap
    //结构转换

    //根据日期排序
    val groupRDD: RDD[(String, Iterable[UserVisitAction])] = actionCache.groupBy(_.session_id)
    val value: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(iter => {
      val sortAction: List[UserVisitAction] = iter.toList.sortWith(
        (left, right) => {
          left.action_time < right.action_time
        }
      )

      val ids: List[Long] = sortAction.map(_.page_id)

      val zipIds: List[(Long, Long)] = ids.zip(ids.tail)

      val tuples: List[(String, Int)] = zipIds.map {
        case (id1, id2) => {
          (id1 + "-" + id2, 1)
        }
      }
      tuples
    })

//    value.map(s=>{
//      val value1: List[(String, Int)] = s._2
//      val value2: String = s._1
//      value1.map(k=>{
//        ((k._1,k._2),value2)
//      })
//    })
//

    val fenzi1: RDD[(String, Int)] = value.map(_._2).flatMap(l=>l)
    val fenzi2: RDD[(String, Int)] = fenzi1.reduceByKey(_+_)



    //分子/ 分母
    fenzi2.foreach{
      case(id, count)=>{
        val ids: Array[String] = id.split("-")
        val FenMuCount: Int = FenMu(ids(0).toLong)
        println(id + "转换率为" + (count.toDouble/FenMuCount))

      }

    }

    //只取pageid


    //:计算单挑页面的平均停留时间

//    val actionRDD: RDD[UserVisitAction] = rdd.map(data => {
//      val datas: Array[String] = data.split("_")
//      UserVisitAction(
//        datas(0),
//        datas(1).toLong,
//        datas(2),
//        datas(3).toLong,
//        datas(4),
//        datas(5),
//        datas(6).toLong,
//        datas(7).toLong,
//        datas(8),
//        datas(9),
//        datas(10),
//        datas(11),
//        datas(12).toLong
//      )
//    })
//
//   val actionRDD2: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy(_.session_id)
//
//    val fenZi1: RDD[(String, List[(Long, Long)])] = actionRDD2.mapValues(iter => {
//      val pageID1: List[UserVisitAction] = iter.toList.sortWith(
//        (left, right) => {
//          left.action_time < right.action_time
//        }
//      )
//      val pageID2: List[Long] = pageID1.map(_.page_id)
//
//      val fenZi: List[(Long, Long)] = pageID2.zip(pageID2.tail)
//      fenZi
//    })
//    val fenZi2: RDD[(List[(Long, Long)], Int)] = fenZi1.map(s => {
//      val value: List[(Long, Long)] = s._2
//      (value, 1)
//    })
//
//    //fenMu
//    val pageTime: RDD[(Long, Int)] = actionRDD.map(s => {
//      (s.page_id, 1)
//    })
//    val fenMuTuple: RDD[(Long, Int)] = pageTime.reduceByKey(_+_)
//    val FenMu: Map[Long, Int] = fenMuTuple.collect().toMap
//
//    val fenZiTime: RDD[(List[(Long, Long)], Int)] = fenZi2.reduceByKey(_+_)
//



  }



}
