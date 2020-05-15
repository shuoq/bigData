package Helper

import finalProject.Bean.HotCategory
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable


//热门品类的累加器
class HotCategoryAccumulator extends AccumulatorV2[(String,String),mutable.Map[String,HotCategory]]{
  private val hcMap: mutable.Map[String, HotCategory] = mutable.Map[String,HotCategory]()

  override def isZero: Boolean = hcMap.isEmpty

  override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = new HotCategoryAccumulator

  override def reset(): Unit = hcMap.clear()

  override def add(v: (String, String)): Unit = {
    val categoryID = v._1
    val actionType = v._2

    val hc: HotCategory = hcMap.getOrElse(categoryID,HotCategory(categoryID,0,0,0))
    actionType match {
      case "click" =>{hc.clickCount+=1 }
      case "order" =>{hc.orderCount+=1 }
      case "click" =>{hc.clickCount+=1 }
    }

  }

  override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
    other.value.foreach{
      case(cid,hotCategory)=>{
        val hc: HotCategory = hcMap.getOrElse(cid,HotCategory(cid,0,0,0))
        hc.clickCount+= hotCategory.clickCount
        hc.orderCount+= hotCategory.orderCount
        hc.payCount+= hotCategory.payCount

        hcMap(cid)=hc
      }
    }
  }

  override def value: mutable.Map[String, HotCategory] = hcMap
}
