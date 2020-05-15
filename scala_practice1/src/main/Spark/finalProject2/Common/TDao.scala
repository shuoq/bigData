package finalProject2.Common

import finalProject2.Utils.ProjectUtil

trait TDao {
  def source(sr:String="input/1.txt"/*"input/user_visit_action.txt"*/)={
    ProjectUtil.getSparkContext().textFile(sr)
  }

}
