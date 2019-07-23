package utils

object StringUtil {

  /**
    * 类型转换
    * @param str
    * @return
    */
  def stringToInt(str:String): Integer ={
    try {
      str.toInt
    }catch {
      case _ : Exception => 0
    }
  }

  /**
    * 类型转换
    * @param str
    * @return
    */
  def stringToDouble(str:String): Double ={
    try {
      str.toDouble
    }catch {
      case _ : Exception => 0.0
    }
  }
}
