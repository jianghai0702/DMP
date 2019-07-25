package utils

object StrUtil {

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


  /**
    * 判断字符串是否为空
    * @param string
    * @return
    */
  def isNotEmpty(string: String): Boolean ={
    if (string == null) false
    string.length != 0
  }

}
