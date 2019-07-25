package tag

import org.apache.spark.sql.Row
import utils.StrUtil

object LocationTag extends Tag {
  /**
    * 打标签接口
    */
  override def makeTag(args: Any*): List[(String, Int)] = {
    // provincename  cityname
    var list = List[(String,Int)]()
    // 解析参数
    val row = args(0).asInstanceOf[Row]
    // 获取广告类型
    val provincename = row.getAs[String]("provincename")
    val cityname = row.getAs[String]("cityname")

    if (StrUtil.isNotEmpty(provincename)){
      list:+=(provincename,1)
    }
    if (StrUtil.isNotEmpty(cityname)){
      list:+=(cityname,1)
    }
    list
  }
}
