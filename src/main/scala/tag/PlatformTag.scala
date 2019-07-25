package tag

import org.apache.spark.sql.Row
import utils.StrUtil

object PlatformTag extends Tag {
  /**
    * 打标签接口
    */
  override def makeTag(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    // 解析参数
    val row = args(0).asInstanceOf[Row]
    // 获取广告类型
    val adplatformproviderid = row.getAs[Int]("adplatformproviderid")
    if (adplatformproviderid >= 100000){
      list:+=("CN"+adplatformproviderid, 1)
    }
    list
  }
}
