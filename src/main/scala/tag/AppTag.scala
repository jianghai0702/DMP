package tag

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
import utils.StrUtil

object AppTag extends Tag {
  /**
    * 打标签接口
    */
  override def makeTag(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    // 解析参数
    val appMapBroadcast = args(1).asInstanceOf[Broadcast[Map[String, String]]]
    val row = args(0).asInstanceOf[Row]

    // 获取广告类型
    val appid = row.getAs[String]("appid")
    var appname = row.getAs[String]("appname")

    if (StrUtil.isNotEmpty(appname)){
      list:+=("APP"+appname,1)
    }else if (StrUtil.isNotEmpty(appid)){
      // 通过appmap获取appname
      appname = appMapBroadcast.value.getOrElse(appid, "未知app")
      list:+=("APP"+appname,1)
    }
    list
  }
}
