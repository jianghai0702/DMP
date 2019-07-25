package tag

import org.apache.spark.sql.Row


object AdTag extends Tag {
  /**
    * 打标签接口
    */
  override def makeTag(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    // 解析参数
    val row = args(0).asInstanceOf[Row]
    // 获取广告类型
    val adspacetype = row.getAs[Int]("adspacetype")
    adspacetype match {
      case v if v>=10 => list:+=("LC"+v,1)
      case v if v<10 => list:+=("LC0"+v,1)
    }

    // 获取广告名称
    val adspacetypename = row.getAs[String]("adspacetypename")
    if (!adspacetypename.isEmpty){
      list:+=("LN"+adspacetypename, 1)
    }

    list
  }
}
