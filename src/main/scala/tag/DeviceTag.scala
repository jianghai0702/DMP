package tag

import org.apache.spark.sql.Row

object DeviceTag extends Tag {
  /**
    * 打标签接口
    */
  override def makeTag(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    // 解析参数
    val row = args(0).asInstanceOf[Row]
    // 设备操作系统
    val client = row.getAs[Int]("client")
    client match {
      case 1 => list:+=("D00010001",1)
      case 2 => list:+=("D00010002",1)
      case 3 => list:+=("D00010003",1)
      case _ => list:+=("D00010004",1)
    }

    // 设备联网方式
    val networkmannerid = row.getAs[Int]("networkmannerid")
    networkmannerid match {
      case 1 => list:+=("D00020001",1)
      case 2 => list:+=("D00020002",1)
      case 3 => list:+=("D00020003",1)
      case 4 => list:+=("D00020004",1)
      case _ => list:+=("D00020005",1)
    }

    // 设备运营商方式
    val ispid = row.getAs[Int]("ispid")
    ispid match {
      case 1 => list:+=("D00030001",1)
      case 2 => list:+=("D00030002",1)
      case 3 => list:+=("D00030003",1)
      case _ => list:+=("D00030004",1)
    }
    list
  }
}
