package tag

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
import utils.StrUtil

object KeywordTag extends  Tag {

  /**
    * 打标签接口
    */
  override def makeTag(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    // 解析参数
    val stopKeywordsBroadcast = args(1).asInstanceOf[Broadcast[Array[String]]]
    val row = args(0).asInstanceOf[Row]
    val stopKeywords: Array[String] = stopKeywordsBroadcast.value

    // 获取广告类型
    val keyword = row.getAs[String]("keywords")
    keyword.split("\\|")
        .filter(word=>{
          !stopKeywords.contains(word) && word.length>=3 && word.length<=8
        })
        .foreach(word=>{
          list:+=("K"+word, 1)
        })
    list
  }
}
