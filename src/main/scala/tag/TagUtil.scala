package tag

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.util.StringUtils
import utils.StrUtil

/**
  * 标签工具
  */
object TagUtil {

  // 用户过滤条件，必须有下列其中一个
  val userFilterCondition =
    """
      |imei !=''     or mac !=''     or idfa !=''     or openudid !=''     or androidid !=''     or
      |imeimd5 !=''  or macmd5 !=''  or idfamd5 !=''  or openudidmd5 !=''  or androididmd5 !=''  or
      |imeisha1 !='' or macsha1 !='' or idfasha1 !='' or openudidsha1 !='' or androididsha1 !=''
    """.stripMargin

  // 获取用户id
  def getUserId(row:Row): String ={
    row match{
      case v if !v.getAs[String]("imei").isEmpty => "IM: "+v.getAs[String]("imei")
      case v if !v.getAs[String]("mac").isEmpty => "IM: "+v.getAs[String]("mac")
      case v if !v.getAs[String]("idfa").isEmpty => "IM: "+v.getAs[String]("idfa")
      case v if !v.getAs[String]("openudid").isEmpty => "IM: "+v.getAs[String]("openudid")
      case v if !v.getAs[String]("androidid").isEmpty => "IM: "+v.getAs[String]("androidid")
      case v if !v.getAs[String]("imeimd5md5").isEmpty => "IM: "+v.getAs[String]("imeimd5")
      case v if !v.getAs[String]("macmd5md5").isEmpty => "IM: "+v.getAs[String]("macmd5")
      case v if !v.getAs[String]("idfamd5").isEmpty => "IM: "+v.getAs[String]("idfamd5")
      case v if !v.getAs[String]("openudidmd5").isEmpty => "IM: "+v.getAs[String]("openudidmd5")
      case v if !v.getAs[String]("androididmd5").isEmpty => "IM: "+v.getAs[String]("androididmd5")
      case v if !v.getAs[String]("imeisha1").isEmpty => "IM: "+v.getAs[String]("imeisha1")
      case v if !v.getAs[String]("macsha1").isEmpty => "IM: "+v.getAs[String]("macsha1")
      case v if !v.getAs[String]("idfasha1").isEmpty => "IM: "+v.getAs[String]("idfasha1")
      case v if !v.getAs[String]("openudidsha1").isEmpty => "IM: "+v.getAs[String]("openudidsha1")
      case v if !v.getAs[String]("androididsha1").isEmpty => "IM: "+v.getAs[String]("androididsha1")
    }
  }
}
