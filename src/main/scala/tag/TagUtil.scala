package tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

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

    row match {
    case v if StringUtils.isNoneBlank(v.getAs[String]("imei")) => "IM: "+v.getAs[String]("imei")
    case v if StringUtils.isNoneBlank(v.getAs[String]("mac")) => "MC: "+v.getAs[String]("mac")
    case v if StringUtils.isNoneBlank(v.getAs[String]("idfa")) => "ID: "+v.getAs[String]("idfa")
    case v if StringUtils.isNoneBlank(v.getAs[String]("openudid")) => "OD: "+v.getAs[String]("openudid")
    case v if StringUtils.isNoneBlank(v.getAs[String]("androidid")) => "AD: "+v.getAs[String]("androidid")
    case v if StringUtils.isNoneBlank(v.getAs[String]("imeimd5")) => "IMMD5: "+v.getAs[String]("imeimd5")
    case v if StringUtils.isNoneBlank(v.getAs[String]("macmd5")) => "MCMD5: "+v.getAs[String]("macmd5")
    case v if StringUtils.isNoneBlank(v.getAs[String]("idfamd5")) => "IDMD5: "+v.getAs[String]("idfamd5")
    case v if StringUtils.isNoneBlank(v.getAs[String]("openudidmd5")) => "ODMD5: "+v.getAs[String]("openudidmd5")
    case v if StringUtils.isNoneBlank(v.getAs[String]("androididmd5")) => "ADMD5: "+v.getAs[String]("androididmd5")
    case v if StringUtils.isNoneBlank(v.getAs[String]("imeisha1")) => "IMS1: "+v.getAs[String]("imeisha1")
    case v if StringUtils.isNoneBlank(v.getAs[String]("macsha1")) => "MCS1: "+v.getAs[String]("macsha1")
    case v if StringUtils.isNoneBlank(v.getAs[String]("idfasha1")) => "IDS1: "+v.getAs[String]("idfasha1")
    case v if StringUtils.isNoneBlank(v.getAs[String]("openudidsha1")) => "ODS1: "+v.getAs[String]("openudidsha1")
    case v if StringUtils.isNoneBlank(v.getAs[String]("androididsha1")) => "ADS1: "+v.getAs[String]("androididsha1")
    }
  }
}
