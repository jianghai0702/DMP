package tag

import ch.hsr.geohash.GeoHash
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._

object ContextTag {
  val kInputPath = "C:\\JiangHai\\hadoophome\\parquet\\01"
  val kOutputPath = "C:\\JiangHai\\hadoophome\\report\\03"
  val kAppPath = "C:\\Users\\jiang\\Desktop\\03_互联网广告项目\\spark_dmp\\src\\main\\scala\\tag\\app_dict.txt"
  val kStopwordsPath = "C:\\Users\\jiang\\Desktop\\03_互联网广告项目\\spark_dmp\\src\\main\\scala\\tag\\stopwords.log"
  val kGeoHashPath = "C:\\Users\\jiang\\Desktop\\03_互联网广告项目\\spark_dmp\\src\\main\\scala\\tag\\GeoHashDict.txt"


  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\JiangHai\\soft\\hadoop-2.8.1-windows\\hadoop-2.8.1")
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)

//    val spark = SparkSession
//      .builder()
//      .appName(this.getClass.getName)
//      .master("local[*]")
//      .getOrCreate()
//    import spark.implicits._
//    import org.apache.spark.sql.functions._




    // appMap[appid, appname]
    val appMap: Map[String, String] = sc.textFile(kAppPath)
      .filter(line=>{
        val fields: Array[String] = line.split("\\s")
        fields.length>=5
      })
      .map(line => {
        val fields: Array[String] = line.split("\\s")
        val appid = fields(4).toString
        val appname = fields(1).toString
        (appid, appname)
      }).collect().toMap
    val appMapBroadcast: Broadcast[Map[String, String]] = sc.broadcast(appMap)


    //stopwordMap
    val stopKeywords: Array[String] = sc.textFile(kStopwordsPath)
      .filter(line => true)
      .collect()
    val stopKeywordsBroadcast: Broadcast[Array[String]] = sc.broadcast(stopKeywords)





    sQLContext.read.parquet(kInputPath)
      .filter(
        """
          |long > 0 and long < 180 and lat > 0 and lat < 90
        """.stripMargin)
      //.filter(TagUtil.userFilterCondition)
      .rdd
      .map(row=>{
        // 获取用户id
        val userid = TagUtil.getUserId(row)
        // 根据每条数据，打上对应的标签（7个标签）
        // 广告标签 adspacetype
        val list4AdTag: List[(String, Int)] = AdTag.makeTag(row)
        // APP标签 appid appname
        val list4AppTag: List[(String, Int)] = AppTag.makeTag(row, appMapBroadcast)
        // 渠道 adplatformproviderid
        val list4PlatformTag: List[(String, Int)] = PlatformTag.makeTag(row)
        // 设备 client networkmannerid  networkmannername ispid  ispname
        val list4DeviceTag: List[(String, Int)] = DeviceTag.makeTag(row)
        // 关键字 keywords
        val list4KeywordTag: List[(String, Int)] = AppTag.makeTag(row, stopKeywordsBroadcast)
        // 地域标签 provincename  cityname
        val list4LocationTag: List[(String, Int)] = LocationTag.makeTag(row)
        // 商圈 经度long 纬度lat
        val list4BusinessTag: List[(String, Int)] = BusinessTag.makeTag(row)
        ("1", list4BusinessTag)
      }).foreach(println)


    sc.stop()
  }
}

