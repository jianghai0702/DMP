package tag

import ch.hsr.geohash.GeoHash
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
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
  val kDay = "20190729"


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




    // appMap[appid, appname] app列表
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


    //stopwordMap停用词
    val stopKeywords: Array[String] = sc.textFile(kStopwordsPath)
      .filter(line => true)
      .collect()
    val stopKeywordsBroadcast: Broadcast[Array[String]] = sc.broadcast(stopKeywords)



    // 加载HBase配置文件
    val load = ConfigFactory.load()
    val hbaseTableName = load.getString("hbase.table.Name")
    val zkHost = load.getString("hbase.zookeeper.host")
    // 创建Hadoop任务配置项
    val configuration = sc.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum",zkHost)
    // 创建conn连接
    val hbConn = ConnectionFactory.createConnection(configuration)
    val hbadmin = hbConn.getAdmin
    if(!hbadmin.tableExists(TableName.valueOf(hbaseTableName))){
      println("表可用")
      // 创建表的对象
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
      // 创建列簇
      val columnDescriptor = new HColumnDescriptor("tags")
      // 将列簇加载到表中
      tableDescriptor.addFamily(columnDescriptor)
      // 将创建好的表进行加载
      hbadmin.createTable(tableDescriptor)
      // 关闭
      hbadmin.close()
      hbConn.close()
    }
    // 加载HBASE相关属性配置
    val jobConf = new JobConf(configuration)
    // 指定Key输出的类型
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    // 是定输出到哪张表
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,hbaseTableName)




    sQLContext.read.parquet(kInputPath)
//      .filter(
//        """
//          |long > 0 and long < 180 and lat > 0 and lat < 90
//        """.stripMargin)
      .filter(TagUtil.userFilterCondition)
      .rdd
      .map(row=>{
        // 获取用户id
        val userid: String = TagUtil.getUserId(row)
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
        //val list4BusinessTag: List[(String, Int)] = BusinessTag.makeTag(row)
        (userid, list4AdTag++list4AppTag++list4PlatformTag++list4DeviceTag++list4KeywordTag++list4LocationTag)
      })
      .reduceByKey((list1,list2)=>{
        (list1++list2)
          .groupBy(p1=>p1._1)
          .mapValues(iter=>iter.size)
          .toList
      })
      //存入hbase
        .map(p1=>{
          p1 match {
            case (userid,userTags) =>{
              val put = new Put(Bytes.toBytes(userid)) //行键
              val tagsstr = userTags.map(p1=>p1._1+":"+p1._2).mkString(",") //k1:v1,k2:v2
              put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes(kDay),Bytes.toBytes(tagsstr))
              (new ImmutableBytesWritable(), put)
            }
          }
        })
        .saveAsHadoopDataset(jobConf)

    sc.stop()
  }
}

