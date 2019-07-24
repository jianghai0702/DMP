package report

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

object LocationReport {
  val kInputPath = "C:\\JiangHai\\hadoophome\\parquet\\01"
  val kOutputPath = "C:\\JiangHai\\hadoophome\\report\\03"



  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\JiangHai\\soft\\hadoop-2.8.1-windows\\hadoop-2.8.1")
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    sQLContext.setConf("spark.sql.parquet.compression.codec","snappy")
    val parquet: DataFrame = sQLContext.read.parquet(kInputPath) //源文件
    parquet.createOrReplaceTempView("log")

    val sql =
      """
        |select provincename, cityname,
        |sum( case when requestmode=1 and processnode>=1 then 1 else 0 end ) a,
        |sum( case when requestmode=1 and processnode>=2 then 1 else 0 end ) b,
        |sum( case when requestmode=1 and processnode=3  then 1 else 0 end ) c,
        |sum( case when iseffective=1 and isbilling=1 and isbid=1 then 1 else 0 end ) d,
        |sum( case when iseffective=1 and isbilling=1 and iswin=1 and androidid!=0 then 1 else 0 end ) e,
        |sum( case when requestmode=2 and iseffective=1 then 1 else 0 end ) f,
        |sum( case when requestmode=3 and iseffective=1 then 1 else 0 end ) g,
        |sum( case when iseffective=1 and isbilling=1 and iswin=1 then winprice/1000  else 0 end ) h,
        |sum( case when iseffective=1 and isbilling=1 and iswin=1 then adpayment/1000 else 0 end ) i
        |from log
        |group by provincename, cityname
      """.stripMargin
    val result: DataFrame = sQLContext.sql(sql)
    result.collect().map(println)



    // 加载配置文件
    val prop2 = new Properties()
    prop2.load(this.getClass.getClassLoader.getResourceAsStream("application.properties"))
    println(prop2.getProperty("jdbc.driver").toString)
    println(prop2.getProperty("jdbc.url").toString)
    println(prop2.getProperty("jdbc.user").toString)
    println(prop2.getProperty("jdbc.password").toString)




    // mysql存储
    // 加载application.conf  application.json   application.properties
    val config = ConfigFactory.load()
    val prop = new Properties()
    prop.setProperty("user", config.getString("jdbc.user") )
    prop.setProperty("password", config.getString("jdbc.password") )
    result.write
      .mode(SaveMode.Append)
      .jdbc(config.getString("jdbc.url") , "LocationReport3", prop)
    sc.stop()
  }
}
