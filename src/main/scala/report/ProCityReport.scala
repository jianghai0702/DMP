package report

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}


/**
  * 地域报告
  *
  * 1.统计省市数据量分布情况
  *   1.将统计的结果输出成json格式文件
  *   2.将 统计结果输出到mysql
  *
  */
object ProCityReport {
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


//    val maped: RDD[((String, String), Int)] = parquet.rdd.map(row => {
//      val provincename = row.getAs[String]("provincename")
//      val cityname = row.getAs[String]("cityname")
//      ((provincename, cityname), 1)
//    })
//    val reduce: RDD[((String, String), Int)] = maped.reduceByKey(_+_)



    // 注册表
    parquet.createOrReplaceTempView("log")
    val result = sQLContext.sql("select provincename, cityname, count(*) count from log group by provincename, cityname")
    // result.foreach(row => println(row))



    // json存储
    // result.repartition(1).write.json(kOutputPath)



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
