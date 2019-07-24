package utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import pojo.LogV2


/**
  * 日志转成parquet文件格式
  *
  * 采用snappy压缩
  */
object Bz2ParquetUtilV2 {

  val kInputPath = "C:\\Users\\jiang\\Desktop\\03_互联网广告项目\\spark_dmp\\src\\main\\resources\\2016-10-01_06_p1_invalid.1475274123982.log"
  val kOutputPath = "C:\\JiangHai\\hadoophome\\02"


  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\JiangHai\\soft\\hadoop-2.8.1-windows\\hadoop-2.8.1")
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
      // kyro序列化机制
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      // 注册自定义类
      //.registerKryoClasses(Array(classOf[Log]))


    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    sQLContext.setConf("spark.sql.parquet.compression.codec", "snappy")


    val log: RDD[LogV2] = sc.textFile(kInputPath)
      .map(line => line.split(",", -1))
      .filter(arr => arr.length >= 85)
      .map(arr => LogV2(arr))


    val df = sQLContext.createDataFrame(log)
    df.write.mode(SaveMode.Append).partitionBy("provincename", "cityname").parquet(kOutputPath)
    sc.stop()
  }
}
