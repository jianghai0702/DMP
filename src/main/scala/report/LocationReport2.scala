package report

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import report.LocationReport.kInputPath

object LocationReport2 {
  val kInputPath = "C:\\JiangHai\\hadoophome\\parquet\\01"
  val kOutputPath = "C:\\JiangHai\\hadoophome\\report\\05"



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

    val mapped = parquet.rdd.map(row => {
      val provincename = row.getAs[String]("provincename")
      val cityname = row.getAs[String]("cityname")
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val androidid = row.getAs[String]("androidid")
      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")

      // 原始请求、有效请求、广告请求
      var list1: List[Double] = null
      if (requestmode == 1 && processnode >= 1) list1 = List[Double](1, 0, 0)
      else if (requestmode == 1 && processnode >= 2) list1 = List[Double](1, 1, 0)
      else if (requestmode == 1 && processnode == 3) list1 = List[Double](1, 1, 1)
      else list1 = List[Double](0, 0, 0)


      // 参与竞价、竞价成功
      // 注意：这是2个不同的请求
      var list2: List[Double] = null
      if (iseffective == 1 && isbilling == 1 && isbid == 1) list2 = List[Double](1, 0)
      else if (iseffective == 1 && isbilling == 1 && iswin == 1 && androidid != 0) list2 = List[Double](0, 1)
      else list2 = List[Double](0, 0)


      // 展示、点击
      // 注意：这是2个不同的请求
      var list3: List[Double] = null
      if (requestmode == 2 && iseffective == 1) list3 = List[Double](1, 0)
      else if (requestmode == 3 && iseffective == 1) list3 = List[Double](0, 1)
      else list3 = List[Double](0, 0)


      // DSP消费、DSP成本
      // 注意：这是2个不同的请求
      var list4: List[Double] = null
      if (iseffective == 1 && isbilling == 1 && iswin == 1) list4 = List[Double](winprice / 1000.0, adpayment / 1000.0)
      else list4 = List[Double](0, 0)

      ((provincename, cityname), list1 ++ list2 ++ list3 ++ list4)
    })


    // 聚合操作
    mapped.reduceByKey((list1, list2) => {
      list1.zip(list2).map(p1 => p1._1 + p1._2)
    }).coalesce(1).saveAsTextFile(kOutputPath)


    sc.stop()
  }
}
