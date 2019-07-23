package utils

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}

object Bz2ParquetUtil {

  val kInputPath = "C:\\Users\\jiang\\Desktop\\03_互联网广告项目\\spark_dmp\\src\\main\\resources\\2016-10-01_06_p1_invalid.1475274123982.log"
  val kOutputPath = "C:\\JiangHai\\hadoophome\\03"


  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\JiangHai\\soft\\hadoop-2.8.1-windows\\hadoop-2.8.1")
    val conf = new SparkConf()
      .setAppName("Bz2ParquetUtil")
      .setMaster("local[*]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    // 设置Spark sql 压缩方式，注意spark1.6版本默认不是snappy，到2.0以后是默认的压缩方式
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")


    val file: RDD[String] = sc.textFile(kInputPath)
    //file.foreach(println)

    val maped: RDD[Row] = file.map(line => {
      // 无法解析连续的逗号,,,,,,,，他会识别为一个元素，所以我们切割的时候，需要进行处理
      line.split("\\,", -1)
    }).filter(arr => {
      // 进行过滤，要保证字段大于八十五个，不然下面的处理会数组越界
      arr.length >= 85
    }).map(arr => {
      Row(
        arr(0),
        StringUtil.stringToInt(arr(1)),
        StringUtil.stringToInt(arr(2)),
        StringUtil.stringToInt(arr(3)),
        StringUtil.stringToInt(arr(4)),
        arr(5),
        arr(6),
        StringUtil.stringToInt(arr(7)),
        StringUtil.stringToInt(arr(8)),
        StringUtil.stringToDouble(arr(9)),
        StringUtil.stringToDouble(arr(10)),
        arr(11),
        arr(12),
        arr(13),
        arr(14),
        arr(15),
        arr(16),
        StringUtil.stringToInt(arr(17)),
        arr(18),
        arr(19),
        StringUtil.stringToInt(arr(20)),
        StringUtil.stringToInt(arr(21)),
        arr(22),
        arr(23),
        arr(24),
        arr(25),
        StringUtil.stringToInt(arr(26)),
        arr(27),
        StringUtil.stringToInt(arr(28)),
        arr(29),
        StringUtil.stringToInt(arr(30)),
        StringUtil.stringToInt(arr(31)),
        StringUtil.stringToInt(arr(32)),
        arr(33),
        StringUtil.stringToInt(arr(34)),
        StringUtil.stringToInt(arr(35)),
        StringUtil.stringToInt(arr(36)),
        arr(37),
        StringUtil.stringToInt(arr(38)),
        StringUtil.stringToInt(arr(39)),
        StringUtil.stringToDouble(arr(40)),
        StringUtil.stringToDouble(arr(41)),
        StringUtil.stringToInt(arr(42)),
        arr(43),
        StringUtil.stringToDouble(arr(44)),
        StringUtil.stringToDouble(arr(45)),
        arr(46),
        arr(47),
        arr(48),
        arr(49),
        arr(50),
        arr(51),
        arr(52),
        arr(53),
        arr(54),
        arr(55),
        arr(56),
        StringUtil.stringToInt(arr(57)),
        StringUtil.stringToDouble(arr(58)),
        StringUtil.stringToInt(arr(59)),
        StringUtil.stringToInt(arr(60)),
        arr(61),
        arr(62),
        arr(63),
        arr(64),
        arr(65),
        arr(66),
        arr(67),
        arr(68),
        arr(69),
        arr(70),
        arr(71),
        arr(72),
        StringUtil.stringToInt(arr(73)),
        StringUtil.stringToDouble(arr(74)),
        StringUtil.stringToDouble(arr(75)),
        StringUtil.stringToDouble(arr(76)),
        StringUtil.stringToDouble(arr(77)),
        StringUtil.stringToDouble(arr(78)),
        arr(79),
        arr(80),
        arr(81),
        arr(82),
        arr(83),
        StringUtil.stringToInt(arr(84))
      )
    })

    // 构建DF
    val df: DataFrame = sqlContext.createDataFrame(maped, SchemaUtil.logStructType)

    // df.write.mode(SaveMode.Overwrite).parquet(kOutputPath)
    // df.write.mode(SaveMode.Overwrite).partitionBy("provincename", "cityname").parquet(kOutputPath)
    // df.write.mode(SaveMode.Overwrite).partitionBy("provincename", "cityname").json(kOutputPath)
    df.write.mode(SaveMode.Overwrite).jdbc()
    sc.stop()
  }
}
