package tools

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import scala.collection.mutable.ListBuffer

object Ceshi {
  val kInputPath = "C:\\Users\\jiang\\Desktop\\03_互联网广告项目\\spark_dmp\\src\\main\\resources\\json.txt"
  val kOutputPath = "C:\\JiangHai\\hadoophome\\report\\03"



  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\JiangHai\\soft\\hadoop-2.8.1-windows\\hadoop-2.8.1")
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)


    val file = sc.textFile(kInputPath)


//
//    val ffff = file.map(line => {
//      val listBuffer = collection.mutable.ListBuffer[(String,Int)]()
//      val rootDict: JSONObject = JSON.parseObject(line)
//      val status = rootDict.getIntValue("status")
//
//
//      if (status == 1) {
//        val regeocodeJson = rootDict.getJSONObject("regeocode")
//        if (regeocodeJson != null && !regeocodeJson.isEmpty) {
//
//          val poisArr = regeocodeJson.getJSONArray("pois")
//          if (poisArr != null && !poisArr.isEmpty) {
//
//            for (item <- poisArr.toArray) {
//              if (item.isInstanceOf[JSONObject]) {
//                val dict = item.asInstanceOf[JSONObject]
//                val businessarea = dict.getString("businessarea")
//
//                if (businessarea != null && businessarea.length > 0 && !businessarea.equalsIgnoreCase("[]")) {
//                  listBuffer.append( (businessarea,1) )
//                }
//
//              }
//            }
//          } //end poisArr
//
//        } //end regeocodeJson
//      }
//      listBuffer
//    })
//
//    ffff.flatMap(list=> list)
//      .filter(p1=> p1._1.length>0)
//      .reduceByKey(_+_)
//      .foreach(println)


    /**
      *
      */
    val ffff2 = file.map(line => {
      val listBuffer = collection.mutable.ListBuffer[(String,Int)]()
      val rootDict: JSONObject = JSON.parseObject(line)
      val status = rootDict.getIntValue("status")


      if (status == 1) {
        val regeocodeJson = rootDict.getJSONObject("regeocode")
        if (regeocodeJson != null && !regeocodeJson.isEmpty) {

          val poisArr = regeocodeJson.getJSONArray("pois")
          if (poisArr != null && !poisArr.isEmpty) {

            for (item <- poisArr.toArray) {
              if (item.isInstanceOf[JSONObject]) {
                val dict = item.asInstanceOf[JSONObject]
                val businessarea = dict.getString("type")

                if (businessarea != null && businessarea.length > 0 && !businessarea.equalsIgnoreCase("[]")) {
                  val types = businessarea.split(";")
                  if (types!=null && types.length>0){
                    for (typestr <- types){
                      listBuffer.append( (typestr,1) )
                    }

                  }

                }

              }
            }
          } //end poisArr

        } //end regeocodeJson
      }
      listBuffer
    })

    ffff2.flatMap(list=> list)
      .filter(p1=> p1._1.length>0)
      .reduceByKey(_+_)
      .foreach(println)


    sc.stop()
  }
}
