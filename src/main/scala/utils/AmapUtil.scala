package utils

import java.util.Properties

import com.alibaba.fastjson
import com.alibaba.fastjson.{JSON}


object AmapUtil {


  def main(args: Array[String]): Unit = {
//    val str = getBusinessArea(120.1979398727, 30.2307984999)
//    println(str)
  }


  /**
    * 从高德地图获取商圈信息
    *
    * @param long 经度 [-180, 180]
    * @param lat [-90, 90]
    * @return 多个结果以，分隔
    */
  def getBusinessFromAMap(long:Double, lat:Double): String ={
    // 过滤
    // 纬度范围：3°51′N至53°33′N
    // 经度范围：73°33′E至135°05′E
    if (long<73 || long>136 || lat<3 || lat>54) {
      println("经纬度不在指定范围内: long="+long+" lat="+lat )
      return null
    }

    // 加载配置
    // https://restapi.amap.com/v3/geocode/regeo?key=cac824e54691942975fd4fbf9131564e&location=120.1979398727,30.2307984999&extensions=base
    val prop = new Properties()
    prop.load(this.getClass.getClassLoader.getResourceAsStream("amap.properties"))
    val url = prop.getProperty("amap.url")
    val key = prop.getProperty("amap.key")
    val extensions = prop.getProperty("amap.extensions")
    val location = long+","+lat
    val urlstr = url  +"?key="+key  +"&location="+location   +"&extensions="+extensions

    // 调用amap接口
    val jsonstr = HttpUtil.get(urlstr)
    val rootDict = JSON.parseObject(jsonstr)

    // 解析json
    var result = collection.mutable.ListBuffer[String]()
    val status = rootDict.getIntValue("status")
    if (status==0) return null

    val regeocodeDict = rootDict.getJSONObject("regeocode")
    if (regeocodeDict==null || regeocodeDict.keySet().isEmpty) return null

    val addressComponentDict = regeocodeDict.getJSONObject("addressComponent")
    if (addressComponentDict==null || addressComponentDict.keySet().isEmpty) return null

    val businessAreasArr = addressComponentDict.getJSONArray("businessAreas")
    if (businessAreasArr==null || businessAreasArr.isEmpty) return null

    for (item <- businessAreasArr.toArray()){
      if (item.isInstanceOf[fastjson.JSONObject]){
        val item2 = item.asInstanceOf[fastjson.JSONObject]
        val name = item2.getString("name")
        result.append(name)
      }
    }
    result.mkString(",")
  }







}
