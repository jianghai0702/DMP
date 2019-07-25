package utils
import java.net.URLEncoder

import com.alibaba.fastjson.JSON
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils


object HttpUtil {
//  def main(args: Array[String]): Unit = {
//    val poitype = "写字楼|学校"
//    val poitype2 = URLEncoder.encode(poitype)
//
//    val url = "https://restapi.amap.com/v3/geocode/regeo?key=cac824e54691942975fd4fbf9131564e&location=120.3423821926,30.3325184913&poitype="+poitype2
//    println(url)
//    println("This get response: ")
//    println(get(url))
//
////    val postUrl = "http://192.168.1.00:8082/risk/group"
////    val params = """{"company_list":["北京佛尔斯特金融信息服务有限公司"],"conditions":{}}"""
////    println("This post response: ")
////    println(post(postUrl, params, """{"Content-Type": "application/json"}"""))
//  }

  /**
    * GET请求
    * @param url
    * @param header
    * @return json字符串
    */
  def get(url: String, header: String = null): String  ={
    val httpClient = HttpClients.createDefault()
    val httpGet = new HttpGet(url)

    // 设置 header
    if (header != null) {
      val json = JSON.parseObject(header)
      json.keySet()
        .toArray.map(_.toString)
        .foreach(key => httpGet.setHeader(key, json.getString(key)))
    }
    // 发送请求
    val response: CloseableHttpResponse = httpClient.execute(httpGet)
    // 获取返回结果
    EntityUtils.toString(response.getEntity,"UTF-8")
  }


  /**
    * POST请求
    * @param url
    * @param params
    * @param header
    * @return json字符串
    */
  def post(url: String, params: String = null, header: String = null): String ={
    val httpClient = HttpClients.createDefault()    // 创建 client 实例
    val post = new HttpPost(url)    // 创建 post 实例

    // 设置 header
    if (header != null) {
      val json = JSON.parseObject(header)
      json.keySet()
        .toArray
        .map(_.toString)
        .foreach(key => post.setHeader(key, json.getString(key)))
    }

    if (params != null) {
      post.setEntity(new StringEntity(params, "UTF-8"))
    }
    // 创建 client 实例
    val response: CloseableHttpResponse = httpClient.execute(post)
    // 获取返回结果
    EntityUtils.toString(response.getEntity, "UTF-8")
  }
}
