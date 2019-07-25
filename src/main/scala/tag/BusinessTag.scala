package tag

import ch.hsr.geohash.GeoHash
import org.apache.spark.sql.Row
import scalikejdbc.config.DBs
import scalikejdbc.{DB, SQL}
import utils.{AmapUtil, JdbcUtil, StrUtil}

object BusinessTag extends Tag {

  /**
    * 加载配置
    */
  DBs.setup() //加载默认配置 db.default.*
  //DBs.setupAll() //加载所有配置
  DBs.setup('project3) //加载自定义配置 db.dmp.*


  /**
    * 打标签接口
    */
  override def makeTag(args: Any*): List[(String, Int)] = {
    // 商圈 经度 long 纬度 lat
    var list = List[(String,Int)]()
    // 解析参数
    val row = args(0).asInstanceOf[Row]

    // 过滤
    // 纬度范围：3°51′N至53°33′N
    // 经度范围：73°33′E至135°05′E
    val long = row.getAs[String]("long").toDouble
    val lat = row.getAs[String]("lat").toDouble
    if (long<73 || long>136 || lat<3 || lat>54) {
      println("经纬度不在指定范围内: long="+long+" lat="+lat )
      return null
    }


    //通过经纬度获取商圈
    val business = getBusiness(long,lat)
    if (StrUtil.isNotEmpty(business)){
      val fields = business.split(",")
      fields.iterator.foreach(str=>{
        list:+=(str,1)
      })
    }
    list
  }


  /**
    * 获取商圈信息
    * @param long
    * @param lat
    */
  def getBusiness(long:Double, lat:Double): String ={
    // 过滤
    // 纬度范围：3°51′N至53°33′N
    // 经度范围：73°33′E至135°05′E
    if (long<73 || long>136 || lat<3 || lat>54) {
      println("经纬度不在指定范围内: long="+long+" lat="+lat )
      return null
    }

    val geohash = GeoHash.geoHashStringWithCharacterPrecision(lat,long,8)
    //去数据库查找
    var business = jdbc_queryBusiness(geohash)

    //去高德请求
    if (business==null || business.length==0){
      business = AmapUtil.getBusinessFromAMap(long,lat)

      //如果走高德，则将请求结果存入数据库
      if (business!=null && business.length>0){
        jdbc_insertBusiness(geohash, business)
      }
    }
    business
  }


  /**
    * 插入商圈
    */
  def jdbc_insertBusiness(geohash:String, business:String): Unit ={
    if (geohash == null || geohash.length == 0) return null
    if (business == null || business.length == 0) return null

    DB.autoCommit(implicit session => {
      SQL("insert into business(geohash, business) values(?,?)")
        .bind(geohash, business)
        .update()
        .apply()
    })
  }

  /**
    * 查询商圈
    * @param geohash
    * @return
    */
  def jdbc_queryBusiness(geohash:String): String ={
    if (geohash == null || geohash.length == 0) return null

    var list: List[String] = DB.readOnly(implicit session => {
      SQL("select geohash, business from business where geohash = ?")
        .bind(geohash)
        .map(rs =>{
          var business = rs.string("business")
          if (business==null || business.length==0) business="未知"
          business
        })
        .list()
        .apply()
    })

    if (list==null || list.length==0) return null
    list(0)
  }

}
