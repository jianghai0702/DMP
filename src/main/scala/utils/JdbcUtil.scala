package utils

import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs

object JdbcUtil {

  /**
    * 加载配置
    */
  DBs.setup() //加载默认配置 db.default.*
  //DBs.setupAll() //加载所有配置
  DBs.setup('project3) //加载自定义配置 db.dmp.*


  /**
    * 插入数据
    */
  def insert(): Unit ={
    DB.autoCommit(implicit session => {
      SQL("insert into business(geohash, business) values(?,?)")
        .bind("1", "2")
        .update()
        .apply()
    })
  }


  def query(): List[(String,String)] ={
    var list: List[(String, String)] = DB.readOnly(implicit session => {
      SQL("select geohash, business from business where 1 = 1")
        .map(rs =>{
          val geohash = rs.string("geohash")
          val business = rs.string("business")
          (geohash,business)
        })
        .list()
        .apply()
    })
    list
  }

}
