package tag

trait Tag {

  /**
    * 打标签接口
    */
  def makeTag(args:Any*): List[(String,Int)]
}
