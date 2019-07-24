package utils

object LocationReportUtil {

  // 处理原始、有效、广告请求方法
  def part01(requestmode:Int, processnode:Int): List[Double] ={
    if (requestmode == 1 && processnode == 1) List[Double](1,0,0)
    else if (requestmode == 1 && processnode == 2)  List[Double](1,1,0)
    else if (requestmode == 1 && processnode == 3)  List[Double](1,1,1)
    else  List[Double](0,0,0)
  }

  // 处理参与竞价数 竞价成功数 广告成品 广告消费
  def part02(
              iseffective:Int,
              isbilling:Int,
              isbid:Int,
              iswin:Int,
              adorderid:Int,
              winprice:Double,
              adpayment:Double
            ): Unit ={


  }
}
