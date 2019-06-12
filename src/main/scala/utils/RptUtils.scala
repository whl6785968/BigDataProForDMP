package utils

object RptUtils {
  def calcReq(reqMode:Int,prcMode:Int):List[Double]=
  {
    if(reqMode == 1 && prcMode == 1)
    {
      List[Double](1,0,0)
    }
    else if (reqMode == 1 && prcMode == 2)
    {
      List[Double](1,1,0)
    }
    else if(reqMode == 1 && prcMode == 3)
    {
      List[Double](1,1,1)
    }
    else List[Double](0,0,0)

  }

  def calcRtb(effective:Int,bill:Int,bid:Int,orderId:Int,win:Int,winPrice:Double,adPayMent:Double): List[Double] =
  {
    if(effective == 1 && bill == 1 && bid == 1 && orderId != 0)
    {
      List[Double](1,0,0,0)
    }
    else if (effective ==1 && bill == 1 && win == 1)
    {
      List[Double](0,1,winPrice/1000.0,adPayMent/1000.0)
    }
    else List[Double](0,0,0,0)
  }

  def calcShow_Click(reqMode:Int,effective:Int):List[Double]={
    if(reqMode == 2 && effective == 1)
    {
      List[Double](1,0)
    }
    else if(reqMode == 3 && effective == 1)
    {
      List[Double](0,1)
    }
    else List[Double](0,0)
  }

}
