package report

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.RptUtils

object AreaAnalyseRptV2 {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("非法参数")
      sys.exit()
    }

    val Array(logInputPath) = args

    val conf = new SparkConf()
    conf.setAppName(s"${this.getClass.getSimpleName}")
    conf.setMaster("local[4]")

    //RDD序列化到磁盘
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    /* val sc = new SparkContext(conf)
     val sqlContext = new SQLContext(sc)*/
    val sqlContext = SparkSession.builder().appName(s"${this.getClass.getSimpleName}").config(conf).getOrCreate()

    val parquetData: DataFrame = sqlContext.read.parquet(logInputPath)
    /*import sqlContext.implicits._*/
   /* df.map(row => {
      val reqMode = row.getAs[Int]("requestmode")
      val prcMode = row.getAs[Int]("processmode")
      val effective = row.getAs[Int]("iseffectivce")
      val bill = row.getAs[Int]("sibilling")
      val bid = row.getAs[Int]("isbid")
      val orderId = row.getAs[Int]("adorderid")
      val win = row.getAs[Int]("iswin")
      val winPrice = row.getAs[Int]("winprice")
      val adPayMent = row.getAs[Int]("adpayment")

      val reqList = RptUtils.calcReq(reqMode,prcMode)
      val billList = RptUtils.calcRtb(effective,bill,bid,orderId,win,winPrice,adPayMent)
      val scList = RptUtils.calcShow_Click(reqMode,effective)

      ((row.getAs[String]("provincename"), row.getAs[String]("cityname")), reqList++billList++scList)

    })*/
  /*  parquetData.map(row =>{
      // 是不是原始请求，有效请求，广告请求 List(原始请求，有效请求，广告请求)
      val reqMode = row.getAs[Int]("requestmode")
      val prcNode = row.getAs[Int]("processnode")
      // 参与竞价, 竞价成功  List(参与竞价，竞价成功, 消费, 成本)
      val effTive = row.getAs[Int]("iseffective")
      val bill = row.getAs[Int]("isbilling")
      val bid = row.getAs[Int]("isbid")
      val orderId = row.getAs[Int]("adorderid")
      val win = row.getAs[Int]("iswin")
      val winPrice = row.getAs[Double]("winprice")
      val adPayMent = row.getAs[Double]("adpayment")


      val reqList = RptUtils.calcReq(reqMode, prcNode)
      val rtbList = RptUtils.calcRtb(effTive,bill,bid,orderId,win,winPrice,adPayMent)
      val showClickList = RptUtils.calcShow_Click(reqMode,effTive)

      // 返回元组
      ((row.getAs[String]("provincename"), row.getAs[String]("cityname")), reqList++rtbList++showClickList)
    })*/





  }
}
