package report

import bean.Log
import org.apache.commons.lang.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import utils.RptUtils2

object AppAnalyseRpt {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("非法参数")
      sys.exit()
    }

    val Array(logInputPath,dicInputPath,resultOutputPath) = args

    val conf = new SparkConf()
    conf.setAppName(s"${this.getClass.getSimpleName}")
    conf.setMaster("local[4]")

    //RDD序列化到磁盘
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = SparkSession.builder().appName(s"${this.getClass.getSimpleName}").config(conf).getOrCreate()

    //appId --> appName
    val dictMap = sc.textFile(dicInputPath).map(line => {
      val fields = line.split("\t",-1)
      (fields(4),fields(1))
    }).collect().toMap

    val broadcast = sc.broadcast(dictMap)

    sc.textFile(logInputPath).map(line => line.split(",",-1))
      .filter(_.length>=85).map(Log(_)).filter(log => !log.appid.isEmpty || !log.appname.isEmpty)
      .map(log => {
        var newAppname = log.appname
        if(!StringUtils.isNotEmpty(newAppname))
          {
            newAppname = broadcast.value.getOrElse(log.appid,"unknown")
          }
        val req = RptUtils2.caculateReq(log.requestmode,log.processnode)
        val rtb = RptUtils2.caculateRtb(log.iseffective,log.isbilling,log.isbid,log.adorderid,log.iswin,log.winprice,log.adpayment)
        val showClick = RptUtils2.caculateShowClick(log.requestmode,log.iseffective)
        (newAppname,req++rtb++showClick)
      })
      .reduceByKey((list1,list2) => {
        list1.zip(list2).map(t => t._1+t._2)
    }).map(t => t._1 + "," + t._2.mkString(",")).saveAsTextFile(resultOutputPath)

  }

}
