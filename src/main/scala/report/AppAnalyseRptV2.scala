package report

import bean.Log
import org.apache.commons.lang.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import utils.{JedisPools, RptUtils2}

object AppAnalyseRptV2 {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println(
        """非法参数
          |正确的参数:
          |   LogInputPath
          |   ResultOutputPath
        """.stripMargin)
      sys.exit()
    }

    val Array(logInputPath,resultOutputPath) = args

    val conf = new SparkConf()
    conf.setAppName(s"${this.getClass.getSimpleName}")
    conf.setMaster("local[4]")

    //RDD序列化到磁盘
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    sc.textFile(logInputPath).map(line => {
      line.split(",",-1)
    }).filter(_.length>=85).map(Log(_)).filter(log => !log.appid.isEmpty || !log.appname.isEmpty)
      .mapPartitions(itr => {
        //使用partition资源只打开一次，若使用map，每次都需要重新打开和创建资源
        val jedis = JedisPools.getJedis()
        val parResult = new collection.mutable.ListBuffer[(String,List[Double])]

        //这里的itr就相当于每个包含一定数量数据的分区
        itr.foreach(log => {
          var newAppName = log.appname
          if(!StringUtils.isNotEmpty(newAppName)){
            newAppName = jedis.get(log.appid)
          }

          val req = RptUtils2.caculateReq(log.requestmode,log.processnode)
          val rtb = RptUtils2.caculateRtb(log.iseffective,log.isbilling,log.isbid,log.adorderid,log.iswin,log.winprice,log.adpayment)
          val showClick = RptUtils2.caculateShowClick(log.requestmode,log.iseffective)

          parResult += ((newAppName,req++rtb++showClick))
        })

        jedis.close()

        parResult.iterator


      })
      .reduceByKey((list1,list2) => {
        list1.zip(list2).map(t => t._1+t._2)
      }).map(t => t._1 + "," + t._2.mkString(",")).saveAsTextFile(resultOutputPath)
  }

}
