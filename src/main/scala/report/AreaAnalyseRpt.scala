package report

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}

object AreaAnalyseRpt {
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

    val df: DataFrame = sqlContext.read.parquet(logInputPath)
    //    df.show(50)
    df.createOrReplaceTempView("t1")
    val result = sqlContext.sql(
      """
        |select
        |provincename, cityname,
        |sum(case when requestmode=1 and processnode >=2 then 1 else 0 end) ER,
        |sum(case when requestmode=1 and processnode =3 then 1 else 0 end) AR,
        |sum(case when iseffective=1 and isbilling=1 and isbid=1 and adorderid !=0 then 1 else 0 end) NOB,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 then 1 else 0 end) SNOB,
        |sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) NOS,
        |sum(case when requestmode=3 and iseffective=1 then 1 else 0 end) NOC,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 then 1.0*adpayment/1000 else 0 end) COA,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 then 1.0*winprice/1000 else 0 end) CONSUMEOA
        |from t1
        |group by provincename, cityname
      """.stripMargin)


    //    result.show(50)
    val load = ConfigFactory.load()
    val props = new Properties()

    /*result.show(50)*/

    props.setProperty("user", load.getString("jdbc.user"))
    props.setProperty("password", load.getString("jdbc.password"))

    result.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"), load.getString("jdbc.arearpt.table"), props)

    /*sc.stop()*/
  }

}
