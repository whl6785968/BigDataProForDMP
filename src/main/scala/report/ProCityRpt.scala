package report

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

object ProCityRpt {
  def main(args: Array[String]): Unit = {
    if(args.length!=2)
    {
      println("非法参数")
      sys.exit()
    }

    val Array(logInputPath,resultOutputPath) = args

    val conf = new SparkConf()
    conf.setAppName(s"${this.getClass.getSimpleName}")
    conf.setMaster("local[4]")

    //RDD序列化到磁盘
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df : DataFrame = sqlContext.read.parquet(logInputPath)
    df.createOrReplaceTempView("t1")

    val result:DataFrame = sqlContext.sql("select provincename,cityname,count(*) from t1 group by provincename,cityname")

    result.coalesce(1).write.json(resultOutputPath)
    sc.stop()
  }
}
