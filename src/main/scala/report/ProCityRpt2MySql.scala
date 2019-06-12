package report

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

object ProCityRpt2MySql {
  def main(args: Array[String]): Unit = {
    if(args.length!=1)
    {
      println("非法参数")
      sys.exit()
    }

    val Array(logInputPath) = args

    val conf = new SparkConf()
    conf.setAppName(s"${this.getClass.getSimpleName}")
    conf.setMaster("local[4]")

    //RDD序列化到磁盘
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df : DataFrame = sqlContext.read.parquet(logInputPath)
    df.createOrReplaceTempView("t1")

    val result:DataFrame = sqlContext.sql("select provincename,cityname,count(*) count from t1 group by provincename,cityname")

    val load = ConfigFactory load()
    val props = new Properties()

    /*result.show(50)*/

    props.setProperty("user",load.getString("jdbc.user"))
    props.setProperty("password",load.getString("jdbc.password"))

    result.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"),load.getString("jdbc.tableName"),props)

    sc.stop()

  }
}
