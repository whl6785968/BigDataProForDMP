package tools

import bean.Log
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object Bzip2ParquetV2 {
  def main(args: Array[String]): Unit = {
    if(args.length!=3)
    {
      println("非法参数")
      sys.exit()
    }

    val Array(logInputPath,compressionCode,resultOutputPath) = args

    val conf = new SparkConf()
    conf.setAppName(s"${this.getClass.getSimpleName}")
    conf.setMaster("local[4]")

    //RDD序列化到磁盘
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.compression.codec",compressionCode)

    val rawData = sc.textFile(logInputPath).map(line => line.split(",",line.length)).filter(_.length>=85)
    val dataLog : RDD[Log] = rawData.map(arr => Log(arr))

    val df = sqlContext.createDataFrame(dataLog)

    df.write.partitionBy("provincename","cityname").parquet(resultOutputPath)

    sc.stop()


  }

}
