package tools

import org.apache.spark.{SparkConf, SparkContext}
import utils.JedisPools

object AppDict2Redis {
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
    val sc = new SparkContext(conf)

    //foreachPartition 针对每个分区集合进行计算，能够提高计算性能
    //如果有IO流、打开关闭资源操作，可以使用foreachPartition
    sc.textFile(logInputPath).map(line =>{
      val fields = line.split("\t",-1)
        (fields(4),fields(1))
      }).foreachPartition(itr => {
        val jedis = JedisPools.getJedis()

        itr.foreach(t=> {
          jedis.set(t._1,t._2)
        })

        jedis.close()
    })
  }

}
