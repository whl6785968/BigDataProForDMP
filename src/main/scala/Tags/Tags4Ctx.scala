package Tags

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import utils.TagsUtils

object Tags4Ctx {
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      println(
        """非法参数
          |正确的参数:
          |   LogInputPath
          |   dictInputPath
          |   stopInputPath
          |   ResultOutputPath
        """.stripMargin)
      sys.exit()
    }

    val Array(logInputPath,dictInputPath,stopInputPath,resultOutputPath) = args

    val conf = new SparkConf()
    conf.setAppName(s"${this.getClass.getSimpleName}")
    conf.setMaster("local[4]")

    //RDD序列化到磁盘
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    val dictMap = sc.textFile(dictInputPath).map(line => {
      val fields = line.split("\t",-1)
      (fields(0),fields(1))
    }).collect().toMap

    val stopMap = sc.textFile(stopInputPath).map((_,0)).collect().toMap

    val broadCastDictMap = sc.broadcast(dictMap)
    val stopMapBc = sc.broadcast(stopMap)

    val sparkSql = SparkSession.builder().getOrCreate()

    sparkSql.read.parquet(logInputPath).where(TagsUtils.hasSomeUserIdCondition).rdd.mapPartitions(par => {
      val listBuffer = new collection.mutable.ListBuffer[(String,List[(String,Int)])]()

      par.foreach(row => {
        val ads = Tags4Ads.makeTags(row)
        val apps = Tags4APP.makeTags(row,broadCastDictMap.value)
        val devices = Tags4Device.makeTags(row)
        val kws = Tags4Kw.makeTags(row,stopMapBc.value)

        val userId = TagsUtils.getAllUserId(row)
        //ListBuffer((IMEI ,List((LC,1),(LN,1),(CN,1),(APP,1),(...)))
        listBuffer.append((userId(0),(ads ++ apps ++ devices ++ kws).toList))

      })
      listBuffer.iterator
    })
      //(IMEI ,List((LC,1),(LN,1),(CN,1),(APP,1),(...))
      .reduceByKey((a,b) => {
      //((LC,1),..,(),..)
      //(LC,((LC,1),(LC,1))) => (LC,2)
      (a ++ b).groupBy(_._1).map{
        case (k,sameTags) => (k,sameTags.map(_._2).sum)
      }.toList
    }).saveAsTextFile(resultOutputPath)




  }
}
