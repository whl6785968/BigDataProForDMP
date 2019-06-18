package Tags

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import utils.{JedisPools, TagsUtils}

import scala.collection.mutable.ListBuffer

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
    val data = sparkSql.read.parquet(logInputPath).where(TagsUtils.hasSomeUserIdCondition)
    val ve = data.rdd.mapPartitions(par => {
      val listBuffer = new collection.mutable.ListBuffer[(Long, (ListBuffer[String],List[(String,Int)]))]()

      /*val jedis = JedisPools.getJedis()*/
      par.foreach(row => {
        val ads = Tags4Ads.makeTags(row)
        val apps = Tags4APP.makeTags(row, broadCastDictMap.value)
        val devices = Tags4Device.makeTags(row)
        val kws = Tags4Kw.makeTags(row, stopMapBc.value)
      /*  val business = Tag4Business.makeTags(row, jedis)*/
        val userId = TagsUtils.getAllUserId(row)

        val tags = (ads ++ apps ++ devices ++ kws).toList
        //ListBuffer((IMEI ,List((LC,1),(LN,1),(CN,1),(APP,1),(...)))
        /* listBuffer.append((userId(0),(ads ++ apps ++ devices ++ kws ++ business).toList))*/
        listBuffer.append((userId(0).hashCode().toLong, (userId,tags)))
      })
//      jedis.close()
      listBuffer.iterator
    })

    val edge = data.rdd.flatMap(row => {
      val allUserId:ListBuffer[String] = TagsUtils.getAllUserId(row)
      allUserId.map(uid => Edge(allUserId(0).hashCode.toLong,uid.hashCode.toLong,0))
    })

    val graph = Graph(ve,edge)

    val cc = graph.connectedComponents().vertices

    cc.join(ve).map{
      case (xxid,(cmId,(uidList,tags))) => {
        (cmId,(uidList,tags))
      }
    }.reduceByKey{
      case (a,b) => {
        val uids = a._1++b._1
        val tags = (a._2++b._2).groupBy(_._1).mapValues(_.foldLeft(0)(_+_._2)).toList
        (uids.distinct,tags)
      }
    }.saveAsTextFile(resultOutputPath)
      //(IMEI ,List((LC,1),(LN,1),(CN,1),(APP,1),(...))
      //(IMEI,(list1,list2))
     /* .reduceByKey((a,b) => {
      //(a ++ b) -> list((LC,1),(LC,1),(LD,1))
      //(LC,((LC,1),(LC,1))) => (LC,2)
      (a ++ b).groupBy(_._1).map{
        case (k,sameTags) => (k,sameTags.map(_._2).sum)
      }.toList
    }).saveAsTextFile(resultOutputPath)*/




  }
}
