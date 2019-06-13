package tools

import ch.hsr.geohash.GeoHash
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import utils.{GeoUtils, JedisPools}

object ExtractGeoDB {
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

    val sparkSql = SparkSession.builder().getOrCreate()

    sparkSql.read.parquet(logInputPath).select("lat","long")
      .where("lat > 3 and lat <54 and long >73 and long <136").distinct()
      .foreachPartition(itr => {
        val jedis = JedisPools.getJedis()

        itr.foreach(row => {
          val lat = row.getAs[String]("lat")
          val longs = row.getAs[String]("long")

          val geoHashCode = GeoHash.withCharacterPrecision(lat.toDouble,longs.toDouble,8).toBase32
          val business = GeoUtils.getBusiness(lat +","+longs)

          if(StringUtils.isNotEmpty(business))
            {
              jedis.set(geoHashCode,business)
            }
        })
        jedis.close()
      })
  }
}
