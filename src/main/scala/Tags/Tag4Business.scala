package Tags

import ch.hsr.geohash.GeoHash
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

object Tag4Business extends Tags {
  override def makeTags(args: Any*): Map[String, Int] = {
    var map = Map[String,Int]()

    val row = args(0).asInstanceOf[Row]
    val jedis = args(1).asInstanceOf[Jedis]

    val lat = row.getAs[String]("lat")
    val longs = row.getAs[String]("long")

    if(StringUtils.isNotEmpty(lat) && StringUtils.isNotEmpty(longs)){
      val lat1 = lat.toDouble
      val longs1 = longs.toDouble

      if(lat1 > 3 && lat1<54 && longs1>73&&longs1<136){
        val geoHashCode = GeoHash.withBitPrecision(lat1,longs1,8).toBase32
        val business = jedis.get(geoHashCode)

        if(StringUtils.isNotEmpty(business)){
          business.split(",").foreach(bs => map += "BS"+bs -> 1)
        }
      }
    }
    map
  }
}
