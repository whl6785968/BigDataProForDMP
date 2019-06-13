package utils

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

object JedisPools {
  private val jedisPool = new JedisPool(new GenericObjectPoolConfig(),"192.168.231.129",6379,3000,null,2)

  def getJedis() = jedisPool.getResource
}
