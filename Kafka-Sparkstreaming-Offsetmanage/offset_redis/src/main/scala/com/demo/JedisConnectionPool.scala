package com.demo
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}
object JedisConnectionPool {
  //连接配置
  val config = new JedisPoolConfig()
  //最大连接数,
  config.setMaxTotal(20)
  //最大空闲连接数
  config.setMaxIdle(10)
  //当调用borrow Object方法时，是否进行有效性检查 -->
  config.setTestOnBorrow(true)
  //10000代表超时时间（10秒）
  val pool = new JedisPool(config, "10.103.104.181", 6379, 10000, "666666")
  def getConnection(): Jedis = {
    pool.getResource
  }
}
