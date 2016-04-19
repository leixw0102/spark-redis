package com.ehl.spark.redis.rdd

import com.ehl.spark.redis.RedisConfig
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._
/**
 * rdd of values
 * @param rdd
 */
class Redis2Function(rdd:RDD[String])(implicit redisConfig: RedisConfig) extends Serializable{
  /**
   *
   * @param setName target set's name which hold all the vs
   * @param ttl time to live
   * @return
   */
  def toRedisSet(setName: String, ttl: Int=0){
    rdd.foreachPartition(partition=>{
      val jedis = redisConfig.connectionForKey(setName)
      val pipeline = jedis.pipelined
      partition.foreach(pipeline.sadd(setName, _))
      if (ttl > 0) pipeline.expire(setName, ttl)
      pipeline.sync
      jedis.close
    })

  }

  /**
   *
   * @param listName  target list's name which hold all the vs
   * @param ttl time to live
   * @return
   */
  def toRedisList(listName: String, ttl: Int=0){
    rdd.foreachPartition(partition=>{
      val jedis = redisConfig.connectionForKey(listName)
      val pipeline = jedis.pipelined
      partition.foreach(pipeline.rpush(listName, _))
      if (ttl > 0) pipeline.expire(listName, ttl)
      pipeline.sync
      jedis.close
    })

  }

  def delRedisKeys(): Unit ={
    rdd.foreachPartition(partition=>
      partition.map(kv => (redisConfig.getHost(kv), kv)).toArray.groupBy(_._1).
        mapValues(a => a.map(p => p._2)).foreach {
        x => {
          val conn = x._1.endpoint.connect()
          x._2.foreach(x=>conn.del(x))
          conn.close
        }
      })
  }
  /**
   *
   * @param key
   * @param listSize target list's size
   * @return
   */
  def toRedisFixedList(key: String, listSize: Int=0){
    rdd.foreachPartition(partition=>{
      val jedis = redisConfig.connectionForKey(key)
      val pipeline = jedis.pipelined
      partition.foreach(pipeline.lpush(key, _))
      if (listSize > 0) {
        pipeline.ltrim(key, 0, listSize - 1)
      }
      pipeline.sync
      jedis.close
    })

  }
}


/**
 * Created by ehl on 2016/4/19.
 */
class RedisKV2Function(rdd:RDD[(String,String)])(implicit redisConfig: RedisConfig) extends Serializable{
  /**
   * @param ttl time to live
   */
  def toRedisKVs( ttl: Int=0){
    rdd.foreachPartition(partition=>
      partition.map(kv => (redisConfig.getHost(kv._1), kv)).toArray.groupBy(_._1).
      mapValues(a => a.map(p => p._2)).foreach {
      x => {
        val conn = x._1.endpoint.connect()
        val pipeline = x._1.endpoint.connect.pipelined
        if (ttl <= 0) {
          x._2.foreach(x => pipeline.set(x._1, x._2))
        }
        else {
          x._2.foreach(x => pipeline.setex(x._1, ttl, x._2))
        }
        pipeline.sync
        conn.close
      }
    })
  }


  /**
   * @param hashName
   * @param ttl time to live
   */
  def toRedisHash(hashName: String, ttl: Int=0){
    rdd.foreachPartition(partition=>{ val conn = redisConfig.connectionForKey(hashName)
      val pipeline = conn.pipelined
      partition.foreach(x => pipeline.hset(hashName, x._1, x._2))
      if (ttl > 0) pipeline.expire(hashName, ttl)
      pipeline.sync
      conn.close
    })

  }

  /**
   * @param zsetName
   * @param ttl time to live
   */
  def toRedisZset(zsetName: String, ttl: Int=0){
    rdd.foreachPartition(partition=>{
      val jedis = redisConfig.connectionForKey(zsetName)
      val pipeline = jedis.pipelined
      partition.foreach(x => pipeline.zadd(zsetName, x._2.toDouble, x._1))
      if (ttl > 0) pipeline.expire(zsetName, ttl)
      pipeline.sync
      jedis.close
    })

  }


}
