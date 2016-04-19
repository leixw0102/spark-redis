package com.ehl.spark

import com.ehl.spark.redis.rdd.{RedisKV2Function, Redis2Function, RedisKV2Save, Redis2Save}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


/**
 * Created by ehl on 2016/4/18.
 */
package object redis{

  implicit def toRedisContext(sc:SparkContext)(implicit redisConfig: RedisConfig)=new RedisContext(sc)

  implicit def toRedisFromRDD(rdd:RDD[String])(implicit redisConfig: RedisConfig)= new Redis2Function(rdd)
  implicit def toRedisKVFromRDD(rdd:RDD[(String,String)])(implicit redisConfig: RedisConfig) = new RedisKV2Function(rdd)
}
