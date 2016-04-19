package com.ehl.spark.redis.example

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by ehl on 2016/4/19.
 *
 * spark-redis config
 * //  conf.get("redis.host", Protocol.DEFAULT_HOST),
//  conf.getInt("redis.port", Protocol.DEFAULT_PORT),
//  conf.get("redis.auth", null),
//  conf.getInt("redis.db", Protocol.DEFAULT_DATABASE),
//  conf.getInt("redis.timeout", Protocol.DEFAULT_TIMEOUT)
 */

import com.ehl.spark.redis._

/**
 * read : sc.fromRedis*
 *
 * write :toRedis*
 */
object ehlSparkRedisKVExample extends App{

  val str="ehl abc;d df;s f;d f;w f;s 8"

  val sc = new SparkContext(new SparkConf().setMaster("local")
    .setAppName("redis")
    .set("redis.host","10.150.27.210")
    .set("redis.port","6379")
    .set("redis.db","3")
//    .set("redis.auth","")
  )

  implicit val redisConfig=new RedisConfig(new RedisEndpoint(sc.getConf))
  sc.makeRDD(str.split(";")).map(_.split(" ")).map(x=>(x(0),x(1)))
  .toRedisKVs(0)
//  sc.fromRedisKV()
//  println("------------------------------------------")
  Thread.sleep(10000L)
//
  sc.fromRedisKV("*").foreach(println)

  sc.stop()

}
