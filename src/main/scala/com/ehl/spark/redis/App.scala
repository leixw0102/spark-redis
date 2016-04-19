package com.ehl.spark.redis

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ${user.name}
 */
object App {


  def main(args : Array[String]) {
    val sc = new SparkContext(new SparkConf().setMaster("local")
      .setAppName("redis")
      .set("redis.host","10.150.27.210")
      .set("redis.port","6379")
      .set("redis.db","3")

    )
  }

}
