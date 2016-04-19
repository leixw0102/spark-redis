package com.ehl.spark.redis.partition

import com.ehl.spark.redis.RedisConfig
import org.apache.spark.Partition


case class RedisPartition(index: Int,
                          redisConfig: RedisConfig,
                          slots: (Int, Int)) extends Partition
