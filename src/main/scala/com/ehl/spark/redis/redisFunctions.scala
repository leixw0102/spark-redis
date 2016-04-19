package com.ehl.spark.redis

import com.ehl.spark.redis.rdd.RedisKeysRDD
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * RedisContext extends sparkContext's functionality with redis functions
  *
  * @param sc a spark context
  */
class RedisContext(@transient val sc: SparkContext)(implicit redisConfig: RedisConfig) extends Serializable {

  /**
    * @param keyPattern a key pattern to match, or a single key
    * @param partitionNum number of partitions
    * @return RedisKeysRDD of simple Keys stored in redis server
    */
  def fromRedisKeyPattern(keyPattern: String = "*",
                          partitionNum: Int = 3)
                         :
  RedisKeysRDD = {
    new RedisKeysRDD(sc, redisConfig, keyPattern, partitionNum, null)
  }

  /**
    * @param keys an array of keys
    * @param partitionNum number of partitions
    * @return RedisKeysRDD of simple Keys stored in redis server
    */
  def fromRedisKeys(keys: Array[String],
                    partitionNum: Int = 3)
                  :
  RedisKeysRDD = {
    new RedisKeysRDD(sc, redisConfig, "", partitionNum, keys)
  }

  /**
    * @param keysOrKeyPattern an array of keys or a key pattern
    * @param partitionNum number of partitions
    * @return RedisKVRDD of simple Key-Values stored in redis server
    */
  def fromRedisKV[T](keysOrKeyPattern: T,
                     partitionNum: Int = 3)
                   :
  RDD[(String, String)] = {
    keysOrKeyPattern match {
      case keyPattern: String => fromRedisKeyPattern(keyPattern, partitionNum).getKV
      case keys: Array[String] => fromRedisKeys(keys, partitionNum).getKV
      case _ => throw new scala.Exception("KeysOrKeyPattern should be String or Array[String]")
    }
  }

  /**
    * @param keysOrKeyPattern an array of keys or a key pattern
    * @param partitionNum number of partitions
    * @return RedisListRDD of related values stored in redis server
    */
  def fromRedisList[T](keysOrKeyPattern: T,
                       partitionNum: Int = 3)
                      :
  RDD[String] = {
    keysOrKeyPattern match {
      case keyPattern: String => fromRedisKeyPattern(keyPattern, partitionNum).getList
      case keys: Array[String] => fromRedisKeys(keys, partitionNum).getList
      case _ => throw new scala.Exception("KeysOrKeyPattern should be String or Array[String]")
    }
  }

  /**
    * @param keysOrKeyPattern an array of keys or a key pattern
    * @param partitionNum number of partitions
    * @return RedisZSetRDD of Keys in related ZSets stored in redis server
    */
  def fromRedisSet[T](keysOrKeyPattern: T,
                      partitionNum: Int = 3)
                    :
  RDD[String] = {
    keysOrKeyPattern match {
      case keyPattern: String => fromRedisKeyPattern(keyPattern, partitionNum).getSet
      case keys: Array[String] => fromRedisKeys(keys, partitionNum).getSet
      case _ => throw new scala.Exception("KeysOrKeyPattern should be String or Array[String]")
    }
  }

  /**
    * @param keysOrKeyPattern an array of keys or a key pattern
    * @param partitionNum number of partitions
    * @return RedisHashRDD of related Key-Values stored in redis server
    */
  def fromRedisHash[T](keysOrKeyPattern: T,
                       partitionNum: Int = 3)
                     :
  RDD[(String, String)] = {
    keysOrKeyPattern match {
      case keyPattern: String => fromRedisKeyPattern(keyPattern, partitionNum).getHash
      case keys: Array[String] => fromRedisKeys(keys, partitionNum).getHash
      case _ => throw new scala.Exception("KeysOrKeyPattern should be String or Array[String]")
    }
  }

  /**
    * @param keysOrKeyPattern an array of keys or a key pattern
    * @param partitionNum number of partitions
    * @return RedisZSetRDD of Keys in related ZSets stored in redis server
    */
  def fromRedisZSet[T](keysOrKeyPattern: T,
                       partitionNum: Int = 3)
                     :
  RDD[String] = {
    keysOrKeyPattern match {
      case keyPattern: String => fromRedisKeyPattern(keyPattern, partitionNum).getZSet
      case keys: Array[String] => fromRedisKeys(keys, partitionNum).getZSet
      case _ => throw new scala.Exception("KeysOrKeyPattern should be String or Array[String]")
    }
  }

  /**
    * @param keysOrKeyPattern an array of keys or a key pattern
    * @param partitionNum number of partitions
    * @return RedisZSetRDD of related Key-Scores stored in redis server
    */
  def fromRedisZSetWithScore[T](keysOrKeyPattern: T,
                                partitionNum: Int = 3)
                               :
  RDD[(String, Double)] = {
    keysOrKeyPattern match {
      case keyPattern: String => fromRedisKeyPattern(keyPattern, partitionNum).getZSetWithScore
      case keys: Array[String] => fromRedisKeys(keys, partitionNum).getZSetWithScore
      case _ => throw new scala.Exception("KeysOrKeyPattern should be String or Array[String]")
    }
  }

  /**
    * @param keysOrKeyPattern an array of keys or a key pattern
    * @param start start position of target zsets
    * @param end end position of target zsets
    * @param partitionNum number of partitions
    * @return RedisZSetRDD of Keys in related ZSets stored in redis server
    */
  def fromRedisZRange[T](keysOrKeyPattern: T,
                         start: Int,
                         end: Int,
                         partitionNum: Int = 3)
                        :
  RDD[String] = {
    keysOrKeyPattern match {
      case keyPattern: String => fromRedisKeyPattern(keyPattern, partitionNum).getZSetByRange(start, end)
      case keys: Array[String] => fromRedisKeys(keys, partitionNum).getZSetByRange(start, end)
      case _ => throw new scala.Exception("KeysOrKeyPattern should be String or Array[String]")
    }
  }

  /**
    * @param keysOrKeyPattern an array of keys or a key pattern
    * @param start start position of target zsets
    * @param end end position of target zsets
    * @param partitionNum number of partitions
    * @return RedisZSetRDD of related Key-Scores stored in redis server
    */
  def fromRedisZRangeWithScore[T](keysOrKeyPattern: T,
                                  start: Int,
                                  end: Int,
                                  partitionNum: Int = 3)
                                 :
  RDD[(String, Double)] = {
    keysOrKeyPattern match {
      case keyPattern: String => fromRedisKeyPattern(keyPattern, partitionNum).getZSetByRangeWithScore(start, end)
      case keys: Array[String] => fromRedisKeys(keys, partitionNum).getZSetByRangeWithScore(start, end)
      case _ => throw new scala.Exception("KeysOrKeyPattern should be String or Array[String]")
    }
  }

  /**
    * @param keysOrKeyPattern an array of keys or a key pattern
    * @param min min score of target zsets
    * @param max max score of target zsets
    * @param partitionNum number of partitions
    * @return RedisZSetRDD of Keys in related ZSets stored in redis server
    */
  def fromRedisZRangeByScore[T](keysOrKeyPattern: T,
                                min: Double,
                                max: Double,
                                partitionNum: Int = 3)
                               (implicit redisConfig: RedisConfig ):
  RDD[String] = {
    keysOrKeyPattern match {
      case keyPattern: String => fromRedisKeyPattern(keyPattern, partitionNum).getZSetByScore(min, max)
      case keys: Array[String] => fromRedisKeys(keys, partitionNum).getZSetByScore(min, max)
      case _ => throw new scala.Exception("KeysOrKeyPattern should be String or Array[String]")
    }
  }

  /**
    * @param keysOrKeyPattern an array of keys or a key pattern
    * @param min min score of target zsets
    * @param max max score of target zsets
    * @param partitionNum number of partitions
    * @return RedisZSetRDD of related Key-Scores stored in redis server
    */
  def fromRedisZRangeByScoreWithScore[T](keysOrKeyPattern: T,
                                         min: Double,
                                         max: Double,
                                         partitionNum: Int = 3)
                                        :
  RDD[(String, Double)] = {
    keysOrKeyPattern match {
      case keyPattern: String => fromRedisKeyPattern(keyPattern, partitionNum).getZSetByScoreWithScore(min, max)
      case keys: Array[String] => fromRedisKeys(keys, partitionNum).getZSetByScoreWithScore(min, max)
      case _ => throw new scala.Exception("KeysOrKeyPattern should be String or Array[String]")
    }
  }

}



