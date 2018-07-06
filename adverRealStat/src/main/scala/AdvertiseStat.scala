import java.util.Date

import commons.conf.ConfigurationManager
import commons.utils.DateUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer


/**
  * Created by zhengbiubiu on 2018/6/29.
  */
object AdvertiseStat {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("adver").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    // StreamingContext.getActiveOrCreate(checkPointPath, Func)
    // 优先从checkpointPath去反序列化回我们的上下文数据
    // 如果没有相应的checkpointPath对应的文件夹，就调用Func去创建新的streamingcontext
    // 如果有Path，但是代码修改过，直接报错，删除原来的Path内容，再次启动

    // offset写入checkpoint，与此同时，还保存一份到zookeeper
    //创建streamingContext
    val streamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(5))

    //使用updateStateByKey需要指定checkpoint
    streamingContext.checkpoint("./spark_streaming")

    //指定broker和topic
    val kafka_brokers = ConfigurationManager.config.getString("kafka.broker.list")
    val kafka_topics = ConfigurationManager.config.getString("kafka.topics")

    //
    val kafkaParam = Map(
      //引导程序
      "bootstrap.servers" -> kafka_brokers,
      //指定key和value的序列化类
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      //指定group组
      "group.id" -> "group1",
      //指定没有offset时从哪里开始消费
      // latest:   在消费指定topic的分区时，如果已经有offset，直接使用之前保存的offset，
      //           如果没有offset，就从最新的数据开始消费
      // earilist: 在消费指定topic的分区时，如果已经有offset，直接使用之前保存的offset，
      //           如果没有offset，就从最早的数据开始消费
      // none:    在消费指定topic的分区时，如果已经有offset，直接使用之前保存的offset，
      //           如果没有offset，直接报错
      "auto.offset.reset" -> "latest",
      //关闭自动提交
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val adRealTimeDataDStream = KafkaUtils.createDirectStream(streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(kafka_topics), kafkaParam)
    )
    // value: timestamp 	  province 	  city        userid         adid
    // Dstream : RDD[String]  timestamp province city userid adid
    val adRealTimeValueDStream = adRealTimeDataDStream.map {
      item => item.value()
    }

    //根据黑名单的userid过滤数据
    //adRealTimeFilteredDStream:[RDD[userid,record]] record: timestamp province city userid adid
    val adRealTimeFilteredDStream = filterBlackList(sparkSession, adRealTimeValueDStream)
    //需求七：实时维护黑名单
    generateBlackListOntime(adRealTimeFilteredDStream)

    //需求八：统计各省市在一天中累计的广告点击量
    val perDayProvinceCityAdClickCountDStream = getPerDayProvinceCityAdClickCount(adRealTimeFilteredDStream)

    //需求九：统计每个省top3热门广告
    getProvinceTop3Ad(sparkSession, perDayProvinceCityAdClickCountDStream)

    //需求十：统计一个小时内每个广告每分钟的点击次数
    getPerHourPerMinuteClickCount(adRealTimeFilteredDStream)

    streamingContext.start()

    streamingContext.awaitTermination()


  }

  /**
    * 需求十
    *
    * @param adRealTimeFilteredDStream
    */
  def getPerHourPerMinuteClickCount(adRealTimeFilteredDStream: DStream[(Long, String)]) = {
    //    adRealTimeFilteredDStream:过滤黑名单后的数据
    val key2MinuteNum = adRealTimeFilteredDStream.map {
      //log:timestamp provinec city userid adid
      case (userId, log) =>
        val timeStamp = log.split(" ")(0).toLong
        val date = new Date(timeStamp)
        val minute = DateUtils.formatTimeMinute(date)
        val adid = log.split(" ")(4).toLong
        val key = minute + "_" + adid
        (key, 1L)
    }
    //放大粒度后会有重复的key,使用window
    val key2HourMinuteDStream = key2MinuteNum.reduceByKeyAndWindow((a:Long,b:Long)=>a+b,Minutes(60),Minutes(1))

    key2HourMinuteDStream.foreachRDD{ rdd =>
      rdd.foreachPartition{ items =>
        //Array[AdClickTrend]
        val clickTrendArray = new ArrayBuffer[AdClickTrend]()
        // item : RDD[(key, count)]
        // key : minute + "_" + adid
        // minute: yyyyMMddHHmm
        for(item <- items){
          val keySplit = item._1.split("_")
          // minute: yyyyMMddHHmm
          val minute = keySplit(0)
          val date = minute.substring(0, 8)
          val hour = minute.substring(8, 10)
          val minu = minute.substring(10)
          val adid = keySplit(1).toLong
          val count = item._2

          clickTrendArray += AdClickTrend(date, hour, minu, adid, count)
        }

        AdClickTrendDAO.updateBatch(clickTrendArray.toArray)
      }

    }
  }


  /**
    * 需求九
    *
    * @param sparkSession
    * @param perDayProvinceCityAdClickCountDStream
    */
  def getProvinceTop3Ad(sparkSession: SparkSession,
                        perDayProvinceCityAdClickCountDStream: DStream[(String, Long)]) = {
    //    perDayProvinceCityAdClickCountDStream：每一天每个省份的每个城市每个广告的点击次数
    val provinceTop3AdDStream = perDayProvinceCityAdClickCountDStream.transform {
      key2CountRDD => {
        //key:dateKey + "_" + province + "_" + city + "_" + adid
        import sparkSession.implicits._
        val key2ProvinceCountRDD = key2CountRDD.map {
          case (key, count) =>
            val keySplit = key.split("_")
            val dateKey = keySplit(0)
            val province = keySplit(1)
            val adid = keySplit(3).toLong

            val newKey = dateKey + "_" + province + "_" + adid
            (newKey, count)
        }
        // 一天中，每个省份每个广告的点击次数
        val key2ProvinceClickCountRDD = key2ProvinceCountRDD.reduceByKey(_ + _)

        // 1. (key, count) -> (province, adid + count)
        // 2. groupByKey -> (province, iterable(adid+count,....)
        // 3. map -> iterable(adid+count,....).toList.sortWith.take(3)

        import sparkSession.implicits._
        key2ProvinceClickCountRDD.map {
          // key: dateKey + "_" + province + "_" + adid
          case (key, count) =>
            val keySplit = key.split("_")
            val dateKey = keySplit(0)
            val province = keySplit(1)
            val adid = keySplit(2).toLong

            (dateKey, province, adid, count)
        }.toDF("date", "province", "adid", "count").createOrReplaceTempView("tmp_province_ad_count")

        val sql = "select date, province, adid, count from(" +
          " select date, province, adid, count," +
          "row_number() over(partition by province order by count desc) rank from tmp_province_ad_count) t" +
          " where rank<=3"

        // 直接返回sql查询的结果，DF[Row] -> RDD[Row]
        sparkSession.sql(sql).rdd
      }

    }
    provinceTop3AdDStream.foreachRDD {
      rdd =>
        rdd.foreachPartition {
          items =>
            val top3Array = new ArrayBuffer[AdProvinceTop3]()
            //item:Row-> date,province,adid,count
            for (item <- items) {
              val date = item.getAs[String]("date")
              val province = item.getAs[String]("province")
              val adid = item.getAs[Long]("adid")
              val count = item.getAs[Long]("count")

              top3Array += AdProvinceTop3(date, province, adid, count)
            }
            AdProvinceTop3DAO.updateBatch(top3Array.toArray)
        }
    }
  }


  /**
    * 需求八
    *
    * @param adRealTimeFilteredDStream
    */
  def getPerDayProvinceCityAdClickCount(adRealTimeFilteredDStream: DStream[(Long, String)]) = {

    //adRealTimeDataDStream:Dstream : RDD[userId,record] record: timestamp province city userid adid
    val key2NumDStream = adRealTimeFilteredDStream.map {
      case (key, record) =>
        val split = record.split(" ")
        val timestamp = split(0).toLong
        val date = new Date(timestamp)
        val dateKey = DateUtils.formatDateKey(date)
        val province = split(1)
        val city = split(2)
        val adid = split(4).toLong
        val key = dateKey + "_" + province + "_" + city + "_" + adid
        (key, 1L)
    }
    //获取累计的点击次数 //values:新进来的value的队列,state当前的key的状态
    val key2StateCountDStream = key2NumDStream.updateStateByKey[Long] { (values: Seq[Long], state: Option[Long]) =>
      //先从当前的state中获取值
      var stateValue = state.getOrElse(0L)
      //将value队列中的value值读取添加到状态中
      for (value <- values) {
        stateValue += value
      }
      Some(stateValue)
    }
    //写出
    key2StateCountDStream.foreachRDD {
      rdd =>
        rdd.foreachPartition {
          items =>
            val statArray = new ArrayBuffer[AdStat]()
            //item:(key,count)  key:dateKey + "_" + province + "_" + city + "_" + adid
            for (item <- items) {
              val keySplit = item._1.split("_")
              val dateKey = keySplit(0)
              val province = keySplit(1)
              val city = keySplit(2)
              val adid = keySplit(3).toLong
              val count = item._2

              statArray += AdStat(dateKey, province, city, adid, count)
            }
            AdStatDAO.updateBatch(statArray.toArray)
        }
    }
    key2StateCountDStream
  }


  def generateBlackListOntime(adRealTimeFilteredDStream: DStream[(Long, String)]): Unit = {
    //adRealTimeFilteredDStream:(userId,record) record:timestamp province city userid adid
    //先转换为（key，1L）结构
    val key2NumDStream = adRealTimeFilteredDStream.map {
      case (userId, record) =>
        val recordSplit = record.split(" ")
        val timestamp = recordSplit(0).toLong
        val date = new Date(timestamp)
        //dateKey:YYYYmmdd按天粒度
        val dateKey = DateUtils.formatDateKey(date)
        val adid = recordSplit(4).toLong

        //拼接key
        val key = dateKey + "_" + userId + "_" + adid
        (key, 1L)
    }
    //一天中，每个用户对于某一个广告的点击次数
    val key2CountDStream = key2NumDStream.reduceByKey(_ + _)
    //
    key2CountDStream.foreachRDD {
      rdd =>
        rdd.foreachPartition {
          items =>
            val clickCountArray = new ArrayBuffer[AdUserClickCount]()

            for (item <- items) {
              val keySplit = item._1.split("_")
              val dateKey = keySplit(0)
              val userId = keySplit(1).toLong
              val adid = keySplit(2).toLong
              val count = item._2

              clickCountArray += AdUserClickCount(dateKey, userId, adid, count)
            }
            AdUserClickCountDAO.updateBatch(clickCountArray.toArray)
        }
    }
    //获取最新的用户点击次数表里的数据，然后过滤所有要加入到黑名单的用户id
    val userIdBlackListDStream = key2CountDStream.filter {
      case (key, count) =>
        val keySplit = key.split("_")
        val dateKey = keySplit(0)
        val userId = keySplit(1).toLong
        val adid = keySplit(2).toLong

        val count = AdUserClickCountDAO.findClickCountByMultiKey(dateKey, userId, adid)
        if (count >= 100) {
          true
        } else {
          false
        }
    }.map {
      case (key, count) =>
        val userId = key.split("_")(1).toLong
        userId
    }.transform(rdd => rdd.distinct())

    userIdBlackListDStream.foreachRDD {
      rdd =>
        rdd.foreachPartition {
          items =>
            val blackListArray = new ArrayBuffer[AdBlacklist]()
            for (item <- items) {
              blackListArray += AdBlacklist(item)

            }
            AdBlacklistDAO.insertBatch(blackListArray.toArray)
        }
    }

  }


  def filterBlackList(sparkSession: SparkSession, adRealTimeValueDStream: DStream[String]) = {
    //将DStream转换RDD进行操作
    adRealTimeValueDStream.transform {
      recordRDD =>
        //获取黑名单
        //blackListUserInfoArray: Array[AdBlacklist]
        // AdBlacklist: case class AdBlacklist(userid:Long)
        val blackListUserInfoArray = AdBlacklistDAO.findAll()
        //为方便后续操作，转换为RDD
        val blackListUserRDD = sparkSession.sparkContext.makeRDD(blackListUserInfoArray).map {
          item => (item.userid, true)
        }
        val userId2RecordRDD = recordRDD.map {
          // item : String -> timestamp 	  province 	  city        userid         adid
          item =>
            val userId = item.split(" ")(3).toLong
            (userId, item)
        }
        val userId2FiteredRDD = userId2RecordRDD.leftOuterJoin(blackListUserRDD).filter {
          case (userId, (record, bool)) =>
            if (bool.isDefined && bool.get) {
              false
            } else {
              true
            }
        }.map {
          case (userId, (record, bool)) => (userId, record)
        }
        userId2FiteredRDD
    }
  }
}
