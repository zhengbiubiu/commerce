import java.util.{Date, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{UserInfo, UserVisitAction}
import commons.utils._
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.util.Random

/**
  * Created by zhengbiubiu on 2018/6/25.
  */
object SessionStat {



  def main(args: Array[String]): Unit = {

    //一，先导入全局限制条件
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam = JSONObject.fromObject(jsonStr)

    //二，创建UUID
    val taskUUID = UUID.randomUUID().toString

    //三，创建sparkConf
    val sparkConf = new SparkConf().setAppName("sessionStat").setMaster("local[*]")

    //四，创建sparkSession
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    //五，获取动作表里的初始数据
    // UserVisitAction(2018-06-23,84,ba683a4a00754a24a9ab6b6fe9a2b7b6,7,2018-06-23 18:02:48,重庆辣子鸡,-1,-1,null,null,null,null,0)
    val actionRDD = getBasicActionData(sparkSession, taskParam)

    //获取按session_id分组后的RDD((session_id,Iterable[UserVisitAction])
    val sessionId2action1 = actionRDD.map(item => (item.session_id, item))

    val sessionId2action = sessionId2action1.groupByKey()

    //缓存持久化
    sessionId2action.cache()

    //获取完整信息数据 Session_id | Search_Keywords | Click_Category | Visit_Length | Step_Length | Start_Time
    //(b90bee646faa4013ab70fd7e489c5572,sessionid=b90bee646faa4013ab70fd7e489c5572
    // |searchKeywords=新辣道鱼火锅,呷哺呷哺|clickCategoryIds=50|visitLength=3224
    // |stepLength=16|startTime=2018-06-23 15:02:43|age=28|professional=professional54
    // |sex=female|city=city81)

    val sessionId2FullInfoRDD = getSessionFullInfo(sparkSession, sessionId2action)
    //    println("----sessionId2FullInfoRDD-----" + sessionId2FullInfoRDD.count())
    //使用自定义累加器
    val sessionStatisticAccumulator = new SessionStatAccumulator
    sparkSession.sparkContext.register(sessionStatisticAccumulator)

    //一，对数据及进行过滤
    //二，对累加器进行累加
    val sessionId2filteredRDD = getFilterRDD(sparkSession, taskParam, sessionStatisticAccumulator, sessionId2FullInfoRDD)
    //println("----sessionId2filteredRDD-----" + sessionId2filteredRDD.count())
    //执行action操作，否则
    //    sessionId2FullInfoRDD.foreach(println(_))
    sessionId2filteredRDD.foreach(println(_))
    //需求一：各范围session占比统计
    getSessionRatio(sparkSession, taskUUID, sessionStatisticAccumulator.value)

    // 需求二：Session随机抽取
    sessionRandomExtract(sparkSession, taskUUID, sessionId2FullInfoRDD)

    //需求三：Top10热门品类统计
    //先得到符合过滤条件的action数据
    val sessionId2FilteredActionRDD = sessionId2action1.join(sessionId2filteredRDD).map {

      case (session, (action, fullInfo)) => (session, action)
    }
    //为第四个需求准备，此时top10Category是Array(sortKey,fullInfo[cid|clickCount|orderCount|payCount])
    val top10Category = getTop10PopularCategories(sparkSession, taskUUID, sessionId2FilteredActionRDD)

    //需求四：统计每一个Top10热门品类的top10活跃session
    //sessionId2filteredRDD:过滤后符合条件的action：(sessionId,action)
    getTop10ActiveSession(sparkSession,taskUUID,top10Category,sessionId2FilteredActionRDD)



  }

  def getTop10ActiveSession(sparkSession: SparkSession,
                            taskUUID: String, top10Category:Array[(SortedKey, String)],
                            sessionId2filteredActionRDD: RDD[(String, UserVisitAction)]): Unit = {

    //先获取top10热门品类的categoryId,返回一个top10的 (cid,cid)
    val top10CategoryRDD = sparkSession.sparkContext.makeRDD(top10Category).map {
      case (sortKey, fullInfo) =>
        val cid = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
        (cid, cid)
    }
    //获取符合条件的session中的点击action，然后以cid为key，和top10的cid做join，得到top10的action
    val cid2ActionRDD = sessionId2filteredActionRDD.map {
      case (sessionId, action) =>
        val cid = action.click_category_id
        (cid, action)
    }

    //join操作,只剩下top10商品的action，再转换格式(sessionId,action)
    val sessionId2FilteredRDD = cid2ActionRDD.join(top10CategoryRDD).map {
      case (cid, (action, ccid)) =>
        val sessionid = action.session_id
        (sessionid, action)
    }

    //统计每个session对top10热门品类的点击次数sessionId2GroupRDD:(sessionId,iterable[action])
    val sessionId2GroupRDD = sessionId2FilteredRDD.groupByKey()
    //经过以下循环，categoryCountMap被填充完成，里面记录了当前的session对于top10热门品类中的每一个品类的点击次数
    val cid2SessionCountRDD = sessionId2GroupRDD.flatMap {
      case (sessionId, iterableAction) =>
        val categoryCountMap = new mutable.HashMap[Long, Long]()
        for (action <- iterableAction) {
          val categoryId = action.click_category_id
          if (!categoryCountMap.contains(categoryId))
            categoryCountMap += (categoryId -> 0)
          categoryCountMap.update(categoryId, categoryCountMap(categoryId) + 1)
        }
        for ((cid, count) <- categoryCountMap)
          yield (cid, sessionId + "=" + count)
    }
    //按照cid分组：cid，(sessionId1=count1,sessionId2=count2)
    val cid2GroupSessionCountRDD = cid2SessionCountRDD.groupByKey()

    val top10SessionRDD = cid2GroupSessionCountRDD.flatMap{
      case (cid,iterableSessionCount) =>
        val sortedSession = iterableSessionCount.toList.sortWith((item1,item2)=>
          item1.split("=")(1) >  item2.split("=")(1)
        )

        val top10Session = sortedSession.take(10)

        val top10SessionCaseClass = top10Session.map{
          item =>
            val sessionId = item.split("=")(0)
            val count = item.split("=")(1).toLong
            Top10Session(taskUUID,cid,sessionId,count)
        }
        top10SessionCaseClass

    }
    import sparkSession.implicits._
    top10SessionRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "top10_session0115")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()



  }


  def getCategoryClickCount(sessionId2FilteredActionRDD: RDD[(String, UserVisitAction)]) = {

    //过滤掉所有没有发生过点击的action
    val sessionId2FilterRDD = sessionId2FilteredActionRDD.filter {
      case (session, action) => action.click_category_id != -1
    }
    val cid2NumRDD = sessionId2FilterRDD.map {
      case (session, action) => (action.click_category_id, 1L)
    }
    cid2NumRDD.reduceByKey(_ + _)
  }

  def getCategoryOrderCount(sessionId2FilteredActionRDD: RDD[(String, UserVisitAction)]) = {
    //将所有对应下单行为的action过滤出来
    val sessionIdFilterRDD = sessionId2FilteredActionRDD.filter {
      case (session, action) => action.order_category_ids != null

    }
    val cid2Num = sessionIdFilterRDD.flatMap {
      case (session, action) => action.order_category_ids.split(",").map(item => (item.toLong, 1L))
    }
    cid2Num.reduceByKey(_ + _)
  }

  def getCategoryPayCount(sessionId2FilteredActionRDD: RDD[(String, UserVisitAction)]) = {
    // 将所有对应于付款行为的action过滤出来
    val sessionIdFilterRDD = sessionId2FilteredActionRDD.filter {
      case (sessionId, action) => action.pay_category_ids != null
    }

    val cid2Num = sessionIdFilterRDD.flatMap {
      case (sessionId, action) => action.pay_category_ids.split(",").map(item => (item.toLong, 1L))
    }

    cid2Num.reduceByKey(_ + _)

  }

  def getFullInfoCount(categoryId2CidRDD: RDD[(Long, Long)],
                       clickCount: RDD[(Long, Long)],
                       orderCount: RDD[(Long, Long)],
                       payCount: RDD[(Long, Long)]) = {

    val clickCountRDD = categoryId2CidRDD.leftOuterJoin(clickCount).map {
      case (categoryId, (cid, cCount)) =>
        val count = if (cCount.isDefined) cCount.get else 0L
        val aggrInfo = Constants.FIELD_CATEGORY_ID + "=" + categoryId + "|" + Constants.FIELD_CLICK_COUNT + "=" + count
        (categoryId, aggrInfo)
    }

    val orderCountRDD = clickCountRDD.leftOuterJoin(orderCount).map {
      case (cid, (aggrInfo, oCount)) =>
        val count = if (oCount.isDefined) oCount.get else 0L
        val orderInfo = aggrInfo + "|" + Constants.FIELD_ORDER_COUNT + "=" + count
        (cid, orderInfo)
    }

    val payCountRDD = orderCountRDD.leftOuterJoin(payCount).map {
      case (cid, (aggrInfo, pCount)) =>
        val count = if (pCount.isDefined) pCount.get else 0L
        val payInfo = aggrInfo + "|" + Constants.FIELD_PAY_COUNT + "=" + count
        (cid, payInfo)
    }

    payCountRDD

  }

  def getTop10PopularCategories(sparkSession: SparkSession,
                                taskUUID: String,
                                sessionId2FilteredActionRDD: RDD[(String, UserVisitAction)]): Array[(SortedKey, String)] = {

    //首先得到所有被点击、下单、付款过的品类
    var categoryId2CidRDD = sessionId2FilteredActionRDD.flatMap {
      case (sessionId, action) =>
        val categoryArray = new mutable.ArrayBuffer[(Long, Long)]()
        if (action.click_category_id != -1L) {
          categoryArray += ((action.click_category_id, action.click_category_id))
        } else if (action.order_category_ids != null) {
          for (cid <- action.order_category_ids.split(",")) {
            categoryArray += ((cid.toLong, cid.toLong))
          }
        } else if (action.pay_category_ids != null) {
          for (cid <- action.pay_category_ids.split(",")) {
            categoryArray += ((cid.toLong, cid.toLong))
          }
        }
        categoryArray

    }
    //所有被点击、下单、付款的品类（不重复）
    //（cid，cid）
    categoryId2CidRDD = categoryId2CidRDD.distinct()

    //统计每一个被点击的品类的点击次数
    //(cid,count)
    val clickCount = getCategoryClickCount(sessionId2FilteredActionRDD)

    //统计每一个被下单的品类的下单次数
    val orderCount = getCategoryOrderCount(sessionId2FilteredActionRDD)

    //统计每一个被付款的品类的付款次数
    val payCount = getCategoryPayCount(sessionId2FilteredActionRDD)

    //
    val cid2FullInfo = getFullInfoCount(categoryId2CidRDD, clickCount, orderCount, payCount)

    val soryKey2FullInfo = cid2FullInfo.map {
      case (cid, fullInfo) =>

        val clickCount = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CLICK_COUNT).toLong
        val orderCount = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_ORDER_COUNT).toLong
        val payCount = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_PAY_COUNT).toLong

        val sortKey = new SortedKey(clickCount, orderCount, payCount)
        (sortKey, fullInfo)
    }

    //
    val top10Category = soryKey2FullInfo.sortByKey(false).take(10)
    val top10CategoryRDD = sparkSession.sparkContext.makeRDD(top10Category)
    //    top10CategoryRDD.foreach(println(_))

    val top10CategoryWriteRDD = top10CategoryRDD.map {
      case (sortKey, fullInfo) =>
        val categoryId = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
        val clickCount = sortKey.clickCount
        val orderCount = sortKey.orderCount
        val payCount = sortKey.payCount

        Top10Category(taskUUID, categoryId, clickCount, orderCount, payCount)

    }
    import sparkSession.implicits._
    top10CategoryWriteRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "top10_category0115")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()

    top10Category
  }


  def sessionRandomExtract(sparkSession: SparkSession, taskUUID: String, sessionId2FullInfoRDD: RDD[(String, String)]): Unit = {
    //先将以session_id为key的fullInfoRDD转化为时间
    val dateHour2FullInfo = sessionId2FullInfoRDD.map {
      case (session, fullInfo) =>
        val startTime = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_START_TIME)
        //将时间转化为 yyyy-MM-dd_HH结构
        val dateHour = DateUtils.getDateHour(startTime)

        //返回以时间为key
        (dateHour, fullInfo)
    }

    //对dateHour2FullInfo进行countByKey，返回一个map，key是date，value是个数  (date,count)
    val dateHourCountMap = dateHour2FullInfo.countByKey()

    //嵌套map结构，第一层key为date，第二层key为hour，value为count
    val dateHourSessionCountMap = new mutable.HashMap[String, mutable.HashMap[String, Long]]()

    //将其转换为以date为key，value为一个map，该map中以hour为key，count为value
    for ((dateHour, count) <- dateHourCountMap) {
      val date = dateHour.split("_")(0)
      val hour = dateHour.split("_")(1)
      dateHourSessionCountMap.get(date) match {
        case None => dateHourSessionCountMap(date) = new mutable.HashMap[String, Long]()
          dateHourSessionCountMap(date) += (hour -> count)
        case Some(map) => dateHourSessionCountMap(date) += (hour -> count)
      }

    }

    for ((date, hourCountMap) <- dateHourSessionCountMap) {

      for ((hour, count) <- hourCountMap) {

        //假设要抽取100条，则每天要抽取的条数为：(dateHourSessionCountMap.size即为总共的天数)
        val sessionExtractCountPerDay = 100 / dateHourSessionCountMap.size

        val dateHourRandomIndexMap = new mutable.HashMap[String, mutable.HashMap[String, mutable.ListBuffer[Int]]]()

        val random = new Random()

        def generateRandomIndex(sessionExtractCountPerDay: Long,
                                sessionDayCount: Long,
                                hourCountMap: mutable.HashMap[String, Long],
                                hourIndexMap: mutable.HashMap[String, mutable.ListBuffer[Int]]): Unit = {
          //得到这一个小时要抽取多少个session
          for ((hour, count) <- hourCountMap) {

            var hourExtractSessionCount = ((count / sessionDayCount.toDouble) * sessionExtractCountPerDay).toInt
            if (hourExtractSessionCount > count.toInt) {
              hourExtractSessionCount = count.toInt
            }

            hourIndexMap.get(hour) match {

              case None => hourIndexMap(hour) = new mutable.ListBuffer[Int]
                for (i <- 0 until hourExtractSessionCount) {

                  var index = random.nextInt(count.toInt)
                  while (hourIndexMap(hour).contains(index)) {
                    index = random.nextInt(count.toInt)
                  }
                  hourIndexMap(hour) += index
                }
              case Some(list) =>
                for (i <- 0 until hourExtractSessionCount) {
                  var index = random.nextInt(count.toInt)
                  while (hourIndexMap(hour).contains(index)) {
                    index = random.nextInt(count.toInt)
                  }
                  hourIndexMap(hour) += index
                }
            }
          }
        }

        for ((date, hourCountMap) <- dateHourSessionCountMap) {
          val dayCount = hourCountMap.values.sum

          dateHourRandomIndexMap.get(date) match {

            case None => dateHourRandomIndexMap(date) = new mutable.HashMap[String, mutable.ListBuffer[Int]]()

              generateRandomIndex(sessionExtractCountPerDay, dayCount, hourCountMap, dateHourRandomIndexMap(date))

            case Some(map) => generateRandomIndex(sessionExtractCountPerDay, dayCount, hourCountMap, dateHourRandomIndexMap(date))
          }
        }

        for ((date, hourCountMap) <- dateHourRandomIndexMap) {
          println("*******************************************")
          println("date: " + date)
          for ((hour, list) <- hourCountMap) {
            println("hour:" + hour)
            for (item <- list) {
              print(item + ", ")
            }
            println()
            println("------------------------------------------")
          }
          println("*******************************************")
        }
        //知道了每个小时要抽取多少条sessin，并且生成了对应随机索引的List
        val dateHour2GroupRDD = dateHour2FullInfo.groupByKey()
        val extractSessionRDD = dateHour2GroupRDD.flatMap {

          case (dateHour, iterableFullInfo) =>
            val date = dateHour.split("_")(0)
            val hour = dateHour.split("_")(1)

            val indexList = dateHourRandomIndexMap.get(date).get(hour)
            val extractSessionArray = new mutable.ArrayBuffer[SessionRandomExtract]()
            var index = 0

            for (fullInfo <- iterableFullInfo) {


              if (indexList.contains(index)) {
                val sessionId = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SESSION_ID)
                val startTime = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_START_TIME)
                val searchKeyWords = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS)
                val clickCategories = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS)

                extractSessionArray += SessionRandomExtract(taskUUID, sessionId, startTime, searchKeyWords, clickCategories)
              }
              index += 1
            }
            extractSessionArray
        }

        import sparkSession.implicits._
        extractSessionRDD.toDF().write.format("jdbc")
          .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
          .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
          .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
          .option("dbtable", "session_random_extract_0115")
          .mode(SaveMode.Append)
          .save()
      }

    }


  }

  /**
    * 统计占比
    *
    * @param sparkSession
    * @param taskUUID
    * @param value
    */
  def getSessionRatio(sparkSession: SparkSession, taskUUID: String
                      , value: mutable.HashMap[String, Int]): Unit = {
    //获取sessionCount，防止分母为零，给个默认1
    val session_count = value.getOrElse(Constants.SESSION_COUNT, 1).toDouble

    //获取分段时长和分段步长的计数
    val visit_length_1s_3s = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
    val visit_length_4s_6s = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
    val visit_length_7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
    val visit_length_10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
    val visit_length_30s_60s = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
    val visit_length_1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
    val visit_length_3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
    val visit_length_10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
    val visit_length_30m = value.getOrElse(Constants.TIME_PERIOD_30m, 0)

    val step_length_1_3 = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
    val step_length_4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val step_length_7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val step_length_10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val step_length_30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
    val step_length_60 = value.getOrElse(Constants.STEP_PERIOD_60, 0)

    //获取占比,精确到小数点后两位
    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s / session_count, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s / session_count, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s / session_count, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s / session_count, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s / session_count, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m / session_count, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m / session_count, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m / session_count, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m / session_count, 2)

    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3 / session_count, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6 / session_count, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9 / session_count, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30 / session_count, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60 / session_count, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60 / session_count, 2)

    //将各个占比封装caseclass,转换成DF，写入mysql
    val stat = SessionAggrStat(taskUUID, session_count.toInt, visit_length_1s_3s_ratio, visit_length_4s_6s_ratio, visit_length_7s_9s_ratio,
      visit_length_10s_30s_ratio, visit_length_30s_60s_ratio, visit_length_1m_3m_ratio,
      visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio,
      step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
      step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)

    val statRDD = sparkSession.sparkContext.makeRDD(Array(stat))
    import sparkSession.implicits._
    //转换成DF写入到mysql

    statRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_stat_0115")
      .mode(SaveMode.Append) //保存模式为追加，否则每条数据创建表，会显示表已存在
      .save()
  }

  /**
    * 获取过滤后的，并且累加后的数据
    *
    * @param sparkSession
    * @param taskParam
    * @param sessionStatisticAccumulator
    * @param sessionId2FullInfoRDD
    * @return
    */
  def getFilterRDD(sparkSession: SparkSession, taskParam: JSONObject
                   , sessionStatisticAccumulator: SessionStatAccumulator
                   , sessionId2FullInfoRDD: RDD[(String, String)]) = {
    //从taskParam提取限制条件
    //开始时间和结束时间
    val startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)

    //其他过滤条件
    val professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val searchKeywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val clickCategories = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

    //将过滤条件拼接,以便后续使用过滤方法
    var filterInfo = (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
      (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
      (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
      (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if (searchKeywords != null) Constants.PARAM_KEYWORDS + "=" + searchKeywords + "|" else "") +
      (if (clickCategories != null) Constants.PARAM_CATEGORY_IDS + "=" + clickCategories else "")

    //去除结尾的 "|"
    if (filterInfo.endsWith("\\|")) {
      filterInfo = filterInfo.substring(0, filterInfo.length - 1)
    }

    //执行过滤条件
    val sessionId2FilteredRDD = sessionId2FullInfoRDD.filter {
      case (sessionId, fullInfo) =>
        var success = true
        //        判断时间是否在要求范围之内，如果不是标记false
        if (!ValidUtils.between(fullInfo, Constants.FIELD_AGE, filterInfo, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
          success = false
        }
        //判断professional是否在要求范围内
        if (!ValidUtils.in(fullInfo, Constants.FIELD_PROFESSIONAL, filterInfo, Constants.PARAM_PROFESSIONALS)) {
          success = false
        }
        //判断city是否在要求范围内
        if (!ValidUtils.in(fullInfo, Constants.FIELD_CITY, filterInfo, Constants.PARAM_CITIES)) {
          success = false
        }
        if (!ValidUtils.equal(fullInfo, Constants.FIELD_SEX, filterInfo, Constants.PARAM_SEX)) {
          success = false
        }

        if (!ValidUtils.in(fullInfo, Constants.FIELD_SEARCH_KEYWORDS, filterInfo, Constants.PARAM_KEYWORDS))
          success = false

        if (!ValidUtils.in(fullInfo, Constants.FIELD_CATEGORY_ID, filterInfo, Constants.PARAM_CATEGORY_IDS))
          success = false

        //判断success的状态，如果为true说明该sessionId2FullInfoRDD是满足条件的需要使用自定义累加器对相关需求累加
        if (success) {
          //对session进行累加
          sessionStatisticAccumulator.add(Constants.SESSION_COUNT)

          //对session时长进行分段累加
          def calculateVisitLength(visitLength: Long) = {
            if (visitLength >= 1 && visitLength <= 3) {
              sessionStatisticAccumulator.add(Constants.TIME_PERIOD_1s_3s)
            } else if (visitLength >= 4 && visitLength <= 6) {
              sessionStatisticAccumulator.add(Constants.TIME_PERIOD_4s_6s)
            } else if (visitLength >= 7 && visitLength <= 9) {
              sessionStatisticAccumulator.add(Constants.TIME_PERIOD_7s_9s)
            } else if (visitLength >= 10 && visitLength <= 30) {
              sessionStatisticAccumulator.add(Constants.TIME_PERIOD_10s_30s)
            } else if (visitLength > 30 && visitLength <= 60) {
              sessionStatisticAccumulator.add(Constants.TIME_PERIOD_30s_60s)
            } else if (visitLength > 60 && visitLength <= 180) {
              sessionStatisticAccumulator.add(Constants.TIME_PERIOD_1m_3m)
            } else if (visitLength > 180 && visitLength <= 600) {
              sessionStatisticAccumulator.add(Constants.TIME_PERIOD_3m_10m)
            } else if (visitLength > 600 && visitLength <= 1800) {
              sessionStatisticAccumulator.add(Constants.TIME_PERIOD_10m_30m)
            } else if (visitLength > 1800) {
              sessionStatisticAccumulator.add(Constants.TIME_PERIOD_30m)
            }
          }

          //对session步长进行累加
          def calculateStepLength(stepLength: Long): Unit = {
            if (stepLength >= 1 && stepLength <= 3) {
              sessionStatisticAccumulator.add(Constants.STEP_PERIOD_1_3)
            } else if (stepLength >= 4 && stepLength <= 6) {
              sessionStatisticAccumulator.add(Constants.STEP_PERIOD_4_6)
            } else if (stepLength >= 7 && stepLength <= 9) {
              sessionStatisticAccumulator.add(Constants.STEP_PERIOD_7_9)
            } else if (stepLength >= 10 && stepLength <= 30) {
              sessionStatisticAccumulator.add(Constants.STEP_PERIOD_10_30)
            } else if (stepLength > 30 && stepLength <= 60) {
              sessionStatisticAccumulator.add(Constants.STEP_PERIOD_30_60)
            } else if (stepLength > 60) {
              sessionStatisticAccumulator.add(Constants.STEP_PERIOD_60)
            }
          }

          //从sessionId2FullInfoRDD中取出session时长和步长
          val stepLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong
          val visitLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong

          //判断session中的时长和步长在哪个段中
          calculateStepLength(stepLength)
          calculateVisitLength(visitLength)
        }

        success
    }

    sessionId2FilteredRDD

  }


  /**
    *
    * @param sparkSession
    * @param sessionId2action
    * @return
    */
  def getSessionFullInfo(sparkSession: SparkSession,
                         sessionId2action: RDD[(String, Iterable[UserVisitAction])]) = {

    val userId2AggrInfoRDD = sessionId2action.map {

      case (sessionId, iterableAction) =>
        var startTime: Date = null
        var endTime: Date = null
        val searchKeyWords = new StringBuilder("")
        val clikCategories = new StringBuilder("")
        var userId = -1L
        var stepLength = 0L

        for (action <- iterableAction) {

          if (userId == -1L) {
            userId = action.user_id
          }

          //更新起始时间和结束时间
          val actionTime = DateUtils.parseTime(action.action_time)
          if (startTime == null || startTime.after(actionTime))
            startTime = actionTime
          if (endTime == null || endTime.before(actionTime))
            endTime = actionTime

          //完成搜索关键词的追加（去重）
          val searchKeyWord = action.search_keyword
          if (StringUtils.isNotEmpty(searchKeyWord) && !searchKeyWords.toString().contains(searchKeyWord))
            searchKeyWords.append(searchKeyWord + ",")

          //完成点击品类的追加（去重）
          val clikCategory = action.click_category_id
          if (clikCategory != -1L && !clikCategories.toString.contains(clikCategory))
            clikCategories.append(clikCategory + ",")

          stepLength += 1
        }

        //去除结尾的 ","
        val searchKW = StringUtils.trimComma(searchKeyWords.toString())
        val clickCG = StringUtils.trimComma(clikCategories.toString())

        //获取访问时长(s)
        val visitLength = (endTime.getTime - startTime.getTime) / 1000

        //最后一步拼接
        val aggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
          Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKW + "|" +
          Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCG + "|" +
          Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
          Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
          Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)

        //map返回值
        (userId, aggrInfo)
    }

    val sql = "select * from user_info"
    import sparkSession.implicits._
    val userId2UserInfoRDD = sparkSession.sql(sql).as[UserInfo].rdd.map(item => (item.user_id, item))

    //userIdAggrInfoRDD --(userId,aggrInfo)
    //userId2UserInfo --(userId,UserInfo)

    val session2FullInfoRDD = userId2AggrInfoRDD.join(userId2UserInfoRDD).map {

      case (userId, (aggrInfo, userInfo)) =>
        val fullInfo = aggrInfo + "|" + Constants.FIELD_AGE + "=" + userInfo.age + "|" +
          Constants.FIELD_PROFESSIONAL + "=" + userInfo.professional + "|" +
          Constants.FIELD_SEX + "=" + userInfo.sex + "|" +
          Constants.FIELD_CITY + "=" + userInfo.city

        //字段名=字段值|。。。
        val sessionId = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SESSION_ID)

        (sessionId, fullInfo)
    }

    session2FullInfoRDD
  }


  /**
    *
    * @param sparkSession
    * @param taskParm
    * @return 返回actionRDD[UserVisitAction]
    */
  def getBasicActionData(sparkSession: SparkSession, taskParm: JSONObject) = {
    val startDate = ParamUtils.getParam(taskParm, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParm, Constants.PARAM_END_DATE)
    val sql = "select * from user_visit_action where date >='" + startDate + "' and date <= '" + endDate + "'"

    import sparkSession.implicits._
    sparkSession.sql(sql).as[UserVisitAction].rdd
  }

}
