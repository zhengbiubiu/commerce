import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.{DateUtils, ParamUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

/**
  * Created by zhengbiubiu on 2018/6/27.
  */
object PageStatistic {


  def main(args: Array[String]): Unit = {

    //获取任务限制参数
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    var taskParam = JSONObject.fromObject(jsonStr)

    //创建主键
    val taskUUID = UUID.randomUUID().toString

    //创建sparkConf
    val sparkConf = new SparkConf().setAppName("pageStat").setMaster("local[*]")

    //创建sparkSession
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    //获取配置文件中的pageflow
    //pageFlow:String 1,2,3,4,5,6,7
    val pageFlow = ParamUtils.getParam(taskParam,Constants.PARAM_TARGET_PAGE_FLOW)
    //转换为数组
    val pageFlowArray = pageFlow.split(",")
    //slice(from : scala.Int, until : scala.Int)
    //targetPageSplit：Array[String] (1_2,2_3,3_4,...)
    val targetPageSplit = pageFlowArray.slice(0,pageFlowArray.length-1).zip(pageFlowArray.tail).map{
      case (page1,page2) =>page1+"_"+page2
    }

    //获取表中的数据 (sessionId,UserVisitAction)
    val sessionId2ActionRDD = getSessionAction(sparkSession,taskParam)
    //先按sessionId聚合 (sessionId,iterable[UservisitAction])
    val sessionId2GroupAction = sessionId2ActionRDD.groupByKey()
    //将同一个sessionId对应的action按照action时间进行排序，再取排序后对应的page_id
    val pageSplitRDD = sessionId2GroupAction.flatMap{
      case (sessionId,iterableAction) =>
        //对action时间进行排序，使用sortWith
      val  sortedAction =iterableAction.toList.sortWith((action1,action2)=>
        //将时间转化为毫秒
        DateUtils.parseTime(action1.action_time).getTime <
        DateUtils.parseTime(action2.action_time).getTime
      )
        //取出按时间排好序的pageFlow   1 ,2 ,3 ,4 ,5 ,6..
        val pageActionFlow =sortedAction.map(item =>item.page_id)
        //转化格式 Array[String] (1_2,2_3,3_4,...)
      val pageSplit = pageActionFlow.slice(0,pageActionFlow.length-1).zip(pageActionFlow.tail).map{
        case (page1,page2) =>page1+"_"+page2
      }
        //targetPageSplit：Array[String] (1_2,2_3,3_4,...)
        //过滤掉不需要的page，再将其转换为（page1_page2,1）(page2_page3,1) ...方便后续计算每个page_page 的总数
        val pageFilterSplit = pageSplit.filter(item =>targetPageSplit.contains(item)).map{
          item =>(item,1L)
        }
        pageFilterSplit
    }

    //pageSplitRDD : RDD[(page_page,1),...],聚合
    //使用countByKey，返回的是一个map(String -> Int) 使用groupByKey返回的是一个RDD
    val pageCountMap = pageSplitRDD.countByKey()

    //获取初始的page1
    val startpage = pageFlowArray(0).toLong
    //获取初始page的count
    val startPageCount = sessionId2ActionRDD.filter{
      case (sessionId,action) =>action.page_id==startpage
    }.count()
    //pageConvertMap用来保存每个有页面切片对应的转化率
    val pageConvertMap = new mutable.HashMap[String,Double]()
    //作为中间变量
    var lastCount = startPageCount.toDouble

    for(pageSplit <- targetPageSplit){
      //pageCountMap:(page1_page2,4) (page2_page3,6) ...
      val currentCount = pageCountMap(pageSplit).toDouble
      val rate = currentCount/lastCount
      //保存对应的转化率
      pageConvertMap += (pageSplit -> rate)
      lastCount = currentCount
    }
    //转换格式.mkString("|"):以符号“|”拼接，返回一个字符串
    val rateStr = pageConvertMap.map{
      case (pageSplit,rate) => pageSplit +"="+rate
    }.mkString("|")

    val pageSplit = PageSplitConvertRate(taskUUID,rateStr)
    //转换为RDD
    val pageSplitNewRDD = sparkSession.sparkContext.makeRDD(Array(pageSplit))

    //写入mysql
    import sparkSession.implicits._
    pageSplitNewRDD.toDF().write.format("jdbc")
        .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
        .option("dbtable", "page_split_convert_rate0115")
        .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
        .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
        .mode(SaveMode.Append)
        .save()

    sessionId2ActionRDD.foreach(println(_))
  }

  /**
    * 从表中读取数据
    * @param sparkSession
    * @param taskParam
    * @return
    */
  def getSessionAction(sparkSession: SparkSession, taskParam: JSONObject) = {
    val startDate = ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE)

    val sql = "select * from user_visit_action where date >='"+startDate+"' and date <='"+endDate+"'"
    //注意导入隐士转换
    import  sparkSession.implicits._
    //读取表中的数据并转化成(sessionId,userVisitAction)
    sparkSession.sql(sql).as[UserVisitAction].rdd.map(item=>(item.session_id,item))
  }
}
