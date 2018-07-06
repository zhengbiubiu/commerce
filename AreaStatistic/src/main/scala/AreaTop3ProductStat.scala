import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.ParamUtils
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
  * Created by zhengbiubiu on 2018/6/29.
  */
object AreaTop3ProductStat {



  def main(args: Array[String]): Unit = {

    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam = JSONObject.fromObject(jsonStr)


    val taskUUID = UUID.randomUUID().toString
    val sparkConf = new SparkConf().setAppName("area").setMaster("local[*]")

    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    //获取原始数据
    //cityId2Row:RDD[city_id,click_product_id[Row]]
    val cityId2RowRDD = getCityAndProductInfo(sparkSession, taskParam)
    //获取区域和city对应关系
    //cityId2AreaInfoRDD:RDD[cityId,areaInfo[Row]]
    val cityId2AreaInfoRDD = getAreaInfo(sparkSession)

    cityId2AreaInfoRDD.foreach(println(_))
    //cityId2RowRDD:RDD[city_id,click_product_id[Row]],cityId2AreaInfoRDD:RDD[cityId,areaInfo[Row]]
    //|city_id|city_name|area|product_id|
    getAreaProductBasicInfo(sparkSession, cityId2RowRDD, cityId2AreaInfoRDD)


    sparkSession.udf.register("concat_long_string",(v1:Long,v2:String,split:String)=>
      v1+split +v2
    )

    sparkSession.udf.register("group_concat_distinct",new GroupConcatDistinct)

    getAreaproductClickCount(sparkSession)

    sparkSession.udf.register("get_json_value", (str:String, field:String) => {
      val jsonObject = JSONObject.fromObject(str)
      jsonObject.getString(field)
    })
    getProductInfo(sparkSession)

    sparkSession.sql("select * from tmp_area_product_basic_info order by city_id desc").show()

    getAreaTop3PopularProduct(sparkSession, taskUUID)

  }

  def getAreaTop3PopularProduct(sparkSession: SparkSession, taskUUID: String): Unit = {
    val sql = "select area, " +
      "CASE " +
      "WHEN area='华北' OR area='华东' THEN 'A Level' " +
      "WHEN area='华南' OR area='华中' THEN 'B Level' " +
      "WHEN area='西南' OR area='西北' THEN 'C Level' " +
      "ELSE 'D Level' " +
      "END area_level, product_id, city_infos, click_count, product_name, product_status from(" +
      " select area, product_id, city_infos, click_count, product_name, product_status, " +
      " row_number() over(partition by area order by click_count desc) rank from tmp_area_product_info) t" +
      " where rank<=3"

    // rdd: RDD[Row]  Row: area, areaLevel, product_id, city_infos, click_count, product_name, product_status
    val areaTop3ProductRDD = sparkSession.sql(sql).rdd

    val areaTop3RDD = areaTop3ProductRDD.map{
      row =>
        AreaTop3Product(taskUUID, row.getAs[String]("area"), row.getAs[String]("area_level"),
          row.getAs[Long]("product_id"), row.getAs[String]("city_infos"),
          row.getAs[Long]("click_count"), row.getAs[String]("product_name"),
          row.getAs[String]("product_status"))
    }

    import sparkSession.implicits._
    areaTop3RDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "area_top3_product0115")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()
  }


  def getProductInfo(sparkSession: SparkSession) = {
    // 进行tmp_area_product_click_count与product_info表格的联立
    // if(判断条件, 为ture执行, 为false执行)
    val sql = "select tapcc.area, tapcc.city_infos, tapcc.product_id, tapcc.click_count, pi.product_name," +
      "if(get_json_value(pi.extend_info, 'product_status')='0', 'Self', 'Third Party') product_status " +
      "from tmp_area_product_click_count tapcc join product_info pi on tapcc.product_id = pi.product_id"

    sparkSession.sql(sql).createOrReplaceTempView("tmp_area_product_info")

  }


  def getAreaproductClickCount(sparkSession: SparkSession): Unit = {
    val sql = "select area, product_id, count(*) click_count, " +
      " group_concat_distinct(concat_long_string(city_id, city_name, ':')) city_infos " +
      " from tmp_area_product_basic_info group by area, product_id"

    sparkSession.sql(sql).createOrReplaceTempView("tmp_area_product_click_count")
  }


  def getAreaProductBasicInfo(sparkSession: SparkSession,
                              cityId2RowRDD: RDD[(Long, Row)],
                              cityId2AreaInfoRDD: RDD[(Long, Row)]): Unit = {
    val areaProductBasicRDD = cityId2RowRDD.join(cityId2AreaInfoRDD).map {

      case (cityId, (productInfo, areaInfo)) =>
        val cityName = areaInfo.getAs[String]("city_name")
        val area = areaInfo.getAs[String]("area")
        val productId = productInfo.getAs[Long]("click_product_id")

        (cityId, cityName, area, productId)
    }
    //创建一个临时表
    import sparkSession.implicits._
    areaProductBasicRDD.toDF("city_id", "city_name", "area", "product_id").
      createOrReplaceTempView("tmp_area_product_basic_info")

  }


  def getAreaInfo(sparkSession: SparkSession) = {

    val cityAreaInfoArray = Array((0L, "北京", "华北"), (1L, "上海", "华东"), (2L, "南京", "华东"),
      (3L, "广州", "华南"), (4L, "三亚", "华南"), (5L, "武汉", "华中"),
      (6L, "长沙", "华中"), (7L, "西安", "西北"), (8L, "成都", "西南"),
      (9L, "哈尔滨", "东北"))

    import sparkSession.implicits._
    val cityAreaInfoRDD = sparkSession.sparkContext.makeRDD(cityAreaInfoArray).toDF("city_id", "city_name", "area")
      .rdd.map {
      row => (row.getLong(0), row)
    }
    cityAreaInfoRDD
  }


  def getCityAndProductInfo(sparkSession: SparkSession, taskParam: JSONObject) = {
    val startTime = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endTime = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    val sql = "select city_id,click_product_id from user_visit_action where date >='" + startTime +
      "' and date <='" + endTime + "' and click_product_id != -1 and click_product_id is not null"

    import sparkSession.implicits._
    sparkSession.sql(sql).rdd.map {
      //RDD[row] 获取值两种，getAS[类型](字段名)等价于getLong(索引下标)
      row => (row.getAs[Long]("city_id"), row)

    }

  }

}
