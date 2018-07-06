
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

/**
  * Created by zhengbiubiu on 2018/6/29.
  */
class GroupConcatDistinct extends UserDefinedAggregateFunction {
  //指定输入类型
  override def inputSchema: StructType = StructType(StructField("city_info", StringType) :: Nil)

  //指定缓冲类型
  override def bufferSchema: StructType = StructType(StructField("buffer_city_info", StringType) :: Nil)

  //指定输出类型
  override def dataType: DataType = StringType

  //一致性检验，当输入相同的时候，是否保证输入也相同
  override def deterministic: Boolean = true

  //初始化缓冲区
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    var bufferCityInfo = buffer.getString(0)
    var cityInfo = input.getString(0)
    if (!bufferCityInfo.contains(cityInfo)) {
      if ("".equals(bufferCityInfo)) {
        bufferCityInfo += cityInfo
      } else {
        bufferCityInfo += "," + cityInfo
      }
      buffer.update(0, bufferCityInfo)
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var bufferCityInfo1 = buffer1.getString(0)
    val bufferCityInfo2 = buffer2.getString(0)
    for(cityInfo <- bufferCityInfo2) {
      if(!bufferCityInfo1.contains(cityInfo)){
        if("".equals(bufferCityInfo1)){
          bufferCityInfo1 += cityInfo
        }else{
          bufferCityInfo1 += ","+cityInfo
        }
      }
    }
    buffer1.update(0,bufferCityInfo1)
  }

  override def evaluate(buffer: Row): Any ={
    buffer.getString(0)
  }
}
