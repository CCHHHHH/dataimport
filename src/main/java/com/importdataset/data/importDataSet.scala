package cn.cloudata.Calculate

import cn.cloudata.Utils.dataControlUtils
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable._

object importDataSet {

  val localMode = true

  val logger: Logger = Logger.getLogger(importDataSet_1.getClass)

  def main(args: Array[String]): Unit = {
    val programStartTime: Long = System.currentTimeMillis()

    // 默认所有标签名是不重复的
    logger.warn("\r\n--------------jsonStr")
    val jsonStr = args(0)
    logger.warn(jsonStr)
    logger.warn("\r\n----------------ruleJson")
    val ruleJson = args(1)
    logger.warn(ruleJson)

    var master = ""
    if(localMode){
      master = "local[*]"
    }else {
      master = "spark://139.219.10.153:7077"
    }
    val spark = getSparkSession(master)
    import spark.implicits._

    // 解析json得到导入参数: csv/mysql - 参数parameters - 标签属性列表[ 标签序列号、字段ID、字段描述名、标签存储类型 ]
    val dataSources: List[(String, Map[String, String], List[(String, String, String, String )])] = getDatasetInformation(jsonStr )
    val Rule = getDatasetManageRule(ruleJson)

    // 预处理工作 需要的参数
    // 新数据集的名字
    val newDatasetName = Rule._1
    // 开始时间： 时间戳、可以为"",但是不能为null
    val startTime = Rule._2.toLong/1000
    // 结束时间： 时间戳、可以为"",但是不能为null
    val finishTime = Rule._3.toLong/1000
    // 时间过滤,截取数据集
    var timestampFilterCondition = " "
    if(startTime.equals("") && !finishTime.equals("")){
      timestampFilterCondition = " where timestamp<="+ finishTime +" "
    }else if(!startTime.equals("") && finishTime.equals("")){
      timestampFilterCondition = " where timestamp>="+ startTime +" "
    }else if(!startTime.equals("") && !finishTime.equals("")){
      timestampFilterCondition = " where timestamp<="+ finishTime +" and timestamp>="+ startTime +" "
    }
    // 新的时间间隔
    var timeInterval = "0"
    if(Rule._5==null || Rule._5.equals("") ||Rule._5.equals("0")){
    }else{
      timeInterval = Rule._5
    }
    // 类型判断不符合时的处理方式： 0 舍弃异常行 1 失败报错  默认是报错 不做任何选择时怎么处理？？？是抛出，还是报错，还是不管不顾？
    var typesOfException = 0
    if(Rule._6==null || Rule._6.equals("")||Rule._6.equals("1")){
      // 默认引入失败
      typesOfException = 1
    }

    // 每一个数据源获得一个子数据集 - 顺便生成临时表，返回临时表表名
    val tmp_dataSourceNames: List[String] = dataSources.map(dataSource => {
      // 数据源
      val dataSourceName: String = dataSource._1
      // 连接数据源的参数
      val dataSourceRule: mutable.Map[String, String] = dataSource._2
      // 本数据源读取的标签
      val dataSourceTagTpyes: List[(String, String, String, String)] = dataSource._3
      // 标签列表
      val tagInformations: String = dataSourceTagTpyes.map(x => {
        "`" + x._2 + "`"
      }).mkString(" , ")
      // 每个子数据集的临时表表名
      val tmp_tableName: String = "`tmp" + dataSourceName + System.currentTimeMillis() + "`"

      // tag的新的名字 - 类型 ： 用来形成schema
      val tagDesc: List[(String, String)] = dataSourceTagTpyes.map(x => {
        (x._3, x._4)
      })

      dataSourceName match {
        case "CSV" => {
          // csv地址 - 输入文件的目录
          val csvAddress: String = dataSourceRule.get("csvAddress").get

          // 读取csv文件获得子数据集临时表
          getPartitionDatasetFromCSV(spark, csvAddress, tagInformations, tmp_tableName, timestampFilterCondition, timeInterval, tagDesc)
        }
        case "mysql" =>
        case _ =>
      }
      tmp_tableName
    })

    logger.warn("\r\n所有数据子集已经全部获取：")
    tmp_dataSourceNames.foreach(x =>{
      logger.warn(x)
    })
    logger.warn ("\r\n")

    // 合并所有子数据集
    //  获得所有标签的顺序组合  序号 - ID - 类型
    val tagsArrray: List[(String, String, String, String)] = dataSources.flatMap(dataSource => {
      dataSource._3.map(x => {
        (x._1, x._2, x._4, x._3)
      })
    })
    val tagsSort = new Array[String](tagsArrray.length)
    for(i <- tagsArrray){
      tagsSort(i._1.toInt-1) = "`"+i._4+"`"
    }
    val tagNames: String = tagsSort.mkString(", ")

//     获得datekey
//     是获得union All key，还是startTime finishTime key ？
    val tmp_keyTimeDataTableName = "tmp_keyTimeData"+ programStartTime
    if(!timeInterval.equals("0")){
      // startTime finishTime key
      val value = finishTime.toLong-startTime.toLong
      val list = List((value, finishTime.toLong), (0L, finishTime.toLong))
      val baseTime: RDD[(Long, Long )] = spark.sparkContext.parallelize(list)
      val schema: StructType = StructType(List(new StructField("timestamp",LongType)))
      val frame: DataFrame = baseTime.toDF("t","timestamp")
      // 如果时间间隔>（3600 * timeInterval），则先进行初步分割，再进行进一步分割
      val keyTimeData: DataFrame = upsampling(spark, frame, schema, timeInterval.toLong)
      keyTimeData.createTempView(tmp_keyTimeDataTableName)
    }else{
      // union All key。
      val keyTimeData: DataFrame = dataControlUtils.getKeyData(spark, tmp_dataSourceNames)
      keyTimeData.createTempView(tmp_keyTimeDataTableName)
    }


    //根据datekey进行合成数据集
    // 临时表组合
    var dataSource = " from `"+tmp_keyTimeDataTableName+"` as `0` "
    for(k <- 0 until tmp_dataSourceNames.length){
      val i = k+1
      dataSource += " full join "+tmp_dataSourceNames(k)+" as `"+ i +"` on `0`.timestamp = `"+i+"`.timestamp "
    }
    val dataSourcesql = "select `0`.timestamp, "+ tagNames + dataSource +" order by timestamp "
    val dataEnd: DataFrame = spark.sql(dataSourcesql)
    if(localMode){
      println("标签：")
      println(tagNames)
      println("表：")
      println(dataSource)
      println("\r\n")
      dataEnd.show
    }


    // 数据合并完，保存前，整合类型判断
    // 类型判断不符合时的处理方式： 0 舍弃异常行 1 失败报错  默认报错
    val rdd = dataEnd.rdd.flatMap(row => {
      val rows: ListBuffer[Row] = new ListBuffer[Row]
      // 对timestamp进行判断
      var timestamp = 0L
      try {
        timestamp = row.getLong(0)
      }catch {
        case _: Exception => {
          if(typesOfException.equals("0")){
            //舍弃
            println("发现错误数据"+ row.toString() +": 该行数据舍弃")
          }else {
            // 报错
            println("发现错误数据"+ row.toString())
            System.exit(1)
          }
        }
      }
      // 循环遍历时间戳之外的值
      val valueList = new ListBuffer[Double]
      var state = true
      var v = ""
      var value = ""
      var d = 0.0
      for (i <- 1 until row.length) {
        d = 0.0
        try {
          value = row.getString(i)
          if(value==null){
            valueList.append(d)
          }
          else {
            valueList.append(value.toDouble)
          }
        }catch {
          case _: Exception => {
            state = false
            v = row.getString(i)
            0.0
          }
        }
      }
      val rd: Row = Row.merge(Row.apply(timestamp), Row.fromSeq(valueList))
      if (state) {
        // 有效数据
        // 没有发现错误
        rows.append(rd)
      } else {
        // 无效数据
        // 发现错误，根据前置条件，选择舍弃还是报错
        if(typesOfException.equals("0")){
          //舍弃
          println(timestamp +"时间点发现"+"标签下错误数据"+ v +": 该行数据舍弃")
        }else {
          // 报错
          println(timestamp +"时间点发现错误数据"+ v +"")
          System.exit(1)
        }
      }
      rows
    })

    // 新数据集的schema
    val structFields = new ListBuffer[StructField]
    structFields.append(new StructField("timestamp", LongType ))
    for(tag <- tagsArrray){
      if(tag._3.equals("DoubleType")){
        val Field = new StructField(tag._4, DoubleType )
        structFields.append(Field)
      }
    }
    val schema: StructType = StructType(structFields)
    val data: DataFrame = spark.createDataFrame(rdd, schema )

    if(localMode) data.show()
    dataControlUtils.saveDataframeToMongoDB(spark, data, newDatasetName)

    // 别忘记资源关闭
    spark.stop()
    getTime(programStartTime, "数据集"+ newDatasetName +"读写完毕 总耗时")
  }



  // 解析第二个json
  def getDatasetManageRule(jsonStr: String ) = {
//    val map = new mutable.HashMap[String, String]
    val strObject: JSONObject = JSON.parseObject(jsonStr)
    val newDatesetID: String = strObject.getString("newDatasetID")
    val startTime: String = strObject.getString("startTime")
    val finishTime: String = strObject.getString("finishTime")
    val resampleMode: String = strObject.getString("resampleMode")
    val typesOfException: String = strObject.getString("typesOfException")
    val timeInterval: String = strObject.getString("timeInterval")

    //    map.+=(("newDatesetID", newDatesetID))
    //    map.+=(("newDatesetID", newDatesetID))
    //    map.+=(("newDatesetID", newDatesetID))

    val tuple: (String, String, String, String, String, String) = (newDatesetID, startTime, finishTime, resampleMode, timeInterval, typesOfException)
    println(tuple)
    tuple
  }



  // 解析第一个json
  // 解析json得到导入参数: csv/mysql - 参数parameters - 标签属性列表[ 标签序列号、字段名、标签中文注释、标签存储类型 ]
  def getDatasetInformation(jsonStr: String ) = {
    val aggregateInformation = new ListBuffer[(String, Map[String, String], List[(String, String, String, String)])]
    val aggInfArray: Array[AnyRef] = JSON.parseArray(jsonStr).toArray
    for (item <- aggInfArray) {
      if (item.isInstanceOf[JSONObject]) {
        val json: JSONObject = item.asInstanceOf[JSONObject]

        // 数据源
        val dataSource: String = json.getString("dataSource")

        // 数据源参数
        val dataSourceRule: String = json.getString("dataSourceRule")
        val ruleMap = new HashMap[String, String ]
        dataSource match {
          case "CSV" => {
            val dataSourceObject: JSONObject = JSON.parseObject(dataSourceRule)
            val csvAddress: String = dataSourceObject.getString("csvAddress")
            ruleMap.+=(("csvAddress", csvAddress))
          }
          case "mysql" => {

          }
          case _ =>
        }

        // 数据源标签列表
        val dataSourceTagTpyes = json.getString("tagInf")
        val tagInormations = new ListBuffer[(String, String, String, String )]
        val tagInfArray: Array[AnyRef] = JSON.parseArray(dataSourceTagTpyes).toArray
        for (item <- tagInfArray) {
          if (item.isInstanceOf[JSONObject]) {
            val json: JSONObject = item.asInstanceOf[JSONObject]
            val tagOrder: String = json.getString("tagOrder")
            val tagID: String = json.getString("tagID")
            val tagDesc: String = json.getString("tagDesc")
            val tagType: String = json.getString("tagType")

            tagInormations.append((tagOrder, tagID, tagDesc, tagType ))
          }
        }
        aggregateInformation.append((dataSource, ruleMap, tagInormations.toList ))
      }
    }
    aggregateInformation.toList
  }



  // 从CSV中获得子数据集,并完成预处理
  // 标签属性列表[ 标签序列号、字段名、标签中文注释、标签存储类型 ]
  def getPartitionDatasetFromCSV(spark: SparkSession, csvAddress: String, tagInformations: String, tmp_dataSourceName: String, timestampFilterCondition: String, newTimeInterval: String, tagDesc: List[(String, String)]) = {
    val readCsvtime: Long = System.currentTimeMillis()

    // 文件数据集合的名字 默认为文件名，或者自定义
    var tableName = csvAddress.split("/").last.split(".csv")(0)

    // 获得csv数据
    val csv: Dataset[Row] = spark.read.option("header", true).csv(csvAddress)
    val tmp_csvDatasetName = "tmp_csv"+readCsvtime
    csv.createTempView(tmp_csvDatasetName)

    logger.warn("\r\n读取"+ csvAddress +" 获得的数据集")
    if(localMode) {
      csv.show()
    }

    // 1
    // 进行数据处理：默认第一列为时间：将yyyy/MM/dd HH:mm:ss格式转化为 时间戳
    // 操作：时间戳转换 - 时间筛选 - 字段筛选
    val fields: Array[StructField] = csv.schema.fields
    val fieldName: String = " `"+fields(0).name+"` "
    val sql_a = "select unix_timestamp(" + fieldName + ",'yyyy/MM/dd HH:mm:ss') as timestamp, "
    val sql_c = " from "+ tmp_csvDatasetName
    val sql = " select *  from( "+ sql_a + tagInformations + sql_c +" ) as a "+ timestampFilterCondition
    val baseData: DataFrame = spark.sql(sql)
    val tmp_baseDataTableName = "tmp_tmp_baseDataTableName"+ readCsvtime
    baseData.createTempView(tmp_baseDataTableName)

    logger.warn("\r\n根据")
    logger.warn("\r\n  标签 "+ tagInformations +"   |")
    logger.warn("\r\n  条件 "+ timestampFilterCondition +"   |")
    logger.warn("\r\n根据表 "+ csvAddress +"筛选后的基本数据集")
    if(localMode) {
      baseData.show()
    }

    // 获得带时间间隔的数据
    val t1: Long = System.currentTimeMillis()
    val oldTimeInterval_sqla = "select lead(timestamp,1,timestamp)over(order by timestamp )-timestamp as t , * from "+ tmp_baseDataTableName
    val timeIntervalDataset: DataFrame = spark.sql(oldTimeInterval_sqla)
    val tmp_timeIntervalDatasetTableName = "tmp_tmp_timeIntervalDatasetTableName"+readCsvtime
    timeIntervalDataset.createTempView(tmp_timeIntervalDatasetTableName)

    logger.warn("\r\n 数据"+ csvAddress +"进行重采样的基本数据 - 每行数据都带着其与下一行数据的时间间隔")
    if(localMode) {
      timeIntervalDataset.show()
    }

    // 获得这个数据的时间间隔
    val oldTimeInterval_sql = " select t as c from "+ tmp_timeIntervalDatasetTableName +" order by t "
    val timeInterval: DataFrame = spark.sql(oldTimeInterval_sql)
    val oldTimeInterval = timeInterval.collectAsList.get(1).getLong(0)
    if(localMode) {
      timeInterval.show()
    }
    logger.warn("\r\n数据"+ csvAddress +"的时间间隔: "+ oldTimeInterval)
    if(localMode) {
      timeInterval.show()
    }

    // 结构：Long - String - String - String ...
    val structFields = new ListBuffer[StructField]
    structFields.append(new StructField("timestamp", LongType ))
    for(tag <- tagDesc){
      if(tag._2.equals("DoubleType")){
        val Field = new StructField(tag._1, StringType )
        structFields.append(Field)
      }
    }
    val schema: StructType = StructType(structFields)

    // 不进行重采样操作
    if(newTimeInterval.equals("0")){
      logger.warn("不进行重采样操作，过滤后数据直接合并且储存")
      val data: DataFrame = spark.createDataFrame(baseData.rdd, schema)
      data.createTempView(tmp_dataSourceName)
      getTime(readCsvtime, "getPartitionDatasetFromCSV:读取 " + csvAddress + " 总耗时")
      if(localMode) {
        data.show()
      }
    }else{
      // 完成过滤操作后直接进行重采集
      if(oldTimeInterval == newTimeInterval.toLong){
        // 等值重采集
        // 直接析分
        val data: DataFrame = upsampling(spark, timeIntervalDataset, schema, newTimeInterval.toLong )
        data.createTempView(tmp_dataSourceName)

        getTime(readCsvtime, "getPartitionDatasetFromCSV:读取 " + csvAddress + " 总耗时")
        if(localMode) {
          data.show()
        }
      }else if(oldTimeInterval < newTimeInterval.toLong){
        // 时间变化频率减慢 5 - 15
        // 先根据old间距析分
        val data1: DataFrame = upsampling(spark, timeIntervalDataset, schema, oldTimeInterval )
        val tmp_tableName = "tmp_upsampling"+ readCsvtime
        data1.createTempView(tmp_tableName)
        // 再聚合
        val data2: DataFrame = downsampled(spark, tmp_tableName, schema, newTimeInterval.toLong )
        data2.createTempView(tmp_dataSourceName)

        getTime(readCsvtime, "getPartitionDatasetFromCSV:读取 " + csvAddress + " 总耗时")
        if(localMode) {
          data2.show()
        }
      }else if(oldTimeInterval > newTimeInterval.toLong){
        // 时间变换频率加快 15 - 5
        // 求最大公约数值
        val timeInterval: Long = gcd(oldTimeInterval, newTimeInterval.toLong)

        // 先析分-到最大公约数
        val data1: DataFrame = upsampling(spark, timeIntervalDataset, schema, timeInterval )
        val tmp_tableName = "tmp_upsampling"+ readCsvtime
        data1.createTempView(tmp_tableName)
        // 再聚合
        val data2: DataFrame = downsampled(spark, tmp_tableName, schema, newTimeInterval.toLong )
        data2.createTempView(tmp_dataSourceName)

        getTime(readCsvtime, "getPartitionDatasetFromCSV:读取 " + csvAddress + " 并完成过滤和重采样操作 总耗时")
        if(localMode) {
          data2.show()
        }
      }
    }
  }

  // 求 最大公约数
  private def gcd(a: Long , b: Long ):  Long = if  (b == 0) a else gcd(b, a % b)

  // 聚合：降采样 - 高频数据到低频数据 - 间隔 5 - 15
  def downsampled(spark: SparkSession, dataTableName: String, schema: StructType, timeInterval: Long) = {
    val downsampledTime = System.currentTimeMillis()

    val fieldNames: ListBuffer[String] = new ListBuffer[String]
    val fields: Array[StructField] = schema.fields
    for(i <- 1 until fields.length){
      val tagName = fields(i).name
      val tagstr = " avg(`"+ tagName +"`) `"+ tagName +"` "
      fieldNames.append(tagstr)
    }
    val tagStrs: String = fieldNames.mkString(",")

    val sqla = " select timestamp - timestamp % "+ timeInterval +" as t, * from "+ dataTableName
    val sql = " select min(timestamp) timestamp, "+ tagStrs +" from ("+ sqla +")a group by t "
    val data: DataFrame = spark.sql(sql)

    logger.warn(tagStrs)
    getTime(downsampledTime, "downsampled:降采样 - 高频数据到低频"+ timeInterval +" 数据变稀疏 耗时")
    if(localMode){
      data.show()
    }
    data
  }



  // 析出：升采样 - 低频数据到高频数据 - 间隔 15 - 5
  def upsampling(spark: SparkSession, data: DataFrame, schema: StructType, timeInterval: Long) = {
    val upsamplingTime = System.currentTimeMillis()

    val dataValue = data.rdd.flatMap(row => {
      val rows: ListBuffer[Row] = new ListBuffer[Row]

      // 本段时间间隔
      val t: Long = row.getLong(0)
      val l: Long = t / timeInterval - 1
      // 需要插入的新 row 的数量
      val n = l

      // 本时间段的初始时间戳值
      val firestTimeStamp: Long = row.getLong(1)

      // 准备需要插入的row的值
      var tags: ListBuffer[String] = new ListBuffer[String]
      for(i <- 2 until row.length){
        tags.append(row.getString(i))
      }

      // 本节点至少有一个row
      rows.append(Row.merge(Row.apply(firestTimeStamp),Row.fromSeq(tags)))

      for (i <- 1 to n.toInt) {
        val timeStamp: Long = firestTimeStamp + i * timeInterval
        val row: Row = Row.merge(Row.apply(timeStamp),Row.fromSeq(tags))
        rows.append(row)
      }
      rows
    })

    val ds: DataFrame = spark.createDataFrame(dataValue, schema )
    getTime(upsamplingTime, "upsampling:升采样 - 低频数据到高频"+ timeInterval +" 数据变稠密 耗时")
    if(localMode){
      ds.show()
    }
    ds
  }



  def getSparkSession(master: String) = {
    val conf = new SparkConf()
    //    conf.set("spark.jars", )

    SparkSession
      .builder()
      .master(master)
      .config(conf)
      .getOrCreate()
  }

  def getTime(t1: Long, str: String) = {
    val t2: Long = System.currentTimeMillis()
    logger.warn("\r\n-------------------------------------\r\n" + str + ":" + (t2 - t1))
  }

}
