package cn.cloudata.Utils

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.matching.Regex

/**
  * 固有算子的数据操作工具类
  */
object dataControlUtils {
  val master = "local[*]"

  val importAddress = "C:\\Users\\keter\\Desktop\\21983.csv"
  val datasetName = "test_Dataset"
  val datasetName_tmp = "testDataframe_tmp"
  val mongoURL = "mongodb://139.219.10.153:8987/admin"

  val schemaTableName = "schemaTable"
  val mongoAddress = "mongodb://139.219.10.153:8987/test"
  val database = "test"

  val saveDataframeToWindowCsv = "C:\\Users\\keter\\Desktop\\tmp"

  val spark = getSparkSession()

  def getTime(t1: Long, str: String) = {
    val t2: Long = System.currentTimeMillis()
    println("\n" + str + ":" + (t2 - t1))
  }

  def main(args: Array[String]): Unit = {
    val t1: Long = System.currentTimeMillis()

//    getDataframeFromCsv_detectionException(spark, importAddress, 0)



//    val list1 = List((1,6))
//    val rdd = spark.sparkContext.parallelize(list1)
//    import spark.implicits._
//    val data: DataFrame = rdd.toDF("t1","t2")
//    data.show
//
//    saveDataframe(data, "delete", "append")








//    val data = getDataframeFromMongoDB(spark, datasetName)
//    data.show()
//    saveDataframeToMongoDB(spark, data, datasetName+"_tmp")

    getTestData

    spark.stop()
    getTime(t1, "总耗时")
  }

  def getTestData(): Unit = {
    val t1: Long = System.currentTimeMillis()
    val data: DataFrame = getDataframeFromCsv(spark, importAddress)
    getTime(t1, "读取耗时")
    data.show

    //    val map: Map[String, String] = Map[String, String] (
    //      ("spark.mongodb.output.uri", mongoURL),
    //      ("spark.mongodb.output.database", database),
    //      ("spark.mongodb.output.collection", "testDataframe"+"_tmp")
    //    )
    //    saveDataframe(data, map)
    // append

    val t2: Long = System.currentTimeMillis()
    saveDataframeToMongoDB(spark, data.filter(x => {
      x.getLong(0)%30 == 0
    }), "123456789", "overwrite")
    getTime(t2, "存储耗时")
  }


  def getKeyData(spark: SparkSession, tmp_NodeNames: List[String] ) = {
    val t1 = System.currentTimeMillis()

    var sql: String = tmp_NodeNames.map(x => {
      " select timestamp from "+ x +" "
    }).mkString(" UNION ALL ")
    println("getKeyData_SQL:"+ sql)
    sql = "select timestamp from ("+ sql + ") as a group by a.timestamp"
    val keyData: DataFrame = spark.sql(sql)

    dataControlUtils.getTime(t1, "getKeyData:获得部分节点keyDate数据 耗时")
    keyData
  }




  /**
    * 读取MongoDB获得DF数据集
    */
  def getDataframeFromMongoDB(spark: SparkSession, dataName: String) = {
    val t1: Long = System.currentTimeMillis()
    val mongoURL = this.mongoURL
    val database = this.database

    // 构造读取mongoDB的参数信息
    val schemaMongoDB: StructType = getStructType(getStructType_MongoDBData())
    val map: Map[String, String] = Map[String, String](
      ("spark.mongodb.input.uri", mongoURL),
      ("spark.mongodb.input.database", database),
      ("spark.mongodb.input.collection", dataName)
    )

    // 读取合并状态的数据
    val dataFrame: DataFrame = spark
      .read.options(map)
      .schema(schemaMongoDB)
      .format("com.mongodb.spark.sql")
      .load()

    // 读取数据结构
    val schema: List[(String, String)] = getStructTypeList(spark, dataName)
    val tmptableName = "tmp_"+dataName+System.currentTimeMillis()
    dataFrame.createTempView(tmptableName)

    // 解析数据，添加数据结构
    var sql = "select timestamp "
    var i: Int = 1;
    while (i < schema.length) {
      sql += ",cast(split(data, ',')[" + (i - 1) + "] as Double)as `" + schema(i)._1+"`"
      i += 1
    }
    sql += " from `" + tmptableName+"` "
    val data: DataFrame = spark.sql(sql)

    getTime(t1, "getDataframeFromMongoDB：读取数据"+ dataName+ "耗时")
    data
  }


  /**
    * DF数据集覆盖写入MongoDB
    *
    * @param spark
    * @param data
    * @param dataName
    */
  def saveDataframeToMongoDB(spark: SparkSession, data: DataFrame, dataName: String, saveMode: String = "overwrite") = {
    val t2: Long = System.currentTimeMillis()

    // 获得schema
    val schema: StructType = data.schema

    //判断是否需要追加
    saveMode match {
      case "overwrite" => {
        // 数据处理
        val frame: DataFrame = dataCombine(spark, data, schema, dataName)

        // 数据存储
        saveDataframe(frame, dataName)

        // 结构存储
        saveSchema(spark, schema, dataName)

      }
      case "append" => {
        // 判断是否可以追加
        judgeAppend(spark, schema, dataName)

        // 数据处理
        val frame: DataFrame = dataCombine(spark, data, schema, dataName)

        // 数据存储
        saveDataframe(frame, dataName, saveMode)

      }
      case _ => throw new IllegalArgumentException(s"Unknown save mode: $saveMode. " +
        "Accepted save modes are 'overwrite', 'append', 'ignore', 'error'.")
    }

    getTime(t2, "saveDataframeToMongoDB:存储数据"+ dataName +"耗时")
  }

  // 是否可以追加的判断
  def judgeAppend(spark: SparkSession, schema: StructType, dataName: String) = {
    val oldSchema: List[(String, String)] = getStructTypeList(spark, dataName)
    val newSchema: Array[StructField] = schema.fields

    // 判断是否字段长度一致
    if (newSchema.length == oldSchema.length) {

      // 判断是否时间戳一致
      val aname: String = oldSchema(0)._1
      val bname: String = newSchema(0).name
      if ((aname != "timestamp" && bname != "timestamp") || (aname == "timestamp" && bname == "timestamp")) {
        println("正在追加数据")
      } else {
        println("结构不统一，追加失败")
        System.exit(1)
      }
    } else {
      println("长度不统一，追加失败")
      System.exit(1)
    }
  }

  // 数据处理
  def dataCombine(spark: SparkSession, data: DataFrame, schema: StructType, dataName: String) = {
    val t1: Long = System.currentTimeMillis()
    val tmp_tableName = "tmp" + t1

    data.createTempView(tmp_tableName)

    // 针对timeAndData数据的转化，SQL拼接
    // 数据分三种：time、timeanddate、data
    var sql = "select "
    val fields: Array[StructField] = schema.fields

    // 带时间戳的数据存储
    if (fields(0).name == "timestamp") {
      sql += (fields(0).name + " as `" + fields(0).name + "`, CONCAT(nvl(`" + fields(1).name+"`")
      var i = 2
      while (i < fields.length) {
        sql += (",0.0),\",\",nvl(`" + fields(i).name+"` ")
        i += 1
      }
      sql += ",0.0)) as data from " + tmp_tableName
    }

    // 不带时间戳的数据存储
    else {
      sql += ("CONCAT(nvl(" + fields(0).name)
      var i = 1
      while (i < fields.length) {
        sql += (",0.0),\",\",nvl(" + fields(i).name)
        i += 1
      }
      sql += ",0.0)) as data from " + tmp_tableName
    }

    val frame: DataFrame = spark.sql(sql)
    getTime(t1, "dataCombine:数据处理 耗时")
    frame
  }

  // 结构存储
  def saveSchema(spark: SparkSession, schema: StructType, tablename: String) = {
    val array = new JSONArray()
    val tagNameList = new ListBuffer[String]
    var i = 0
    val fields: Array[StructField] = schema.fields
    while (i < fields.length) {
      val column = new JSONObject()
      val nameS = fields(i).name.split("\\.")
      var name = ""
      try{
        name = nameS.last
      }catch {
        case _: Exception =>  name = fields(i).name
      }
      tagNameList.append(name)
      column.put("colname", fields(i).name)
      column.put("datatype", fields(i).dataType.toString)
      column.put("orderby", i)
      array.add(column)
      i += 1
    }

    val docs: Seq[(String, String, String)] = Seq((tablename, array.toString, tagNameList.mkString(",")))
    import spark.implicits._
    val schemaDF: DataFrame = spark.sparkContext.parallelize(docs).toDF("tablename", "columns", "tagNames")
    val map: Map[String, String] = Map[String, String](
      ("spark.mongodb.output.uri", mongoURL),
      ("spark.mongodb.output.database", database),
      ("spark.mongodb.output.collection", schemaTableName)
    )
    schemaDF.write
      .options(map)
      .mode("append")
      .format("com.mongodb.spark.sql")
      .save()
  }

  // 数据存储
  def saveDataframe(data: DataFrame, dataName: String, saveMode: String = "overwrite") = {
    val t1: Long = System.currentTimeMillis()

    val mongoURL = this.mongoURL
    val database = this.database

    // 构造写入mongoDB的参数信息
    val map: Map[String, String] = Map[String, String](
      ("spark.mongodb.output.uri", mongoURL),
      ("spark.mongodb.output.database", database),
      ("spark.mongodb.output.collection", dataName)
    )

    data.write
      .options(map)
      .mode(saveMode)
      .format("com.mongodb.spark.sql")
      .save()

    getTime(t1, "saveDataframe:本次"+ dataName +"表数据"+ saveMode +"导入 耗时")
  }

  def saveDataframeToCSV(data: DataFrame, address: String) = {
    data.write.csv(address)

    println("\n" + "数据成功保存到" + address)
  }

  /**
    * 读取csv文件获得DF数据集
    */
  def getDataframeFromCsv_detectionException(spark: SparkSession, importAddress: String, typesOfException: Int = 0) = {
    val t1: Long = System.currentTimeMillis()

    // 数据读取完毕
    val data: Dataset[Row] = spark.read
      .option("header", true)
      .csv(importAddress)

    // 处理数据
    data.rdd.map(x => {
      val tags: Seq[Any] = x.toSeq
      for(tag <- tags){
        // 判断第一个tag是Long
        // 判断其余tag是Double
        // 不符合标准的进行抛出，或者标记，方便过滤


      }
    })


    // 获得schema
    val schemaList = new ListBuffer[(String, String )]
    val fields: Array[StructField] = data.schema.fields
    // 默认首列就是timestamp
    schemaList.append(("timestamp","LongType"))
    for(i <- 1 to fields.length-1 ){
      schemaList.append((fields(i).name, "DoubleType"))
    }
    val structType: StructType = getStructType(schemaList.toList)


    // 等待处理后的数据集的写入
    val da: DataFrame = spark.createDataFrame(data.rdd, data.schema)
    da.show


    System.out.println("数据读取成功")
    getTime(t1, "getDataframeFromCsv: 耗时")
  }
  def getDataframeFromCsv(spark: SparkSession, importAddress: String, typesOfException: Int = 0) = {
    val t1: Long = System.currentTimeMillis()
    val tmp_tableName = "tmp_" + t1

    // 数据读取完毕
    val csv: Dataset[Row] = spark.read
      .option("inferSchema", "true")
      .option("nullValue ", "0.0")
      .option("header", true)
      .csv(importAddress)

    // 构建sparkSQL临时表
    csv.createTempView(tmp_tableName)

    // 获得数据结构
    val fields: Array[StructField] = csv.schema.fields

    // 组装SQL
    var sql = ""

    // 判断是否有date列
    val fieldName: String = "`"+fields(0).name+"`"

    if(fieldName =="date"){
      // 专门读大文件用的
      sql = "select "+fieldName+" as timestamp "
    } else {
      // 现在默认的所有csv文件首列都是时间戳
      if (fieldName == "timestamp" || fieldName == "date" || true) {
        // 首列是时间列
        sql = "select unix_timestamp(" + fieldName + ",'yyyy/MM/dd HH:mm:ss') as timestamp"
      } else {
        // 首列不是时间列
        sql = "select " + "case when " + fieldName + "=\'null\' then 0 when " + fieldName + " is null then 0 else " + fieldName + " end as " + fieldName
      }
    }

    var i = 1
    // 拼接之后的标签列名
    while ( {
      i < fields.length
    }) {
      val fieldName = "`"+fields(i).name+"`"

      // 字段名判断
//      judgeFieldName(fieldName)

      // 将数据转化为Double
      //      sql += (" ,cast(case when " + fieldName + "=\'null\' then 0 when " + fieldName + " is null then 0 else " + fieldName + " end as Double)as " + fieldName)

      // 不将数据转化为Double
      sql += (" ,case when " + fieldName + "=\'null\' then 0 when " + fieldName + " is null then 0 else " + fieldName + " end as " + fieldName)
      i += 1
    }
    sql += (" from " + tmp_tableName)

    // 运行sql获取数据

    val frame = spark.sql(sql)
    System.out.println("数据读取成功")
    getTime(t1, "getDataframeFromCsv: 耗时")

    frame
  }

  // 列名格式的匹配
  def judgeFieldName(fieldName: String) = {
    // 正则匹配的规则是：变量名的匹配规则
    val regex: Regex = "^[A-Za-z_][A-Za-z0-9_]*$".r
    regex.findFirstMatchIn(fieldName) match {
      case Some(_) => {
      }
      case None => {
        println("Wrong input：fieldName => " + fieldName)
        System.exit(1)
      }
    }
  }

  //  /**
  //    * 读取csv文件获得DF数据集
  //    */
  //  def getDataframeFromCsv_windows(spark:SparkSession, importAddress:String) = {
  //    val structType: StructType = getStructType(getStructTypeFile_Test_String())
  //    val dataset: DataFrame = spark.read
  //      .schema(structType)
  //      .option("inferSchema", "true")
  //      .option("nullValue ", "0.0")
  //      .csv(importAddress)
  //    val tableName = "tmp_"+importAddress.split("\\\\").last.split(".csv")(0)
  //    dataset.createTempView(tableName)
  //    val fields: Array[StructField] = structType.fields
  //
  //    //时间格式：yyyy/MM/dd HH:mm:ss
  ////    var sql = "select unix_timestamp(timestamp,'yyyy/MM/dd HH:mm:ss') as timestamp"
  //
  //    // 时间格式：时间戳123456789
  //    var sql = "select "+fields(0).name+""
  //    var i = 1
  //    while(i<fields.length){
  //      sql += (" ,cast (case " + fields(i).name+" when  \"null\" then \"0.0\" else "+fields(i).name+" end as Double)as "+fields(i).name)
  //      i +=1
  //    }
  //    sql += (" from "+tableName)
  ////    println(sql)
  //    spark.sql(sql)
  //  }


  /**
    * 获得spark上下文
    *
    * @return
    */
  def getSparkSession() = {
    //    val conf = new SparkConf
    //    conf.set("spark.mongodb.input.uri", mongoURL)
    //    conf.set("spark.mongodb.output.uri", mongoURL)
    //    conf.set("spark.mongodb.input.database","test")
    //    conf.set("spark.mongodb.output.database","test")
    //    conf.set("spark.mongodb.input.collection",schemaTableName)
    //    conf.set("spark.mongodb.output.collection",schemaTableName)

    SparkSession.builder()
      .master(master)
      //      .config(conf)
      .appName("MyApp")
      .getOrCreate()
  }

  /**
    * 解析数据
    * 获得数据的表结构
    */
  def getStructTypeList(data: DataFrame) = {
    val tagNameAndTypeList: ListBuffer[(String, String)] = ListBuffer[(String, String)]()
    val fields: Array[StructField] = data.schema.fields
    for (i <- fields) {
      tagNameAndTypeList.append((i.name, i.dataType.toString))
    }
    tagNameAndTypeList.toList
  }

  /**
    * 生成数据的表结构
    *
    * @param tageNameAndTypeList
    * @return StructType
    */
  def getStructType(tageNameAndTypeList: List[(String, String)]) = {
    val structFieldList = new ListBuffer[StructField]()
    for (i <- tageNameAndTypeList) {
      val tageName = i._1
      i._2 match {
        case "Double" => {
          structFieldList.append(StructField(tageName, DoubleType))
        }
        case "DoubleType" => {
          structFieldList.append(StructField(tageName, DoubleType))
        }
        case "IntegerType" => {
          structFieldList.append(StructField(tageName, IntegerType))
        }
        case "String" => {
          structFieldList.append(StructField(tageName, StringType))
        }
        case "StringType" => {
          structFieldList.append(StructField(tageName, StringType))
        }
        case "Date" => {
          structFieldList.append(StructField(tageName, DateType))
        }
        case "TimestampType" => {
          structFieldList.append(StructField(tageName, TimestampType))
        }
        case "Long" => {
          structFieldList.append(StructField(tageName, LongType))
        }
        case "LongType" => {
          structFieldList.append(StructField(tageName, LongType))
        }
        case _ => {
          throw new Exception("构建数据的表结构：输入了无效的的数据结构")
        }
      }
    }
    StructType(structFieldList)
  }

  def getStructType_MongoDBData(): List[(String, String)] = {
    List[(String, String)](
      ("timestamp", "Long"),
      ("data", "String")
    )
  }

  def getStructType_MongoDBSchema(): List[(String, String)] = {
    List[(String, String)](
      ("columns", "String"),
      ("tablename", "String")
    )
  }

  // 获得数据schema
  def getStructTypeList(spark: SparkSession, dataName: String): List[(String, String)] = {
    val map: Map[String, String] = Map[String, String](
      ("spark.mongodb.input.uri", mongoURL),
      ("spark.mongodb.input.database", database),
      ("spark.mongodb.input.collection", schemaTableName)
    )
    val schema = getStructType(getStructType_MongoDBSchema)
    val dataFrame: DataFrame = spark
      .read.options(map)
      .schema(schema)
      .format("com.mongodb.spark.sql")
      .load()
    val tmp_schemTtableName = dataName + "_schema"+ System.currentTimeMillis()
    dataFrame.createTempView(tmp_schemTtableName)
    val sql = "select tablename, columns from " + tmp_schemTtableName + " a where a.tablename='" + dataName + "' limit 1"
    val row: Row = spark.sql(sql).take(1)(0)
    val schemaList = row.getAs[String]("columns")

    val array: JSONArray = JSON.parseArray(schemaList)
    var arrayBuffer: ArrayBuffer[(String, String)] = new ArrayBuffer[(String, String)](array.toArray.length)
    for (item <- array.toArray) {
      if (item.isInstanceOf[JSONObject]) {
        val json = item.asInstanceOf[JSONObject]
        val name = json.getString("colname")
        val datatype = json.getString("datatype")
        val i = json.getString("orderby").toInt

        arrayBuffer.insert(i, (name, datatype))
      }
    }
    arrayBuffer.toList
  }

  // 异常抛出
  def throwException(spark: SparkSession, appName: String , exceptionMessage: String): Unit ={
    val mes: List[(String, String)] = List((appName, exceptionMessage))
    val rdd: RDD[(String, String)] = spark.sparkContext.parallelize(mes)
    import spark.implicits._
    val data: DataFrame = rdd.toDF("t1","t2")
    val tableName = "ExceptionMessage"
    saveDataframe(data, tableName, "append")
  }

}
